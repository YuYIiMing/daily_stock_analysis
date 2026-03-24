# -*- coding: utf-8 -*-
"""Trend-system service for A-share full-market trading assistance."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import pandas as pd

from data_provider import DataFetcherManager
from src.config import Config, get_config
from src.services.market_service import MarketService
from src.storage import DatabaseManager, get_db

logger = logging.getLogger(__name__)

STAGE_ORDER = {"choppy": 0, "initial": 1, "middle": 2, "late": 3}
RISK_MODE_PRIORITY = {
    "normal": 0,
    "reduced_risk": 1,
    "elite_disabled": 2,
    "breakout_paused": 3,
    "degraded_system": 4,
    "cooldown": 5,
}
DOWNGRADE_RULES = {
    "late": {"middle", "initial", "choppy"},
    "middle": {"initial", "choppy"},
    "initial": {"choppy"},
    "choppy": set(),
}
MAX_UNIVERSE_SCAN = 180
LIMIT_UP_PCT = 9.5
DAILY_SNAPSHOT_REQUIRED_KEYS = {
    "overview",
    "position",
    "concept_sectors",
    "industry_sectors",
    "candidates",
    "portfolio",
    "risk_state",
    "plan",
    "diagnostics",
}


@dataclass
class StockUniverseItem:
    """Normalized A-share stock context used by the trend engines."""

    code: str
    name: str
    latest: Dict[str, Any]
    previous: Dict[str, Any]
    history: pd.DataFrame
    industry: Optional[str]
    concepts: List[str]
    avg_amount_b: float
    float_market_cap_b: Optional[float]


class TrendSystemService:
    """Main application service for the full-market trend system."""

    _shared_fetcher_manager: Optional[DataFetcherManager] = None
    _shared_market_service: Optional[MarketService] = None
    _snapshot_condition = threading.Condition()
    _snapshot_rebuilding = False
    _last_recompute_started_at: Optional[str] = None
    _last_recompute_finished_at: Optional[str] = None
    _last_recompute_error: Optional[str] = None
    _daily_snapshot_cache: Optional[Dict[str, Any]] = None
    _symbol_map_cache: Dict[str, Any] = {"as_of": None, "data": {}}
    _sector_membership_cache: Dict[str, Dict[str, Any]] = {}
    _float_cap_cache: Dict[str, Dict[str, Any]] = {}

    def __init__(
        self,
        config: Optional[Config] = None,
        db: Optional[DatabaseManager] = None,
        fetcher_manager: Optional[DataFetcherManager] = None,
        market_service: Optional[MarketService] = None,
    ) -> None:
        self.config = config or get_config()
        self.db = db or get_db()
        if fetcher_manager is not None:
            self.fetcher_manager = fetcher_manager
        else:
            if self.__class__._shared_fetcher_manager is None:
                self.__class__._shared_fetcher_manager = DataFetcherManager()
            self.fetcher_manager = self.__class__._shared_fetcher_manager

        if market_service is not None:
            self.market_service = market_service
        else:
            if self.__class__._shared_market_service is None:
                self.__class__._shared_market_service = MarketService()
            self.market_service = self.__class__._shared_market_service

    # ------------------------------------------------------------------
    # Public APIs
    # ------------------------------------------------------------------

    def get_overview(self) -> Dict[str, Any]:
        """Return the latest daily overview."""
        return self._get_daily_snapshot_bundle()["overview"]

    def get_position(self) -> Dict[str, Any]:
        """Return index-based position sizing details."""
        return self._get_daily_snapshot_bundle()["position"]

    def get_sectors(self, view: str) -> List[Dict[str, Any]]:
        """Return sector decisions from the latest snapshot."""
        snapshot = self._get_daily_snapshot_bundle()
        return snapshot["concept_sectors"] if view == "concept" else snapshot["industry_sectors"]

    def get_candidates(self) -> List[Dict[str, Any]]:
        """Return trade candidates from the latest snapshot."""
        return self._get_daily_snapshot_bundle()["candidates"]

    def get_risk_state(self) -> Dict[str, Any]:
        """Return discipline state from the latest snapshot."""
        return self._get_daily_snapshot_bundle()["risk_state"]

    def get_plan(self) -> Dict[str, Any]:
        """Return next-day plan from the latest snapshot."""
        return self._get_daily_snapshot_bundle()["plan"]

    def get_portfolio(self) -> Dict[str, Any]:
        """Return portfolio management analysis."""
        return self._get_daily_snapshot_bundle()["portfolio"]

    def get_diagnostics(self) -> Dict[str, Any]:
        """Return scan coverage and filtering diagnostics."""
        return self._get_daily_snapshot_bundle()["diagnostics"]

    def get_status(self) -> Dict[str, Any]:
        """Return snapshot status summary."""
        daily = self.db.get_latest_trend_daily_snapshot()
        preopen = self.db.get_latest_trend_preopen_snapshot()
        return {
            "as_of": date.today().isoformat(),
            "daily_snapshot": self._snapshot_status_payload(daily),
            "preopen_snapshot": self._snapshot_status_payload(preopen),
            "recompute_state": {
                "running": self.__class__._snapshot_rebuilding,
                "started_at": self.__class__._last_recompute_started_at,
                "finished_at": self.__class__._last_recompute_finished_at,
                "last_error": self.__class__._last_recompute_error,
            },
        }

    def recompute(self, snapshot_type: str = "manual_recompute", wait: bool = True) -> Dict[str, Any]:
        """Rebuild daily and pre-open snapshots (sync or background mode)."""
        if wait:
            return self._recompute_singleflight(snapshot_type=snapshot_type)
        return self._start_recompute_in_background(snapshot_type=snapshot_type)

    def get_preopen_snapshot(self) -> Dict[str, Any]:
        """Return latest pre-open snapshot without triggering market rebuild."""
        existing = self.db.get_latest_trend_preopen_snapshot()
        if existing:
            payload = existing.get("payload", {})
            if payload:
                return payload
        daily = self._get_daily_snapshot_bundle()
        return self._build_empty_preopen_snapshot(daily_snapshot=daily)

    def create_stage_override(
        self,
        sector_view: str,
        sector_key: str,
        sector_name: str,
        original_stage: str,
        target_stage: str,
        reason: str,
        operator: str = "web",
    ) -> Dict[str, Any]:
        """Persist one manual stage downgrade."""
        if target_stage not in DOWNGRADE_RULES.get(original_stage, set()):
            raise ValueError("Only downward stage overrides are allowed")
        result = self.db.create_or_update_trend_stage_override(
            {
                "override_date": date.today(),
                "sector_view": sector_view,
                "sector_key": sector_key,
                "sector_name": sector_name,
                "original_stage": original_stage,
                "target_stage": target_stage,
                "reason": reason,
                "operator": operator,
            }
        )
        self._patch_snapshot_after_stage_override(
            sector_view=sector_view,
            sector_key=sector_key,
            target_stage=target_stage,
        )
        return result

    def list_trades(
        self,
        code: Optional[str] = None,
        sector_key: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """List manual trade journal records."""
        return self.db.list_trend_trade_records(code=code, sector_key=sector_key, status=status, limit=limit)

    def create_trade(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Create one trade-journal record."""
        created = self.db.create_trend_trade_record(payload)
        self._save_risk_state_snapshot()
        self._refresh_latest_snapshot_state()
        return created

    def update_trade(self, trade_id: int, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update one trade-journal record."""
        updated = self.db.update_trend_trade_record(trade_id, updates)
        self._save_risk_state_snapshot()
        self._refresh_latest_snapshot_state()
        return updated

    def list_positions(
        self,
        code: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """List holding records."""
        return self.db.list_trend_positions(code=code, status=status, limit=limit)

    def create_position(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Create one manual position and its linked trade-journal row."""
        trade_payload = {
            "code": payload["code"],
            "name": payload.get("name"),
            "sector_view": payload.get("sector_view", "concept"),
            "sector_key": payload.get("sector_key"),
            "sector_name": payload.get("sector_name"),
            "open_date": payload["open_date"],
            "open_type": payload["open_type"],
            "entry_price": payload["entry_price"],
            "initial_stop_loss": payload.get("initial_stop_loss"),
            "position_pct": payload["position_pct"],
            "is_elite_strategy": payload.get("is_elite_strategy", False),
        }
        trade = self.db.create_trend_trade_record(trade_payload)
        position_payload = {
            **payload,
            "linked_trade_id": trade["id"],
            "status": payload.get("status", "open"),
            "current_stop_loss": payload.get("current_stop_loss", payload.get("initial_stop_loss")),
        }
        created = self.db.create_trend_position(position_payload)
        self._save_risk_state_snapshot()
        self._refresh_latest_snapshot_state()
        return created

    def update_position(self, position_id: int, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update one position and sync linked trade-journal status when needed."""
        existing = next((item for item in self.db.list_trend_positions(limit=500) if item["id"] == position_id), None)
        updated = self.db.update_trend_position(position_id, updates)
        if updated and existing and existing.get("linked_trade_id"):
            trade_updates: Dict[str, Any] = {}
            if "close_date" in updates:
                trade_updates["close_date"] = updates["close_date"]
            if "exit_price" in updates:
                trade_updates["exit_price"] = updates["exit_price"]
            if "exit_reason" in updates:
                trade_updates["exit_reason"] = updates["exit_reason"]
            if "status" in updates and updates["status"] == "closed":
                trade_updates.setdefault("close_date", updates.get("close_date", date.today()))
            if "notes" in updates and updates["notes"]:
                trade_updates.setdefault("exit_reason", updates["notes"])
            if trade_updates:
                self.db.update_trend_trade_record(existing["linked_trade_id"], trade_updates)
        self._save_risk_state_snapshot()
        self._refresh_latest_snapshot_state()
        return updated

    def ack_alert(self, alert_id: int) -> Optional[Dict[str, Any]]:
        """Acknowledge one alert."""
        return self.db.ack_trend_intraday_alert(alert_id)

    def list_alerts(self, days: int = 5, limit: int = 100) -> List[Dict[str, Any]]:
        """List recent alerts."""
        return self.db.list_trend_intraday_alerts(days=days, limit=limit)

    # ------------------------------------------------------------------
    # Snapshot orchestration
    # ------------------------------------------------------------------

    def _get_daily_snapshot_bundle(self) -> Dict[str, Any]:
        with self.__class__._snapshot_condition:
            if self.__class__._daily_snapshot_cache is not None:
                return self._clone_snapshot(self.__class__._daily_snapshot_cache)

        snapshot_row = self._get_latest_compatible_daily_snapshot()
        if snapshot_row is None:
            empty = self._build_empty_daily_snapshot(status="missing", reason="尚未生成趋势系统快照，请手动点击“重算”。")
            with self.__class__._snapshot_condition:
                self.__class__._daily_snapshot_cache = self._clone_snapshot(empty)
            return empty

        payload = snapshot_row.get("payload", {})
        snapshot_date = snapshot_row.get("snapshot_date")
        is_today = snapshot_date == date.today().isoformat()
        enriched = self._clone_snapshot(payload)
        enriched["overview"]["snapshot_status"] = "ready" if is_today else "stale"
        enriched["overview"]["generated_at"] = payload.get("generated_at")
        if not is_today and not enriched["overview"].get("empty_reason"):
            enriched["overview"]["empty_reason"] = f"当前展示的是 {snapshot_date} 的最近快照。"
        with self.__class__._snapshot_condition:
            self.__class__._daily_snapshot_cache = self._clone_snapshot(enriched)
        return enriched

    def _get_latest_compatible_daily_snapshot(self) -> Optional[Dict[str, Any]]:
        latest = self.db.get_latest_trend_daily_snapshot()
        if not latest:
            return None
        payload = latest.get("payload", {})
        if self._is_compatible_daily_snapshot(payload):
            return latest
        snapshot_date_text = latest.get("snapshot_date")
        if snapshot_date_text:
            try:
                snapshot_date = date.fromisoformat(snapshot_date_text)
            except ValueError:
                snapshot_date = date.today()
        else:
            snapshot_date = date.today()

        logger.warning("Trend system found legacy/incompatible daily snapshot, auto-invalidating it.")
        upgraded_payload = self._build_empty_daily_snapshot(
            status="legacy",
            reason="检测到旧版快照结构，已自动失效，请手动点击“重算”。",
            source="auto_invalidate_legacy_snapshot",
        )
        upgraded_payload["as_of"] = snapshot_date.isoformat()
        self.db.save_trend_daily_snapshot(snapshot_date, upgraded_payload)
        return self.db.get_latest_trend_daily_snapshot()

    def _recompute_singleflight(self, snapshot_type: str) -> Dict[str, Any]:
        condition = self.__class__._snapshot_condition
        with condition:
            if self.__class__._snapshot_rebuilding:
                condition.wait_for(lambda: not self.__class__._snapshot_rebuilding, timeout=180)
                latest = self._get_latest_compatible_daily_snapshot()
                payload = latest.get("payload", {}) if latest else self._build_empty_daily_snapshot(
                    status="missing",
                    reason="等待中的重算未产出可用快照。",
                )
                return {
                    "status": payload.get("status", "ready"),
                    "snapshot_type": snapshot_type,
                    "generated_at": payload.get("generated_at") or datetime.now().isoformat(),
                    "snapshot_date": payload.get("as_of") or date.today().isoformat(),
                }

            self.__class__._snapshot_rebuilding = True
            self.__class__._last_recompute_started_at = datetime.now().isoformat()
            self.__class__._last_recompute_error = None

        try:
            return self._run_recompute_pipeline(snapshot_type=snapshot_type)
        finally:
            with condition:
                self.__class__._snapshot_rebuilding = False
                self.__class__._last_recompute_finished_at = datetime.now().isoformat()
                condition.notify_all()

    def _start_recompute_in_background(self, snapshot_type: str) -> Dict[str, Any]:
        condition = self.__class__._snapshot_condition
        with condition:
            if self.__class__._snapshot_rebuilding:
                latest = self._get_latest_compatible_daily_snapshot()
                payload = latest.get("payload", {}) if latest else {}
                return {
                    "status": "running",
                    "snapshot_type": snapshot_type,
                    "generated_at": self.__class__._last_recompute_started_at or datetime.now().isoformat(),
                    "snapshot_date": payload.get("as_of") or date.today().isoformat(),
                }

            self.__class__._snapshot_rebuilding = True
            self.__class__._last_recompute_started_at = datetime.now().isoformat()
            self.__class__._last_recompute_error = None

            worker = threading.Thread(
                target=self._recompute_background_worker,
                args=(snapshot_type,),
                daemon=True,
                name="trend-system-recompute",
            )
            worker.start()

            return {
                "status": "running",
                "snapshot_type": snapshot_type,
                "generated_at": self.__class__._last_recompute_started_at,
                "snapshot_date": date.today().isoformat(),
            }

    def _recompute_background_worker(self, snapshot_type: str) -> None:
        try:
            self._run_recompute_pipeline(snapshot_type=snapshot_type)
        finally:
            with self.__class__._snapshot_condition:
                self.__class__._snapshot_rebuilding = False
                self.__class__._last_recompute_finished_at = datetime.now().isoformat()
                self.__class__._snapshot_condition.notify_all()

    def _run_recompute_pipeline(self, snapshot_type: str) -> Dict[str, Any]:
        started_at = datetime.now()
        try:
            logger.info("Trend system recompute started (snapshot_type=%s)", snapshot_type)
            daily_snapshot = self._build_daily_snapshot(source=snapshot_type)
            self._save_daily_bundle(daily_snapshot)
            preopen_snapshot = self._build_preopen_snapshot(daily_snapshot, source=snapshot_type)
            self.db.save_trend_preopen_snapshot(date.today(), preopen_snapshot)
            with self.__class__._snapshot_condition:
                self.__class__._daily_snapshot_cache = self._clone_snapshot(daily_snapshot)

            elapsed = (datetime.now() - started_at).total_seconds()
            logger.info(
                "Trend system recompute finished (snapshot_type=%s, status=%s, elapsed=%.2fs)",
                snapshot_type,
                daily_snapshot.get("status", "ready"),
                elapsed,
            )
            return {
                "status": daily_snapshot.get("status", "ready"),
                "snapshot_type": snapshot_type,
                "generated_at": daily_snapshot["generated_at"],
                "snapshot_date": daily_snapshot["as_of"],
            }
        except Exception as exc:
            self.__class__._last_recompute_error = str(exc)
            failed_snapshot = self._build_empty_daily_snapshot(status="failed", reason=f"重算失败：{exc}")
            self.db.save_trend_daily_snapshot(date.today(), failed_snapshot)
            with self.__class__._snapshot_condition:
                self.__class__._daily_snapshot_cache = self._clone_snapshot(failed_snapshot)

            elapsed = (datetime.now() - started_at).total_seconds()
            logger.error(
                "Trend system recompute failed (snapshot_type=%s, elapsed=%.2fs): %s",
                snapshot_type,
                elapsed,
                exc,
                exc_info=True,
            )
            return {
                "status": "failed",
                "snapshot_type": snapshot_type,
                "generated_at": failed_snapshot["generated_at"],
                "snapshot_date": failed_snapshot["as_of"],
            }

    def _save_daily_bundle(self, daily_snapshot: Dict[str, Any]) -> None:
        snapshot_date = date.fromisoformat(daily_snapshot["as_of"])
        self.db.save_trend_daily_snapshot(snapshot_date, daily_snapshot)
        self.db.save_trend_risk_state_snapshot(snapshot_date, daily_snapshot["risk_state"])
        self.db.save_trend_diagnostics_snapshot(snapshot_date, daily_snapshot["diagnostics"])

        for alert in daily_snapshot.get("alerts", []):
            self.db.create_trend_intraday_alert(alert)

    def _save_risk_state_snapshot(self) -> None:
        self.db.save_trend_risk_state_snapshot(date.today(), self._compute_risk_state())

    def _build_daily_snapshot(self, source: str) -> Dict[str, Any]:
        try:
            universe, diagnostics = self._build_market_universe(resolve_float_caps=False)
            position = self._compute_position()
            market_context = self._safe_market_context()
            concept_sectors = self._aggregate_sectors("concept", universe, market_context)
            industry_sectors = self._aggregate_sectors("industry", universe, market_context)
            risk_state = self._compute_risk_state(concept_sectors, industry_sectors)
            if risk_state["flags"].get("degraded_system"):
                self._enrich_float_caps(universe)
                diagnostics["float_cap_resolved"] = sum(1 for item in universe if item.float_market_cap_b is not None)
            candidates, candidate_diag = self._build_candidates(
                concept_sectors,
                industry_sectors,
                universe,
                risk_state,
                position["recommended_position_pct"],
            )
            portfolio = self._build_portfolio(universe, concept_sectors)
            alerts = self._build_alerts(portfolio, concept_sectors)
            diagnostics["candidate_filters"] = candidate_diag
            diagnostics["alert_count"] = len(alerts)
            diagnostics["open_position_count"] = portfolio["summary"]["open_count"]
            diagnostics["open_position_pct"] = portfolio["summary"]["total_position_pct"]
            overview = self._build_overview(position, concept_sectors, risk_state, candidates, diagnostics)
            plan = self._build_plan(overview, candidates, risk_state, portfolio, diagnostics)
            generated_at = datetime.now().isoformat()
            return {
                "as_of": date.today().isoformat(),
                "generated_at": generated_at,
                "source": source,
                "snapshot_type": "daily_close",
                "status": "ready",
                "position": position,
                "concept_sectors": concept_sectors,
                "industry_sectors": industry_sectors,
                "candidates": candidates,
                "portfolio": portfolio,
                "risk_state": risk_state,
                "plan": plan,
                "diagnostics": diagnostics,
                "alerts": alerts,
                "overview": overview,
            }
        except Exception as exc:
            logger.error("Trend system daily snapshot build failed: %s", exc, exc_info=True)
            return self._build_empty_daily_snapshot(status="failed", reason=f"重算失败：{exc}", source=source)

    def _build_preopen_snapshot(self, daily_snapshot: Dict[str, Any], source: str) -> Dict[str, Any]:
        risk_state = daily_snapshot["risk_state"]
        candidates = daily_snapshot["candidates"]
        actionable = [item for item in candidates if item["actionable"]][:5]
        blocked_signal_types = []
        if risk_state["flags"].get("breakout_paused"):
            blocked_signal_types.extend(["breakout", "compensation"])
        if risk_state["flags"].get("cooldown"):
            blocked_signal_types = ["all_new_entries"]
        return {
            "snapshot_type": "preopen",
            "source": source,
            "as_of": date.today().isoformat(),
            "generated_at": datetime.now().isoformat(),
            "recommended_position_pct": daily_snapshot["position"]["recommended_position_pct"],
            "trade_allowed": daily_snapshot["overview"]["trade_allowed"],
            "main_sectors": daily_snapshot["overview"]["main_sectors"],
            "primary_stage": daily_snapshot["overview"]["primary_stage"],
            "risk_state": risk_state,
            "allowed_signal_types": sorted({item["signal_type"] for item in actionable}),
            "blocked_signal_types": blocked_signal_types,
            "watch_items": actionable,
            "discipline_checks": [
                "Confirm total position before open.",
                "Only execute planned entries.",
                "No rule changes intraday.",
            ],
        }

    # ------------------------------------------------------------------
    # Universe & scanners
    # ------------------------------------------------------------------

    def _build_market_universe(
        self,
        resolve_float_caps: bool = False,
    ) -> Tuple[List[StockUniverseItem], Dict[str, Any]]:
        symbol_map = self._load_symbol_map()
        db_codes = self.db.list_latest_stock_codes(limit=MAX_UNIVERSE_SCAN * 2)
        diagnostics = {
            "mode": "full_market_snapshot",
            "total_market_symbols": len(symbol_map),
            "db_backed_symbols": len(db_codes),
            "scan_limit": MAX_UNIVERSE_SCAN,
            "etf_excluded": 0,
            "index_excluded": 0,
            "missing_history": 0,
            "sector_resolution_failures": 0,
            "sector_resolved": 0,
            "float_cap_resolved": 0,
            "source_notes": [],
        }

        selected_codes: List[str] = []
        for code in db_codes:
            if self._is_index_code(code):
                diagnostics["index_excluded"] += 1
                continue
            name = symbol_map.get(code, "")
            if self._is_etf_code(code) or self._is_etf_name(name):
                diagnostics["etf_excluded"] += 1
                continue
            selected_codes.append(code)
            if len(selected_codes) >= MAX_UNIVERSE_SCAN:
                break

        if not symbol_map:
            diagnostics["source_notes"].append("Bulk market symbol list unavailable; using DB-backed codes only.")
        if not selected_codes:
            diagnostics["source_notes"].append("No DB-backed A-share symbols available for full-market scan.")

        universe: List[StockUniverseItem] = []
        for code in selected_codes:
            try:
                history = self._get_stock_history(code, days=260)
                if history.empty:
                    diagnostics["missing_history"] += 1
                    continue

                name = symbol_map.get(code) or code
                latest = history.iloc[-1].to_dict()
                previous = history.iloc[-2].to_dict() if len(history) >= 2 else latest
                industry, concepts = self._resolve_sector_membership(code)
                if industry or concepts:
                    diagnostics["sector_resolved"] += 1
                else:
                    diagnostics["sector_resolution_failures"] += 1
                float_market_cap_b = self._resolve_float_market_cap_b(code) if resolve_float_caps else None
                if float_market_cap_b is not None:
                    diagnostics["float_cap_resolved"] += 1

                universe.append(
                    StockUniverseItem(
                        code=code,
                        name=name or code,
                        latest=latest,
                        previous=previous,
                        history=history,
                        industry=industry,
                        concepts=concepts,
                        avg_amount_b=self._avg_amount_b(history),
                        float_market_cap_b=float_market_cap_b,
                    )
                )
            except Exception as exc:
                diagnostics["missing_history"] += 1
                logger.warning("Trend system failed to build universe item for %s: %s", code, exc)
                continue

        diagnostics["scanned_symbols"] = len(universe)
        diagnostics["coverage_ratio"] = self._safe_round(
            len(universe) / max(diagnostics["total_market_symbols"] or len(db_codes), 1) * 100, 2
        )
        return universe, diagnostics

    def _load_symbol_map(self) -> Dict[str, str]:
        cached = self.__class__._symbol_map_cache
        if cached["as_of"] == date.today().isoformat() and cached["data"]:
            return dict(cached["data"])

        symbol_map: Dict[str, str] = {}
        for fetcher in self._iter_preferred_fetchers():
            if not hasattr(fetcher, "get_stock_list"):
                continue
            try:
                stock_list = fetcher.get_stock_list()
            except Exception as exc:
                logger.debug("Trend system get_stock_list failed via %s: %s", getattr(fetcher, "name", "unknown"), exc)
                continue

            if stock_list is None or stock_list.empty:
                continue
            if "code" not in stock_list.columns or "name" not in stock_list.columns:
                continue

            for _, row in stock_list.iterrows():
                code = str(row.get("code") or "").strip()
                name = str(row.get("name") or "").strip()
                if code and name and code.isdigit():
                    symbol_map.setdefault(code, name)
            if symbol_map:
                break
        if symbol_map:
            self.__class__._symbol_map_cache = {"as_of": date.today().isoformat(), "data": dict(symbol_map)}
        return symbol_map

    def _get_stock_history(self, code: str, days: int = 260) -> pd.DataFrame:
        end_date = date.today()
        start_date = end_date - timedelta(days=max(days * 2, 120))
        bars = self.db.get_data_range(code, start_date, end_date)
        if not bars:
            return pd.DataFrame()

        df = pd.DataFrame([bar.to_dict() for bar in bars])
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        return self._normalize_price_history(df)

    def _compute_position(self) -> Dict[str, Any]:
        index_df = self._get_stock_history("sh000001", days=260)
        if index_df.empty:
            rules = [
                {"key": "annual_trend", "label": "年线多头", "matched": False, "value": None},
                {"key": "ma_alignment", "label": "均线多头", "matched": False, "value": None},
                {"key": "up_days", "label": "上涨天数多", "matched": False, "value": 0},
            ]
            return {"recommended_position_pct": 20, "matched_rules": 0, "rules": rules, "index_code": "sh000001"}

        latest = index_df.iloc[-1]
        annual_trend = bool(latest.get("close", 0) > latest.get("ma250", 0))
        ma_alignment = bool(latest.get("ma5", 0) > latest.get("ma10", 0) > latest.get("ma20", 0))
        recent = index_df.tail(10)
        up_days = int((recent["pct_chg"].fillna(0) > 0).sum()) if not recent.empty else 0
        up_days_matched = up_days >= 6
        matched = sum([annual_trend, ma_alignment, up_days_matched])

        return {
            "recommended_position_pct": 70 if matched >= 2 else 30 if matched == 1 else 20,
            "matched_rules": matched,
            "rules": [
                {
                    "key": "annual_trend",
                    "label": "年线多头",
                    "matched": annual_trend,
                    "value": self._safe_round(latest.get("ma250")),
                },
                {
                    "key": "ma_alignment",
                    "label": "均线多头",
                    "matched": ma_alignment,
                    "value": {
                        "ma5": self._safe_round(latest.get("ma5")),
                        "ma10": self._safe_round(latest.get("ma10")),
                        "ma20": self._safe_round(latest.get("ma20")),
                    },
                },
                {"key": "up_days", "label": "上涨天数多", "matched": up_days_matched, "value": up_days},
            ],
            "index_code": "sh000001",
        }

    def _aggregate_sectors(
        self,
        view: str,
        universe: List[StockUniverseItem],
        market_context: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        sector_map: Dict[str, Dict[str, Any]] = {}
        top_sector_names = {str(item.get("name")) for item in (market_context or {}).get("top_sectors", [])}

        for item in universe:
            sector_names = item.concepts if view == "concept" else ([item.industry] if item.industry else [])
            for sector_name in sector_names:
                if not sector_name:
                    continue
                sector_key = f"{view}:{sector_name}"
                sector_entry = sector_map.setdefault(
                    sector_key,
                    {"sector_key": sector_key, "sector_name": sector_name, "sector_view": view, "members": []},
                )
                sector_entry["members"].append(item)

        sectors: List[Dict[str, Any]] = []
        for sector in sector_map.values():
            members: List[StockUniverseItem] = sector["members"]
            ranked = self._rank_sector_members(members)
            leader = ranked[0] if ranked else None
            leader2 = ranked[1] if len(ranked) > 1 else None
            frontline = ranked[:5]
            latest_amount_b = sum(float(member.latest.get("amount") or 0.0) / 1e8 for member in members)
            strong_member_count = sum(1 for member in members if float(member.latest.get("pct_chg") or 0.0) > 5.0)
            limit_up_count = sum(1 for member in members if float(member.latest.get("pct_chg") or 0.0) >= LIMIT_UP_PCT)
            recent_2d_gt7 = any(self._recent_change(member.history, 2) > 7 for member in members)
            recent_3d_limit = any(self._has_limit_up(member.history, 3) for member in members)
            quant_stage, stage_meta = self._infer_market_stage(leader, members, top_sector_names)
            stage_meta = {
                **stage_meta,
                "leader_pullback_pct": (
                    self._safe_round(self._intraday_pullback(pd.Series(leader.latest)), 2) if leader else 0.0
                ),
                "leader_pct_chg": self._safe_round(float(leader.latest.get("pct_chg") or 0.0), 2) if leader else 0.0,
            }
            final_stage, override = self._apply_stage_override(view, sector["sector_key"], quant_stage)

            sectors.append(
                {
                    "sector_key": sector["sector_key"],
                    "sector_name": sector["sector_name"],
                    "sector_view": view,
                    "member_count": len(members),
                    "latest_amount_b": self._safe_round(latest_amount_b, 2) or 0.0,
                    "top_amount_rank": 0,
                    "matched_conditions": 0,
                    "conditions": {
                        "top_amount_rank": False,
                        "leader_strength": recent_2d_gt7 or recent_3d_limit,
                        "breadth": strong_member_count >= 3,
                    },
                    "trade_allowed": False,
                    "quant_stage": quant_stage,
                    "final_stage": final_stage,
                    "stage_meta": stage_meta,
                    "override": override,
                    "leader": self._member_brief(leader),
                    "leader_2": self._member_brief(leader2),
                    "frontline_members": [self._member_brief(item) for item in frontline],
                    "sector_breadth": {
                        "strong_member_count": strong_member_count,
                        "limit_up_count": limit_up_count,
                        "top5_avg_pct": self._safe_round(
                            sum(float(member.latest.get("pct_chg") or 0.0) for member in frontline[:5])
                            / max(min(len(frontline), 5), 1),
                            2,
                        ),
                        "consistency_score": min(strong_member_count * 10 + limit_up_count * 8, 100),
                    },
                    "members": [
                        {
                            "code": member.code,
                            "name": member.name,
                            "pct_chg": self._safe_round(member.latest.get("pct_chg")) or 0.0,
                        }
                        for member in frontline
                    ],
                }
            )

        sectors.sort(key=lambda item: (item["latest_amount_b"], item["member_count"]), reverse=True)
        for index, sector in enumerate(sectors):
            sector["top_amount_rank"] = index + 1
            sector["conditions"]["top_amount_rank"] = index < 3
            matched = sum(1 for flag in sector["conditions"].values() if flag)
            sector["matched_conditions"] = matched
            sector["trade_allowed"] = matched >= 2
        return sectors

    def _build_candidates(
        self,
        concept_sectors: List[Dict[str, Any]],
        industry_sectors: List[Dict[str, Any]],
        universe: List[StockUniverseItem],
        risk_state: Dict[str, Any],
        total_position_cap_pct: int,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        preferred = [sector for sector in concept_sectors if sector["trade_allowed"]]
        if not preferred:
            preferred = [sector for sector in industry_sectors if sector["trade_allowed"]]

        open_positions = self.db.list_trend_positions(status="open", limit=200)
        total_used_pct = sum(float(item.get("position_pct") or 0.0) for item in open_positions)
        sector_used_pct: Dict[str, float] = {}
        item_used_pct: Dict[str, float] = {}
        for item in open_positions:
            sector_key = item.get("sector_key") or ""
            code = str(item.get("code") or "")
            used_pct = float(item.get("position_pct") or 0.0)
            sector_used_pct[sector_key] = sector_used_pct.get(sector_key, 0.0) + used_pct
            item_used_pct[code] = item_used_pct.get(code, 0.0) + used_pct

        sector_lookup = {
            member["code"]: sector
            for sector in preferred
            for member in sector["frontline_members"] + sector["members"]
        }
        elite_lookup = {
            sector["sector_key"]: [
                item.get("code")
                for item in [sector.get("leader"), sector.get("leader_2")]
                if item
            ]
            for sector in preferred
        }

        filter_stats = {
            "total_scanned": len(universe),
            "outside_mainline": 0,
            "below_ma60": 0,
            "insufficient_momentum": 0,
            "blocked_by_stage": 0,
            "blocked_by_degraded_filter": 0,
            "blocked_by_risk_mode": 0,
            "actionable_candidates": 0,
        }
        candidates: List[Dict[str, Any]] = []

        for item in universe:
            sector = sector_lookup.get(item.code)
            if sector is None:
                filter_stats["outside_mainline"] += 1
                continue

            latest_close = float(item.latest.get("close") or 0.0)
            ma60 = float(item.history["close"].tail(60).mean()) if len(item.history) >= 60 else 0.0
            gain_20d = self._recent_change(item.history, 20)
            gain_60d = self._recent_change(item.history, 60)
            if latest_close <= ma60:
                filter_stats["below_ma60"] += 1
                continue
            if max(gain_20d, gain_60d) < 10:
                filter_stats["insufficient_momentum"] += 1
                continue

            if risk_state["flags"].get("degraded_system"):
                # Missing market-cap data should not block candidate generation.
                if (
                    item.float_market_cap_b is not None
                    and item.float_market_cap_b < self.config.trend_system_degraded_min_float_market_cap_b
                ):
                    filter_stats["blocked_by_degraded_filter"] += 1
                    continue
                if item.avg_amount_b < self.config.trend_system_degraded_min_avg_amount_b:
                    filter_stats["blocked_by_degraded_filter"] += 1
                    continue

            signal = self._infer_entry_signal(item, sector, risk_state)
            if signal is None:
                filter_stats["blocked_by_stage"] += 1
                continue

            is_elite = item.code in elite_lookup.get(sector["sector_key"], [])

            candidate = {
                "code": item.code,
                "name": item.name,
                "sector_key": sector["sector_key"],
                "sector_name": sector["sector_name"],
                "sector_view": sector["sector_view"],
                "final_stage": sector["final_stage"],
                "signal_type": signal["signal_type"],
                "signal_label": signal["signal_label"],
                "signal_score": signal["signal_score"],
                "actionable": False,
                "action_block_reason": None,
                "suggested_entry": signal["suggested_entry"],
                "invalid_if": signal["invalid_if"],
                "stop_loss": signal["stop_loss"],
                "position_limit_pct": signal["position_limit_pct"],
                "recommended_position_pct": 0,
                "reason_checks": signal["reason_checks"],
                "is_elite_candidate": is_elite,
                "latest_close": self._safe_round(latest_close),
                "gain_20d": self._safe_round(gain_20d),
                "gain_60d": self._safe_round(gain_60d),
                "filter_reasons": [],
                "_signal_actionable": bool(signal["actionable"]),
            }
            candidates.append(candidate)

        candidates = self._apply_candidate_allocation_constraints(
            candidates,
            risk_state,
            total_position_cap_pct,
            total_used_pct,
            sector_used_pct,
            item_used_pct,
        )
        filter_stats["actionable_candidates"] = sum(1 for item in candidates if item["actionable"])
        filter_stats["blocked_by_risk_mode"] = sum(1 for item in candidates if not item["actionable"])
        return candidates, filter_stats

    def _build_portfolio(
        self,
        universe: List[StockUniverseItem],
        concept_sectors: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        open_positions = self.db.list_trend_positions(status="open", limit=200)
        universe_lookup = {item.code: item for item in universe}
        sector_lookup = {sector["sector_key"]: sector for sector in concept_sectors}
        items: List[Dict[str, Any]] = []

        for position in open_positions:
            item = universe_lookup.get(position["code"])
            if item is None:
                history = self._get_stock_history(position["code"], days=260)
                if history.empty:
                    continue
                item = StockUniverseItem(
                    code=position["code"],
                    name=position.get("name") or position["code"],
                    latest=history.iloc[-1].to_dict(),
                    previous=history.iloc[-2].to_dict() if len(history) >= 2 else history.iloc[-1].to_dict(),
                    history=history,
                    industry=position.get("sector_name"),
                    concepts=[],
                    avg_amount_b=self._avg_amount_b(history),
                    float_market_cap_b=None,
                )

            latest = item.history.iloc[-1]
            latest_close = float(latest.get("close") or 0.0)
            entry_price = float(position["entry_price"] or 0.0)
            profit_pct = (latest_close / entry_price - 1) * 100 if entry_price > 0 else 0.0
            action, action_reason, suggested_sell_pct = self._evaluate_position_action(
                item,
                position,
                sector_lookup.get(position.get("sector_key") or ""),
            )

            items.append(
                {
                    "id": position["id"],
                    "code": position["code"],
                    "name": position.get("name"),
                    "sector_key": position.get("sector_key"),
                    "sector_name": position.get("sector_name"),
                    "open_type": position["open_type"],
                    "position_pct": position["position_pct"],
                    "entry_price": position["entry_price"],
                    "latest_close": self._safe_round(latest_close),
                    "profit_pct": self._safe_round(profit_pct),
                    "initial_stop_loss": position.get("initial_stop_loss"),
                    "current_stop_loss": position.get("current_stop_loss") or position.get("initial_stop_loss"),
                    "trend_exit_line": self._safe_round(latest.get("ma10")),
                    "take_profit_stage": position.get("take_profit_stage", 0),
                    "action": action,
                    "action_reason": action_reason,
                    "suggested_sell_pct": suggested_sell_pct,
                    "signals": self._position_signals(
                        item,
                        position,
                        sector_lookup.get(position.get("sector_key") or ""),
                    ),
                    "status": position.get("status", "open"),
                }
            )

        items.sort(
            key=lambda row: (
                row["action"] != "hold",
                row["suggested_sell_pct"],
                abs(row["profit_pct"] or 0.0),
            ),
            reverse=True,
        )
        return {
            "summary": {
                "open_count": len(items),
                "total_position_pct": (
                    self._safe_round(sum(float(item["position_pct"] or 0.0) for item in items), 2) or 0.0
                ),
                "reduce_count": sum(1 for item in items if item["action"] == "reduce"),
                "exit_count": sum(1 for item in items if item["action"] == "exit"),
            },
            "items": items,
        }

    def _build_alerts(self, portfolio: Dict[str, Any], concept_sectors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        alerts: List[Dict[str, Any]] = []
        leader_lookup = {sector["sector_key"]: sector.get("leader") for sector in concept_sectors}

        for item in portfolio.get("items", []):
            if item["action"] == "hold":
                continue
            alerts.append(
                {
                    "alert_date": date.today(),
                    "alert_type": "stop_loss" if item["action"] == "exit" else "take_profit",
                    "code": item["code"],
                    "name": item.get("name"),
                    "sector_key": item.get("sector_key"),
                    "message": f"{item['code']} {item['action_reason']}",
                    "payload": item,
                }
            )

        for sector in concept_sectors[:3]:
            leader = leader_lookup.get(sector["sector_key"])
            if not leader:
                continue
            leader_pct = float(sector.get("stage_meta", {}).get("leader_pct_chg") or leader.get("pct_chg") or 0.0)
            leader_pullback = float(sector.get("stage_meta", {}).get("leader_pullback_pct") or 0.0)
            if leader_pct <= -9.0:
                alerts.append(
                    {
                        "alert_date": date.today(),
                        "alert_type": "leader_limit_down",
                        "code": leader.get("code"),
                        "name": leader.get("name"),
                        "sector_key": sector["sector_key"],
                        "message": f"{sector['sector_name']} 龙头跌停，建议防守。",
                        "payload": {"sector_key": sector["sector_key"], "leader": leader},
                    }
                )
                continue

            if sector["final_stage"] == "late" and sector["sector_breadth"]["strong_member_count"] <= 1:
                alerts.append(
                    {
                        "alert_date": date.today(),
                        "alert_type": "leader_break",
                        "code": leader.get("code"),
                        "name": leader.get("name"),
                        "sector_key": sector["sector_key"],
                        "message": f"{sector['sector_name']} 出现明显分歧，关注龙头转弱。",
                        "payload": {"sector_key": sector["sector_key"], "leader": leader},
                    }
                )
                continue

            if leader_pullback >= 3.0 and sector["sector_breadth"]["strong_member_count"] <= 2:
                alerts.append(
                    {
                        "alert_date": date.today(),
                        "alert_type": "leader_break",
                        "code": leader.get("code"),
                        "name": leader.get("name"),
                        "sector_key": sector["sector_key"],
                        "message": f"{sector['sector_name']} 龙头冲高回落，关注分歧风险。",
                        "payload": {"sector_key": sector["sector_key"], "leader": leader},
                    }
                )
        return alerts

    def _build_overview(
        self,
        position: Dict[str, Any],
        concept_sectors: List[Dict[str, Any]],
        risk_state: Dict[str, Any],
        candidates: List[Dict[str, Any]],
        diagnostics: Dict[str, Any],
    ) -> Dict[str, Any]:
        main_sectors = [sector for sector in concept_sectors if sector["trade_allowed"]][:3]
        trade_allowed = bool(main_sectors) and risk_state["current_mode"] != "cooldown"
        primary_stage = main_sectors[0]["final_stage"] if main_sectors else "choppy"
        empty_reason = self._build_empty_reason(main_sectors, diagnostics, risk_state)
        return {
            "as_of": date.today().isoformat(),
            "generated_at": datetime.now().isoformat(),
            "position": position,
            "trade_allowed": trade_allowed,
            "trade_gate": "allowed" if trade_allowed else "blocked",
            "primary_stage": primary_stage,
            "main_sectors": [
                {
                    "sector_key": sector["sector_key"],
                    "sector_name": sector["sector_name"],
                    "sector_view": sector["sector_view"],
                    "final_stage": sector["final_stage"],
                    "trade_allowed": sector["trade_allowed"],
                }
                for sector in main_sectors
            ],
            "risk_state": risk_state,
            "candidate_count": len(candidates),
            "snapshot_status": "ready",
            "empty_reason": empty_reason,
        }

    def _build_plan(
        self,
        overview: Dict[str, Any],
        candidates: List[Dict[str, Any]],
        risk_state: Dict[str, Any],
        portfolio: Dict[str, Any],
        diagnostics: Dict[str, Any],
    ) -> Dict[str, Any]:
        actionable = [item for item in candidates if item["actionable"]][:5]
        blocked = []
        if risk_state["flags"].get("cooldown"):
            blocked.append("冷静期内禁止新开仓")
        if risk_state["flags"].get("breakout_paused"):
            blocked.append("暂停突破交易")
        if risk_state["flags"].get("elite_disabled"):
            blocked.append("精英策略暂时禁用")
        return {
            "generated_at": datetime.now().isoformat(),
            "recommended_position_pct": overview["position"]["recommended_position_pct"],
            "trade_allowed": overview["trade_allowed"],
            "main_sectors": overview["main_sectors"],
            "primary_stage": overview["primary_stage"],
            "risk_state": risk_state,
            "candidates": actionable,
            "portfolio_actions": [item for item in portfolio.get("items", []) if item["action"] != "hold"],
            "blocked_rules": blocked,
            "discipline_notes": [
                "收盘后检查止损与止盈触发。",
                "盘前只执行计划内买点。",
                "盘中禁止临时改规则或情绪交易。",
            ],
            "empty_reason": overview["empty_reason"],
            "diagnostics_summary": {
                "scanned_symbols": diagnostics.get("scanned_symbols", 0),
                "actionable_candidates": diagnostics.get("candidate_filters", {}).get("actionable_candidates", 0),
            },
        }

    # ------------------------------------------------------------------
    # Engines
    # ------------------------------------------------------------------

    def _compute_risk_state(
        self,
        concept_sectors: Optional[List[Dict[str, Any]]] = None,
        industry_sectors: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        records = self.db.list_trend_trade_records(limit=200)
        closed = [record for record in records if record.get("close_date")]
        consecutive_stop_losses = self._count_recent_streak(closed, lambda item: bool(item.get("is_stop_loss")))
        consecutive_non_stop = self._count_recent_streak(closed, lambda item: item.get("is_stop_loss") is False)
        breakout_trades = [record for record in closed if record.get("open_type") in {"breakout", "compensation"}][:3]
        breakout_failures = sum(1 for record in breakout_trades if record.get("breakout_failed"))

        flags = {
            "reduced_risk": consecutive_stop_losses >= 2,
            "elite_disabled": consecutive_stop_losses >= 2 and consecutive_non_stop < 3,
            "breakout_paused": breakout_failures >= 2,
            "cooldown": False,
            "degraded_system": self._is_degraded_system(concept_sectors, industry_sectors),
        }

        cooldown_until = None
        if consecutive_stop_losses >= 3 and closed:
            latest_close_date = self._parse_date(closed[0].get("close_date"))
            if latest_close_date:
                cooldown_until = latest_close_date + timedelta(days=7)
                flags["cooldown"] = cooldown_until >= date.today()

        current_mode = "normal"
        reasons: List[str] = []
        for mode, enabled in flags.items():
            if enabled:
                reasons.append(mode)
                if RISK_MODE_PRIORITY[mode] > RISK_MODE_PRIORITY[current_mode]:
                    current_mode = mode

        return {
            "current_mode": current_mode,
            "flags": flags,
            "consecutive_stop_losses": consecutive_stop_losses,
            "consecutive_non_stop_losses": consecutive_non_stop,
            "recent_breakout_failures": breakout_failures,
            "cooldown_until": cooldown_until.isoformat() if cooldown_until else None,
            "new_position_limit_pct": 10 if flags["reduced_risk"] else None,
            "reasons": reasons or ["normal"],
        }

    def _rank_sector_members(self, members: List[StockUniverseItem]) -> List[StockUniverseItem]:
        return sorted(
            members,
            key=lambda member: (
                self._recent_change(member.history, 20),
                self._recent_change(member.history, 3),
                float(member.latest.get("amount") or 0.0),
                1 if self._has_limit_up(member.history, 3) else 0,
                1 if self._is_near_new_high(member.history, 20) else 0,
            ),
            reverse=True,
        )

    def _infer_market_stage(
        self,
        leader: Optional[StockUniverseItem],
        members: List[StockUniverseItem],
        top_sector_names: Iterable[str],
    ) -> Tuple[str, Dict[str, Any]]:
        if leader is None or leader.history.empty:
            return "choppy", {"scores": {}, "reasons": []}

        history = leader.history
        latest = history.iloc[-1]
        prev = history.iloc[-2] if len(history) >= 2 else latest
        recent3 = history.tail(3)
        limit_up_count = sum(1 for member in members if float(member.latest.get("pct_chg") or 0.0) >= LIMIT_UP_PCT)
        strong_count = sum(1 for member in members if float(member.latest.get("pct_chg") or 0.0) > 5.0)
        top5_avg = (
            sum(sorted([float(member.latest.get("pct_chg") or 0.0) for member in members], reverse=True)[:5])
            / max(min(len(members), 5), 1)
        )

        scores = {
            "initial": sum(
                [
                    self._new_high_hits(history, 3) >= 1,
                    float(leader.latest.get("amount") or 0.0) > 0,
                    limit_up_count <= 3,
                    self._intraday_pullback(latest) >= 2.0,
                ]
            ),
            "middle": sum(
                [
                    self._new_high_streak(history, 3),
                    float(latest.get("close") or 0.0) >= min(
                        float(latest.get("ma10") or 0.0),
                        float(latest.get("ma20") or 0.0),
                    ),
                    float(latest.get("amount") or 0.0) <= float(latest.get("avg_amount_5") or 0.0),
                    self._sector_up_streak(members, 3),
                ]
            ),
            "late": sum(
                [
                    (sum(1 for value in recent3["pct_chg"].fillna(0) if value >= 7.0) >= 3) or limit_up_count >= 3,
                    limit_up_count >= 5,
                    top5_avg >= 3 and not any(float(member.latest.get("pct_chg") or 0.0) <= -9 for member in members),
                    float(latest.get("close") or 0.0) >= float(latest.get("ma5") or 0.0) * 1.03,
                ]
            ),
        }
        candidates = [stage for stage, score in scores.items() if score >= 2]
        stage = max(candidates, key=lambda item: STAGE_ORDER[item]) if candidates else "choppy"
        reasons = [
            f"leader_top_sector={leader.industry in top_sector_names or leader.name in top_sector_names}",
            f"limit_up_count={limit_up_count}",
            f"strong_member_count={strong_count}",
            f"leader_change={self._safe_round(latest.get('pct_chg'))}",
            f"prev_change={self._safe_round(prev.get('pct_chg'))}",
        ]
        return stage, {"scores": scores, "reasons": reasons}

    def _apply_stage_override(
        self,
        sector_view: str,
        sector_key: str,
        quant_stage: str,
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        overrides = self.db.list_trend_stage_overrides(override_date=date.today(), sector_view=sector_view)
        override = next((item for item in overrides if item["sector_key"] == sector_key), None)
        if override:
            return override["target_stage"], override
        return quant_stage, None

    def _infer_entry_signal(
        self,
        item: StockUniverseItem,
        sector: Dict[str, Any],
        risk_state: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        final_stage = sector["final_stage"]
        if final_stage == "choppy":
            return None

        history = item.history
        latest = history.iloc[-1]
        prev = history.iloc[-2] if len(history) >= 2 else latest
        avg_amount_5 = float(latest.get("avg_amount_5") or 0.0)
        amount = float(latest.get("amount") or 0.0)
        volume_expanded = avg_amount_5 > 0 and amount >= avg_amount_5 * 1.5
        breakout_paused = bool(risk_state["flags"].get("breakout_paused"))
        action_blocked = bool(risk_state["flags"].get("cooldown"))
        linked_up = sector["sector_breadth"]["strong_member_count"] >= 1

        if final_stage in {"initial", "middle"}:
            platform_high = float(history["high"].tail(6).head(5).max()) if len(history) >= 6 else float(prev["high"])
            breakout = float(latest["close"]) >= platform_high
            checks = {
                "platform_days": len(history.tail(6)) >= 6,
                "breakout": breakout,
                "volume_expanded": volume_expanded,
                "upper_shadow_ok": self._upper_shadow_pct(latest) <= 5.0,
                "linked_up": linked_up,
            }
            score = sum(1 for value in checks.values() if value)
            if breakout and score >= 4 and not breakout_paused:
                return {
                    "signal_type": "breakout",
                    "signal_label": "突破交易",
                    "signal_score": score,
                    "actionable": not action_blocked,
                    "suggested_entry": self._safe_round(max(float(latest["close"]), platform_high)),
                    "invalid_if": f"跌破平台或20日线 {self._safe_round(float(latest.get('ma20') or prev['low']))}",
                    "stop_loss": self._safe_round(min(float(prev["low"]), float(latest.get("ma20") or prev["low"]))),
                    "position_limit_pct": 20,
                    "reason_checks": checks,
                }
            if sector["sector_breadth"]["limit_up_count"] >= 3:
                return {
                    "signal_type": "compensation",
                    "signal_label": "踏空补偿",
                    "signal_score": 3,
                    "actionable": not action_blocked and not breakout_paused,
                    "suggested_entry": self._safe_round(float(latest["close"])),
                    "invalid_if": f"跌破前一日低点 {self._safe_round(float(prev['low']))}",
                    "stop_loss": self._safe_round(float(prev["low"])),
                    "position_limit_pct": 10,
                    "reason_checks": {"limit_cluster": True, "stage_allows": True},
                }

        if final_stage == "middle":
            checks = {
                "above_support": float(latest["close"]) >= min(
                    float(latest.get("ma20") or 0.0),
                    float(latest.get("ma60") or 0.0),
                ),
                "volume_contract": avg_amount_5 > 0 and amount <= avg_amount_5,
                "stop_signal": self._has_stop_signal(latest, prev),
            }
            score = sum(1 for value in checks.values() if value)
            if score >= 3:
                support_line = min(
                    float(latest.get("ma20") or latest["low"]),
                    float(latest.get("ma60") or latest["low"]),
                )
                return {
                    "signal_type": "pullback",
                    "signal_label": "回调交易",
                    "signal_score": score,
                    "actionable": not action_blocked,
                    "suggested_entry": self._safe_round(float(latest["close"])),
                    "invalid_if": f"跌破20/60日线支撑 {self._safe_round(support_line)}",
                    "stop_loss": self._safe_round(support_line),
                    "position_limit_pct": 15,
                    "reason_checks": checks,
                }

        if final_stage == "late":
            checks = {
                "pullback_ma": float(latest["close"]) >= float(latest.get("ma5") or 0.0),
                "not_break_ma5": float(latest["low"]) >= float(latest.get("ma5") or 0.0) * 0.98,
                "front_weak_back_strong": (
                    float(prev["close"]) < float(prev.get("ma5") or prev["close"])
                    and float(latest["close"]) > float(latest.get("ma5") or latest["close"])
                ),
            }
            score = sum(1 for value in checks.values() if value)
            if score >= 2:
                return {
                    "signal_type": "late_reclaim",
                    "signal_label": "后期转强",
                    "signal_score": score,
                    "actionable": not action_blocked,
                    "suggested_entry": self._safe_round(float(latest["close"])),
                    "invalid_if": f"跌破10日线 {self._safe_round(float(latest.get('ma10') or latest['low']))}",
                    "stop_loss": self._safe_round(float(latest.get("ma10") or latest["low"])),
                    "position_limit_pct": 10,
                    "reason_checks": checks,
                }
        return None

    def _allocate_position_limit(
        self,
        item_code: str,
        sector_key: str,
        signal_type: str,
        is_elite_candidate: bool,
        risk_state: Dict[str, Any],
        total_position_cap_pct: int,
        total_used_pct: float,
        sector_used_pct: float,
        item_used_pct: float,
    ) -> int:
        if risk_state["flags"].get("cooldown"):
            return 0
        if risk_state["flags"].get("breakout_paused") and signal_type in {"breakout", "compensation"}:
            return 0

        if signal_type == "pullback":
            base_limit = 15
            single_stock_cap = 20
        elif signal_type == "late_reclaim":
            base_limit = 10
            single_stock_cap = 20
        elif signal_type == "compensation":
            base_limit = 10
            single_stock_cap = 20
        elif is_elite_candidate and not risk_state["flags"].get("elite_disabled"):
            base_limit = 35
            single_stock_cap = 35
        else:
            base_limit = 20
            single_stock_cap = 20

        if risk_state["flags"].get("reduced_risk"):
            base_limit = min(base_limit, 10)
            single_stock_cap = min(single_stock_cap, 10)

        remaining_total = max(total_position_cap_pct - total_used_pct, 0.0)
        remaining_sector = max(40 - sector_used_pct, 0.0)
        remaining_single = max(single_stock_cap - item_used_pct, 0.0)
        return int(max(min(base_limit, remaining_total, remaining_sector, remaining_single), 0))

    def _evaluate_position_action(
        self,
        item: StockUniverseItem,
        position: Dict[str, Any],
        sector: Optional[Dict[str, Any]],
    ) -> Tuple[str, str, int]:
        latest = item.history.iloc[-1]
        current_close = float(latest.get("close") or 0.0)
        entry_price = float(position.get("entry_price") or 0.0)
        stop_line = float(position.get("current_stop_loss") or position.get("initial_stop_loss") or 0.0)
        profit_pct = (current_close / entry_price - 1) * 100 if entry_price > 0 else 0.0
        prior_low = float(item.history["low"].tail(20).iloc[:-1].min() or 0.0) if len(item.history) >= 2 else 0.0
        strong_stop_line = max(stop_line, float(latest.get("ma20") or 0.0), prior_low)

        if current_close <= strong_stop_line:
            return "exit", "触发强止损或跌破20日线", 100

        weak_stop_line = stop_line or float(latest.get("ma20") or 0.0) or prior_low
        if self._weak_stop_confirmed(item.history, weak_stop_line):
            return "exit", "缩量跌破关键位且两日未收回", 100

        if profit_pct >= 20 and int(position.get("take_profit_stage", 0) or 0) < 2:
            return "reduce", "盈利达到20%，执行第二次分批止盈", 33

        if profit_pct >= 10 and int(position.get("take_profit_stage", 0) or 0) < 1:
            return "reduce", "盈利达到10%，执行第一次分批止盈", 33

        ma20 = float(latest.get("ma20") or 0.0)
        ma10 = float(latest.get("ma10") or 0.0)
        if ma20 > 0 and current_close < ma20:
            return "exit", "跌破20日线，趋势清仓", 100
        if ma10 > 0 and current_close < ma10:
            return "reduce", "跌破10日线，趋势减仓", 50

        if self._emotion_exit_triggered(item, sector):
            return "exit", "情绪止盈触发", 100

        return "hold", "继续持有", 0

    def _position_signals(
        self,
        item: StockUniverseItem,
        position: Dict[str, Any],
        sector: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        latest = item.history.iloc[-1]
        current_close = float(latest.get("close") or 0.0)
        entry_price = float(position.get("entry_price") or 0.0)
        stop_line = float(position.get("current_stop_loss") or position.get("initial_stop_loss") or 0.0)
        return {
            "strong_stop_loss": current_close <= max(stop_line, float(latest.get("ma20") or 0.0)),
            "weak_stop_loss": self._weak_stop_confirmed(item.history, stop_line),
            "take_profit_10": entry_price > 0 and (current_close / entry_price - 1) * 100 >= 10,
            "take_profit_20": entry_price > 0 and (current_close / entry_price - 1) * 100 >= 20,
            "trend_exit_ma10": current_close < float(latest.get("ma10") or 0.0),
            "trend_exit_ma20": current_close < float(latest.get("ma20") or 0.0),
            "emotion_exit": self._emotion_exit_triggered(item, sector),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _safe_market_context(self) -> Dict[str, Any]:
        try:
            return self.market_service.get_market_context(region="cn", use_cache=True)
        except Exception as exc:
            logger.warning("Trend system failed to load market context, using empty fallback: %s", exc)
            return {"top_sectors": [], "bottom_sectors": [], "indices": [], "market_breadth": {}}

    def _enrich_float_caps(self, universe: List[StockUniverseItem]) -> None:
        for item in universe:
            if item.float_market_cap_b is None:
                item.float_market_cap_b = self._resolve_float_market_cap_b(item.code)

    def _build_empty_daily_snapshot(
        self,
        status: str,
        reason: str,
        source: str = "snapshot_only",
    ) -> Dict[str, Any]:
        generated_at = datetime.now().isoformat()
        risk_state = self._compute_risk_state([], [])
        position = {
            "recommended_position_pct": 20,
            "matched_rules": 0,
            "rules": [
                {"key": "annual_trend", "label": "年线多头", "matched": False, "value": None},
                {"key": "ma_alignment", "label": "均线多头", "matched": False, "value": None},
                {"key": "up_days", "label": "上涨天数多", "matched": False, "value": 0},
            ],
            "index_code": "sh000001",
        }
        overview = {
            "as_of": date.today().isoformat(),
            "generated_at": generated_at,
            "position": position,
            "trade_allowed": False,
            "trade_gate": "blocked",
            "primary_stage": "choppy",
            "main_sectors": [],
            "risk_state": risk_state,
            "candidate_count": 0,
            "snapshot_status": status,
            "empty_reason": reason,
        }
        return {
            "as_of": date.today().isoformat(),
            "generated_at": generated_at,
            "source": source,
            "snapshot_type": "daily_close",
            "status": status,
            "position": position,
            "concept_sectors": [],
            "industry_sectors": [],
            "candidates": [],
            "portfolio": {
                "summary": {"open_count": 0, "total_position_pct": 0.0, "reduce_count": 0, "exit_count": 0},
                "items": [],
            },
            "risk_state": risk_state,
            "plan": {
                "generated_at": generated_at,
                "recommended_position_pct": 20,
                "trade_allowed": False,
                "main_sectors": [],
                "primary_stage": "choppy",
                "risk_state": risk_state,
                "candidates": [],
                "portfolio_actions": [],
                "blocked_rules": [],
                "discipline_notes": [
                    "收盘后检查止损与止盈触发。",
                    "盘前只执行计划内买点。",
                    "盘中禁止临时改规则或情绪交易。",
                ],
                "empty_reason": reason,
                "diagnostics_summary": {},
            },
            "diagnostics": {
                "mode": "full_market_snapshot",
                "total_market_symbols": 0,
                "db_backed_symbols": 0,
                "scan_limit": MAX_UNIVERSE_SCAN,
                "scanned_symbols": 0,
                "etf_excluded": 0,
                "index_excluded": 0,
                "missing_history": 0,
                "sector_resolution_failures": 0,
                "sector_resolved": 0,
                "float_cap_resolved": 0,
                "coverage_ratio": 0.0,
                "alert_count": 0,
                "open_position_count": 0,
                "open_position_pct": 0.0,
                "source_notes": [reason],
                "candidate_filters": {},
            },
            "alerts": [],
            "overview": overview,
        }

    def _build_empty_preopen_snapshot(self, daily_snapshot: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "snapshot_type": "preopen",
            "source": "snapshot_only",
            "as_of": date.today().isoformat(),
            "generated_at": datetime.now().isoformat(),
            "recommended_position_pct": daily_snapshot["position"]["recommended_position_pct"],
            "trade_allowed": False,
            "main_sectors": [],
            "primary_stage": "choppy",
            "risk_state": daily_snapshot["risk_state"],
            "allowed_signal_types": [],
            "blocked_signal_types": [],
            "watch_items": [],
            "discipline_checks": ["尚未生成盘前快照，请先手动重算。"],
        }

    def _clone_snapshot(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            key: value.copy() if isinstance(value, dict) else list(value) if isinstance(value, list) else value
            for key, value in payload.items()
        }

    def _patch_snapshot_after_stage_override(self, sector_view: str, sector_key: str, target_stage: str) -> None:
        latest = self._get_latest_compatible_daily_snapshot()
        if latest is None:
            return
        payload = latest.get("payload", {})
        collection_key = "concept_sectors" if sector_view == "concept" else "industry_sectors"
        for sector in payload.get(collection_key, []):
            if sector.get("sector_key") == sector_key:
                sector["final_stage"] = target_stage

        for candidate in payload.get("candidates", []):
            if candidate.get("sector_view") == sector_view and candidate.get("sector_key") == sector_key:
                candidate["final_stage"] = target_stage
                if target_stage == "choppy":
                    candidate["actionable"] = False
                    candidate["action_block_reason"] = "stage_choppy"

        risk_state = self._compute_risk_state(payload.get("concept_sectors", []), payload.get("industry_sectors", []))
        payload["risk_state"] = risk_state
        payload["portfolio"] = self._build_portfolio_from_open_positions(payload.get("concept_sectors", []))
        payload["candidates"] = self._reconcile_candidate_snapshot(
            payload.get("candidates", []),
            risk_state,
            int(payload.get("position", {}).get("recommended_position_pct", 20) or 20),
        )
        payload["overview"] = self._build_overview(
            payload.get(
                "position",
                self._build_empty_daily_snapshot(status="missing", reason="no snapshot")["position"],
            ),
            payload.get("concept_sectors", []),
            risk_state,
            payload.get("candidates", []),
            payload.get("diagnostics", {}),
        )
        payload["plan"] = self._build_plan(
            payload["overview"],
            payload.get("candidates", []),
            risk_state,
            payload.get("portfolio", {}),
            payload.get("diagnostics", {}),
        )
        payload["generated_at"] = datetime.now().isoformat()
        self.db.save_trend_daily_snapshot(date.fromisoformat(payload["as_of"]), payload)
        with self.__class__._snapshot_condition:
            self.__class__._daily_snapshot_cache = self._clone_snapshot(payload)

    def _refresh_latest_snapshot_state(self) -> None:
        latest = self._get_latest_compatible_daily_snapshot()
        if latest is None:
            return
        payload = latest.get("payload", {})
        risk_state = self._compute_risk_state(payload.get("concept_sectors", []), payload.get("industry_sectors", []))
        payload["risk_state"] = risk_state
        payload["portfolio"] = self._build_portfolio_from_open_positions(payload.get("concept_sectors", []))
        fallback_position = {
            "recommended_position_pct": 20,
            "matched_rules": 0,
            "rules": [],
            "index_code": "sh000001",
        }
        payload["candidates"] = self._reconcile_candidate_snapshot(
            payload.get("candidates", []),
            risk_state,
            int(payload.get("position", {}).get("recommended_position_pct", 20) or 20),
        )
        payload["overview"] = self._build_overview(
            payload.get("position", fallback_position),
            payload.get("concept_sectors", []),
            risk_state,
            payload.get("candidates", []),
            payload.get("diagnostics", {}),
        )
        payload["plan"] = self._build_plan(
            payload["overview"],
            payload.get("candidates", []),
            risk_state,
            payload["portfolio"],
            payload.get("diagnostics", {}),
        )
        payload["generated_at"] = datetime.now().isoformat()
        self.db.save_trend_daily_snapshot(date.fromisoformat(payload["as_of"]), payload)
        with self.__class__._snapshot_condition:
            self.__class__._daily_snapshot_cache = self._clone_snapshot(payload)

    def _build_portfolio_from_open_positions(self, concept_sectors: List[Dict[str, Any]]) -> Dict[str, Any]:
        codes = [
            item.get("code")
            for item in self.db.list_trend_positions(status="open", limit=200)
            if item.get("code")
        ]
        universe: List[StockUniverseItem] = []
        for code in codes:
            history = self._get_stock_history(code, days=260)
            if history.empty:
                continue
            latest = history.iloc[-1].to_dict()
            previous = history.iloc[-2].to_dict() if len(history) >= 2 else latest
            universe.append(
                StockUniverseItem(
                    code=code,
                    name=self._get_stock_name(code),
                    latest=latest,
                    previous=previous,
                    history=history,
                    industry=None,
                    concepts=[],
                    avg_amount_b=self._avg_amount_b(history),
                    float_market_cap_b=None,
                )
            )
        return self._build_portfolio(universe, concept_sectors)

    def _reconcile_candidate_snapshot(
        self,
        candidates: List[Dict[str, Any]],
        risk_state: Dict[str, Any],
        total_position_cap_pct: int,
    ) -> List[Dict[str, Any]]:
        open_positions = self.db.list_trend_positions(status="open", limit=200)
        total_used_pct = sum(float(item.get("position_pct") or 0.0) for item in open_positions)
        sector_used_pct: Dict[str, float] = {}
        item_used_pct: Dict[str, float] = {}
        for item in open_positions:
            sector_key = item.get("sector_key") or ""
            code = str(item.get("code") or "")
            used_pct = float(item.get("position_pct") or 0.0)
            sector_used_pct[sector_key] = sector_used_pct.get(sector_key, 0.0) + used_pct
            item_used_pct[code] = item_used_pct.get(code, 0.0) + used_pct

        normalized: List[Dict[str, Any]] = []
        for candidate in candidates:
            cloned = dict(candidate)
            # Re-evaluate risk gating on snapshot refresh; do not persist old actionable=false as a hard block.
            cloned["_signal_actionable"] = cloned.get("final_stage") != "choppy"
            normalized.append(cloned)
        return self._apply_candidate_allocation_constraints(
            normalized,
            risk_state,
            total_position_cap_pct,
            total_used_pct,
            sector_used_pct,
            item_used_pct,
        )

    def _apply_candidate_allocation_constraints(
        self,
        candidates: List[Dict[str, Any]],
        risk_state: Dict[str, Any],
        total_position_cap_pct: int,
        total_used_pct: float,
        sector_used_pct: Dict[str, float],
        item_used_pct: Dict[str, float],
    ) -> List[Dict[str, Any]]:
        signal_priority = {"breakout": 4, "pullback": 3, "compensation": 2, "late_reclaim": 1}
        ranked = sorted(
            [dict(item) for item in candidates],
            key=lambda item: (
                item.get("signal_score", 0),
                signal_priority.get(str(item.get("signal_type", "")), 0),
                1 if item.get("is_elite_candidate") else 0,
                float(item.get("gain_20d") or 0.0),
            ),
            reverse=True,
        )

        running_total_used = float(total_used_pct)
        running_sector_used = {str(key): float(value) for key, value in sector_used_pct.items()}
        running_item_used = {str(key): float(value) for key, value in item_used_pct.items()}
        updated: List[Dict[str, Any]] = []

        for candidate in ranked:
            signal_type = str(candidate.get("signal_type") or "breakout")
            code = str(candidate.get("code") or "")
            sector_key = str(candidate.get("sector_key") or "")
            if candidate.get("final_stage") == "choppy":
                candidate["recommended_position_pct"] = 0
                candidate["actionable"] = False
                candidate["action_block_reason"] = "stage_choppy"
                candidate.pop("_signal_actionable", None)
                updated.append(candidate)
                continue

            recommended_pct = self._allocate_position_limit(
                item_code=code,
                sector_key=sector_key,
                signal_type=signal_type,
                is_elite_candidate=bool(candidate.get("is_elite_candidate")),
                risk_state=risk_state,
                total_position_cap_pct=total_position_cap_pct,
                total_used_pct=running_total_used,
                sector_used_pct=running_sector_used.get(sector_key, 0.0),
                item_used_pct=running_item_used.get(code, 0.0),
            )
            signal_actionable = bool(candidate.get("_signal_actionable", True))
            actionable = signal_actionable and recommended_pct > 0 and not risk_state["flags"].get("cooldown")
            candidate["recommended_position_pct"] = recommended_pct
            candidate["actionable"] = actionable
            candidate["action_block_reason"] = (
                None if actionable else self._candidate_block_reason(risk_state, recommended_pct, signal_type)
            )
            candidate.pop("_signal_actionable", None)
            updated.append(candidate)

            if actionable:
                running_total_used += recommended_pct
                running_sector_used[sector_key] = running_sector_used.get(sector_key, 0.0) + recommended_pct
                running_item_used[code] = running_item_used.get(code, 0.0) + recommended_pct

        updated.sort(
            key=lambda item: (
                bool(item.get("actionable")),
                int(item.get("recommended_position_pct") or 0),
                int(item.get("signal_score") or 0),
                float(item.get("gain_20d") or 0.0),
            ),
            reverse=True,
        )
        return updated

    def _resolve_sector_membership(self, code: str) -> Tuple[Optional[str], List[str]]:
        cached = self.__class__._sector_membership_cache.get(code)
        if cached and cached.get("as_of") == date.today().isoformat():
            return cached.get("industry"), list(cached.get("concepts", []))

        industry: Optional[str] = None
        concepts: List[str] = []
        industry_resolved = False
        industry_attempted = False
        for fetcher in self._iter_preferred_fetchers():
            if not hasattr(fetcher, "get_stock_sectors"):
                continue
            industry_attempted = True
            try:
                industries = fetcher.get_stock_sectors(code)
            except Exception as exc:
                logger.debug(
                    "Resolve industry failed for %s via %s: %s",
                    code,
                    getattr(fetcher, "name", "unknown"),
                    exc,
                )
                continue
            if industries is not None:
                industry_resolved = True
                if industries:
                    industry = industries[0]
                break

        if not industry_resolved and not industry_attempted:
            try:
                industries = self.fetcher_manager.get_stock_sectors(code)
                if industries:
                    industry = industries[0]
            except Exception as exc:
                logger.debug("Resolve industry failed for %s via manager fallback: %s", code, exc)

        for fetcher in self._iter_preferred_fetchers():
            if not hasattr(fetcher, "get_belong_board"):
                continue
            try:
                board_df = fetcher.get_belong_board(code)
            except Exception as exc:
                logger.debug(
                    "Resolve concept boards failed for %s via %s: %s",
                    code,
                    getattr(fetcher, "name", "unknown"),
                    exc,
                )
                continue

            if board_df is None or board_df.empty:
                continue
            type_col = next(
                (
                    col
                    for col in board_df.columns
                    if "板块类型" in str(col) or str(col).lower() in {"type", "board_type"}
                ),
                None,
            )
            name_col = next(
                (
                    col
                    for col in board_df.columns
                    if "板块名称" in str(col) or "板块" == str(col) or "股票名称" == str(col)
                ),
                None,
            )
            for _, row in board_df.iterrows():
                board_name = str(row.get(name_col) or "").strip() if name_col else ""
                board_type = str(row.get(type_col) or "").strip() if type_col else ""
                if not board_name:
                    continue
                if "行业" in board_type and industry is None:
                    industry = board_name
                elif board_name != industry:
                    concepts.append(board_name)
            break

        concepts = list(dict.fromkeys([item for item in concepts if item]))
        self.__class__._sector_membership_cache[code] = {
            "as_of": date.today().isoformat(),
            "industry": industry,
            "concepts": list(concepts),
        }
        return industry, concepts

    def _resolve_float_market_cap_b(self, code: str) -> Optional[float]:
        cached = self.__class__._float_cap_cache.get(code)
        if cached and cached.get("as_of") == date.today().isoformat():
            return cached.get("value")

        info = None
        for fetcher in self._iter_preferred_fetchers():
            if not hasattr(fetcher, "get_base_info"):
                continue
            try:
                info = fetcher.get_base_info(code)
            except Exception as exc:
                logger.debug(
                    "Resolve base info failed for %s via %s: %s",
                    code,
                    getattr(fetcher, "name", "unknown"),
                    exc,
                )
                continue
            if info:
                break

        if not info:
            return None
        for key in ("流通市值", "float_market_cap", "circulating_market_cap", "总市值", "market_cap"):
            normalized = self._parse_cap_value_to_b(info.get(key))
            if normalized is not None:
                self.__class__._float_cap_cache[code] = {"as_of": date.today().isoformat(), "value": normalized}
                return normalized
        self.__class__._float_cap_cache[code] = {"as_of": date.today().isoformat(), "value": None}
        return None

    def _normalize_price_history(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        normalized = df.copy().sort_values("date").reset_index(drop=True)
        if "pct_chg" not in normalized.columns:
            normalized["pct_chg"] = normalized["close"].pct_change() * 100
        normalized["ma5"] = normalized["close"].rolling(5).mean()
        normalized["ma10"] = normalized["close"].rolling(10).mean()
        normalized["ma20"] = normalized["close"].rolling(20).mean()
        normalized["ma60"] = normalized["close"].rolling(60).mean()
        normalized["ma250"] = normalized["close"].rolling(250).mean()
        normalized["avg_amount_5"] = normalized["amount"].rolling(5).mean()
        return normalized

    def _is_degraded_system(
        self,
        concept_sectors: Optional[List[Dict[str, Any]]],
        industry_sectors: Optional[List[Dict[str, Any]]],
    ) -> bool:
        snapshots = self.db.list_trend_daily_snapshots(days=14)
        if snapshots:
            for snapshot in snapshots:
                payload = snapshot.get("payload", {})
                for key in ("concept_sectors", "industry_sectors"):
                    healthy = any(
                        sector.get("trade_allowed") and sector.get("final_stage") in {"initial", "middle"}
                        for sector in payload.get(key, [])
                    )
                    if healthy:
                        return False
            return True

        sectors = concept_sectors if concept_sectors is not None else (industry_sectors or [])
        return not any(sector["trade_allowed"] and sector["final_stage"] in {"initial", "middle"} for sector in sectors)

    def _emotion_exit_triggered(self, item: StockUniverseItem, sector: Optional[Dict[str, Any]]) -> bool:
        history = item.history
        if history.empty:
            return False
        recent3 = history.tail(3)
        latest = recent3.iloc[-1]
        three_limit_up = sum(1 for value in recent3["pct_chg"].fillna(0) if value >= LIMIT_UP_PCT) >= 3
        volume_not_lock = (
            float(latest.get("amount") or 0.0) > float(latest.get("avg_amount_5") or 0.0)
            and float(latest.get("pct_chg") or 0.0) < LIMIT_UP_PCT
        )
        leader_break = False
        leader_limit_down = False
        if sector and sector.get("leader"):
            leader_meta = sector.get("stage_meta", {})
            leader_break = float(leader_meta.get("leader_pullback_pct") or 0.0) >= 3.0
            leader_limit_down = (
                float(leader_meta.get("leader_pct_chg") or sector["leader"].get("pct_chg") or 0.0) <= -9.0
            )
            if sector["leader"].get("code") == item.code:
                leader_break = leader_break or self._intraday_pullback(latest) >= 3.0
                leader_limit_down = leader_limit_down or float(latest.get("pct_chg") or 0.0) <= -9.0
        return (three_limit_up and volume_not_lock) or leader_break or leader_limit_down

    def _weak_stop_confirmed(self, history: pd.DataFrame, stop_line: float) -> bool:
        if stop_line <= 0 or len(history) < 2:
            return False
        recent = history.tail(2)
        below = all(float(row.get("close") or 0.0) < stop_line for _, row in recent.iterrows())
        shrink = all(
            float(row.get("amount") or 0.0) <= float(row.get("avg_amount_5") or 0.0)
            for _, row in recent.iterrows()
        )
        return below and shrink

    def _new_high_hits(self, history: pd.DataFrame, lookback_days: int) -> int:
        if len(history) < 10:
            return 0
        hits = 0
        for idx in range(max(len(history) - lookback_days, 1), len(history)):
            if float(history.iloc[idx]["high"] or 0.0) >= float(history.iloc[:idx]["high"].max() or 0.0):
                hits += 1
        return hits

    def _new_high_streak(self, history: pd.DataFrame, streak_days: int) -> bool:
        if len(history) < 10:
            return False
        recent = history.tail(streak_days)
        for idx, (_, row) in enumerate(recent.iterrows()):
            prefix = history.iloc[: len(history) - streak_days + idx]
            if prefix.empty or float(row.get("high") or 0.0) < float(prefix["high"].max() or 0.0):
                return False
        return True

    def _sector_up_streak(self, members: List[StockUniverseItem], days: int) -> bool:
        return (
            sum(1 for member in members if self._recent_change(member.history, days) > 0)
            >= max(1, len(members) // 2)
        )

    def _is_near_new_high(self, history: pd.DataFrame, lookback: int) -> bool:
        if len(history) < lookback:
            return False
        recent_high = float(history["high"].tail(lookback).max() or 0.0)
        latest_close = float(history.iloc[-1]["close"] or 0.0)
        return recent_high > 0 and latest_close >= recent_high * 0.97

    def _recent_change(self, history: pd.DataFrame, days: int) -> float:
        if history.empty or len(history) < 2:
            return 0.0
        start_index = max(len(history) - days - 1, 0)
        start_close = float(history.iloc[start_index]["close"] or 0.0)
        latest_close = float(history.iloc[-1]["close"] or 0.0)
        if start_close <= 0:
            return 0.0
        return (latest_close / start_close - 1) * 100

    def _has_limit_up(self, history: pd.DataFrame, days: int) -> bool:
        if history.empty:
            return False
        recent = history.tail(days)
        return bool((recent["pct_chg"].fillna(0) >= LIMIT_UP_PCT).any())

    def _intraday_pullback(self, row: pd.Series) -> float:
        high = float(row.get("high") or 0.0)
        close = float(row.get("close") or 0.0)
        if high <= 0:
            return 0.0
        return max((high - close) / high * 100, 0.0)

    def _upper_shadow_pct(self, row: pd.Series) -> float:
        high = float(row.get("high") or 0.0)
        close = float(row.get("close") or 0.0)
        open_price = float(row.get("open") or 0.0)
        body_top = max(close, open_price)
        if high <= 0:
            return 0.0
        return max(high - body_top, 0.0) / high * 100

    def _has_stop_signal(self, latest: pd.Series, prev: pd.Series) -> bool:
        body = abs(float(latest.get("close") or 0.0) - float(latest.get("open") or 0.0))
        lower_shadow = min(float(latest.get("close") or 0.0), float(latest.get("open") or 0.0)) - float(
            latest.get("low") or 0.0
        )
        rebound = (
            float(latest.get("amount") or 0.0) > float(prev.get("amount") or 0.0)
            and float(latest.get("pct_chg") or 0.0) > 0
        )
        if body <= 0:
            return rebound
        return lower_shadow >= body * 0.5 or rebound

    def _count_recent_streak(self, items: List[Dict[str, Any]], predicate: Any) -> int:
        streak = 0
        for item in items:
            if predicate(item):
                streak += 1
            else:
                break
        return streak

    def _avg_amount_b(self, history: pd.DataFrame, window: int = 20) -> float:
        if history.empty or "amount" not in history.columns:
            return 0.0
        avg_amount = float(history["amount"].tail(window).fillna(0).mean() or 0.0)
        return avg_amount / 1e8

    def _parse_cap_value_to_b(self, value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            raw = float(value)
            return raw / 1e8 if raw > 10000 else raw
        text = str(value).strip().replace(",", "")
        try:
            if text.endswith("亿"):
                return float(text[:-1])
            if text.endswith("万亿"):
                return float(text[:-2]) * 10000
            raw = float(text)
        except ValueError:
            return None
        return raw / 1e8 if raw > 10000 else raw

    def _get_stock_name(self, code: str) -> str:
        try:
            return self.fetcher_manager.get_stock_name(code) or code
        except Exception:
            return code

    def _parse_date(self, value: Any) -> Optional[date]:
        if value is None:
            return None
        if isinstance(value, date):
            return value
        try:
            return date.fromisoformat(str(value))
        except ValueError:
            return None

    def _member_brief(self, member: Optional[StockUniverseItem]) -> Optional[Dict[str, Any]]:
        if member is None:
            return None
        return {
            "code": member.code,
            "name": member.name,
            "pct_chg": self._safe_round(member.latest.get("pct_chg")) or 0.0,
            "amount_b": self._safe_round(float(member.latest.get("amount") or 0.0) / 1e8, 2) or 0.0,
            "gain_20d": self._safe_round(self._recent_change(member.history, 20), 2) or 0.0,
        }

    def _candidate_block_reason(self, risk_state: Dict[str, Any], recommended_pct: int, signal_type: str) -> str:
        if risk_state["flags"].get("cooldown"):
            return "cooldown"
        if risk_state["flags"].get("breakout_paused") and signal_type in {"breakout", "compensation"}:
            return "breakout_paused"
        if recommended_pct <= 0:
            return "position_limit_reached"
        return "risk_control"

    def _build_empty_reason(
        self,
        main_sectors: List[Dict[str, Any]],
        diagnostics: Dict[str, Any],
        risk_state: Dict[str, Any],
    ) -> Optional[str]:
        if main_sectors:
            return None
        if diagnostics.get("scanned_symbols", 0) == 0:
            return "当前没有可供扫描的全市场缓存数据。"
        if diagnostics.get("sector_resolution_failures", 0) >= diagnostics.get("scanned_symbols", 0):
            return "板块归属解析失败过多，无法稳定识别主线。"
        if risk_state["flags"].get("degraded_system"):
            return "最近两周无初期/中期主线板块，系统进入失效观察。"
        return "当前没有满足条件的可交易主线板块。"

    def _snapshot_status_payload(self, snapshot_row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not snapshot_row:
            return {"status": "missing", "snapshot_date": None, "generated_at": None}
        payload = snapshot_row.get("payload", {})
        snapshot_type = str(payload.get("snapshot_type") or "")
        if snapshot_type == "preopen":
            compatible = self._is_compatible_preopen_snapshot(payload)
        elif snapshot_type == "daily_close":
            compatible = self._is_compatible_daily_snapshot(payload)
        else:
            compatible = self._is_compatible_daily_snapshot(payload) or self._is_compatible_preopen_snapshot(payload)
        return {
            "status": payload.get("status", "ready") if compatible else "legacy",
            "snapshot_date": snapshot_row.get("snapshot_date"),
            "generated_at": payload.get("generated_at"),
            "source": payload.get("source"),
        }

    def _is_compatible_daily_snapshot(self, payload: Dict[str, Any]) -> bool:
        if not payload:
            return False
        if not DAILY_SNAPSHOT_REQUIRED_KEYS.issubset(set(payload.keys())):
            return False
        overview = payload.get("overview", {})
        if not isinstance(overview, dict):
            return False
        return {"trade_gate", "snapshot_status", "generated_at"}.issubset(set(overview.keys()))

    @staticmethod
    def _is_compatible_preopen_snapshot(payload: Dict[str, Any]) -> bool:
        if not payload:
            return False
        required = {
            "snapshot_type",
            "as_of",
            "generated_at",
            "recommended_position_pct",
            "trade_allowed",
            "main_sectors",
            "primary_stage",
            "risk_state",
        }
        return required.issubset(set(payload.keys()))

    def _iter_preferred_fetchers(self) -> Iterator[Any]:
        fetchers = list(getattr(self.fetcher_manager, "_fetchers", []))
        if not fetchers:
            return iter(())

        def _weight(fetcher: Any) -> Tuple[int, int]:
            name = str(getattr(fetcher, "name", "")).lower()
            # Prefer stable HTTP providers, de-prioritize noisy/rate-limited providers.
            if "efinance" in name:
                return (0, 0)
            if "akshare" in name:
                return (1, 0)
            if "tushare" in name:
                return (2, 1)
            if "baostock" in name:
                return (3, 1)
            return (1, 0)

        sorted_fetchers = sorted(fetchers, key=_weight)
        selected: List[Any] = []
        for fetcher in sorted_fetchers:
            name = str(getattr(fetcher, "name", "")).lower()
            if "baostock" in name:
                continue
            if "tushare" in name:
                continue
            selected.append(fetcher)

        if not selected:
            selected = sorted_fetchers
        return iter(selected)

    @staticmethod
    def _safe_round(value: Any, digits: int = 2) -> Optional[float]:
        if value is None:
            return None
        try:
            return round(float(value), digits)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _is_etf_code(code: str) -> bool:
        normalized = str(code or "").strip().upper()
        return normalized.isdigit() and len(normalized) == 6 and normalized.startswith(
            ("15", "16", "18", "51", "52", "56", "58")
        )

    @staticmethod
    def _is_etf_name(name: str) -> bool:
        return "ETF" in str(name or "").upper()

    @staticmethod
    def _is_index_code(code: str) -> bool:
        normalized = str(code or "").strip().lower()
        return normalized.startswith(("sh", "sz")) or not normalized.isdigit()
