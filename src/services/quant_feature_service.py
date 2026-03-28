# -*- coding: utf-8 -*-
"""Service layer for quant feature queries and candidate generation."""

from __future__ import annotations

import json
import re
from dataclasses import MISSING, asdict, dataclass, fields
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from src.core.quant_features import (
    IndexSnapshot,
    MarketRegimeResult,
    StockSetupSnapshot,
    apply_stage_demotion,
    build_entry_plan,
    choose_entry_module,
    classify_board_stage,
    classify_market_regime,
    compute_fund_flow_score,
    compute_theme_score,
    get_stage_cycle_label,
    is_board_trade_allowed,
)
from src.repositories.quant_feature_repo import QuantFeatureRepository
from src.repositories.stock_fund_flow_repo import StockFundFlowRepository
from src.storage import DatabaseManager, StockDailyFeature


@dataclass(frozen=True)
class QuantCandidate:
    code: str
    board_code: Optional[str]
    board_name: Optional[str]
    stage: str
    entry_module: str
    signal_score: float
    planned_entry_price: Optional[float]
    initial_stop_price: Optional[float]
    reason: Dict[str, Any]


class QuantFeatureService:
    """High-level feature access for quant strategy."""

    INDEX_CODES = ("sh000001", "sz399001", "sz399006")
    EXCLUDED_BOARD_NAMES = {"未归类概念"}
    BOARD_NAME_ALIAS_MAP = {
        "新能源": ("新能源汽车",),
        "新能源车": ("新能源汽车",),
        "电网概念": ("智能电网",),
        "电网": ("智能电网",),
        "锂矿概念": ("盐湖提锂", "锂电池概念"),
        "CPO概念": ("共封装光学(CPO)",),
        "液冷概念": ("液冷服务器",),
        "券商概念": ("参股券商",),
        "并购重组概念": ("股权转让(并购重组)",),
        "猪肉概念": ("猪肉",),
    }
    MODULE_SCORE_WEIGHTS = {
        # Slightly de-prioritize BREAKOUT and favor PULLBACK based on recent run diagnostics.
        "BREAKOUT": 11.0,
        "PULLBACK": 12.0,
        "LATE_WEAK_TO_STRONG": 8.0,
        # Forward-compatible aliases for revised daily strategy semantics.
        "CLIMAX_PULLBACK": 12.0,
        "CLIMAX_WEAK_TO_STRONG": 8.0,
    }
    MODULE_FAMILY_MAP = {
        "CLIMAX_PULLBACK": "PULLBACK",
        "CLIMAX_WEAK_TO_STRONG": "LATE_WEAK_TO_STRONG",
    }

    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        repository: Optional[QuantFeatureRepository] = None,
        fund_flow_repo: Optional[StockFundFlowRepository] = None,
    ):
        self.db = db_manager or DatabaseManager.get_instance()
        self.repo = repository or QuantFeatureRepository(self.db)
        self.fund_flow_repo = fund_flow_repo or StockFundFlowRepository(self.db)

    def get_market_regime(self, trade_date: date) -> MarketRegimeResult:
        """Return market regime from stored index features."""
        rows = self.repo.list_index_features(trade_date=trade_date)
        snapshots = [
            IndexSnapshot(
                index_code=row.index_code,
                close=float(row.close or 0.0),
                ma5=float(row.ma5 or 0.0),
                ma10=float(row.ma10 or 0.0),
                ma20=float(row.ma20 or 0.0),
                ma250=float(row.ma250 or 0.0),
                up_day_count_10=int(row.up_day_count_10 or 0),
            )
            for row in rows
            if row.index_code in self.INDEX_CODES
        ]
        reliable_snapshots = [snapshot for snapshot in snapshots if self._is_reliable_index_snapshot(snapshot)]
        if reliable_snapshots:
            snapshots = reliable_snapshots
        return classify_market_regime(snapshots)

    @staticmethod
    def _is_reliable_index_snapshot(snapshot: IndexSnapshot) -> bool:
        """Ignore obviously incomplete index snapshots that would bias regime scoring."""
        moving_averages = [snapshot.ma5, snapshot.ma10, snapshot.ma20, snapshot.ma250]
        if any(value <= 0 for value in [snapshot.close, *moving_averages]):
            return False
        all_equal = all(abs(snapshot.close - value) < 1e-9 for value in moving_averages)
        if all_equal and snapshot.up_day_count_10 <= 1:
            return False
        return True

    def get_board_stage_map(self, trade_date: date) -> Dict[str, Dict[str, Any]]:
        """Return board scores and stages keyed by board_code."""
        rows = self.repo.list_board_features(trade_date=trade_date)
        result: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            board_name = str(getattr(row, "board_name", "") or "")
            if board_name in self.EXCLUDED_BOARD_NAMES:
                continue
            meta = self._build_board_meta(row)
            result[row.board_code] = meta
        return result

    def build_board_name_lookup(
        self,
        board_meta_map: Dict[str, Dict[str, Any]],
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """Build exact-name and tolerant-alias lookups for board matching."""
        exact_lookup: Dict[str, Dict[str, Any]] = {}
        alias_lookup: Dict[str, Dict[str, Any]] = {}
        for meta in board_meta_map.values():
            board_name = str(meta.get("board_name", "") or "")
            if not board_name:
                continue
            exact_lookup.setdefault(board_name, meta)
            for alias in self._board_name_aliases(board_name):
                alias_lookup.setdefault(alias, meta)
        return exact_lookup, alias_lookup

    def get_recent_board_stage_by_name(
        self,
        trade_date: date,
        *,
        lookback_days: int = 7,
    ) -> Dict[str, Dict[str, Any]]:
        """Return the latest available board metadata by board name within a recent lookback window."""
        rows = self.repo.list_concept_board_features(
            start_date=trade_date - timedelta(days=max(int(lookback_days), 1)),
            end_date=trade_date,
        )
        result: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            board_name = str(getattr(row, "board_name", "") or "")
            if not board_name or board_name in self.EXCLUDED_BOARD_NAMES or board_name in result:
                continue
            result[board_name] = self._build_board_meta(row)
        return result

    def build_recent_board_name_lookup(
        self,
        trade_date: date,
        *,
        lookback_days: int = 7,
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """Build recent board lookup with exact-name and tolerant-alias keys."""
        exact_map = self.get_recent_board_stage_by_name(trade_date, lookback_days=lookback_days)
        exact_lookup: Dict[str, Dict[str, Any]] = {}
        alias_lookup: Dict[str, Dict[str, Any]] = {}
        for board_name, meta in exact_map.items():
            exact_lookup.setdefault(board_name, meta)
            for alias in self._board_name_aliases(board_name):
                alias_lookup.setdefault(alias, meta)
        return exact_lookup, alias_lookup

    def resolve_board_meta(
        self,
        *,
        board_code: Optional[str],
        board_name: Optional[str],
        board_map: Dict[str, Dict[str, Any]],
        board_name_exact_lookup: Dict[str, Dict[str, Any]],
        board_name_alias_lookup: Dict[str, Dict[str, Any]],
        recent_board_name_exact_lookup: Dict[str, Dict[str, Any]],
        recent_board_name_alias_lookup: Dict[str, Dict[str, Any]],
    ) -> Tuple[Dict[str, Any], str]:
        """Resolve board metadata from same-day code/name first, then recent alias fallback."""
        board_code_text = str(board_code or "")
        board_name_text = str(board_name or "")
        same_day_meta = board_map.get(board_code_text or "")
        if same_day_meta:
            return same_day_meta, "same_day"
        if board_name_text:
            meta = board_name_exact_lookup.get(board_name_text)
            if meta:
                return meta, "same_day"
            meta = recent_board_name_exact_lookup.get(board_name_text)
            if meta:
                return meta, "recent_fallback"
        for alias in self._board_name_aliases(board_name_text):
            meta = board_name_alias_lookup.get(alias)
            if meta:
                return meta, "same_day_alias"
        for alias in self._board_name_aliases(board_name_text):
            meta = recent_board_name_alias_lookup.get(alias)
            if meta:
                return meta, "recent_fallback"
        return {}, "missing"

    def get_trade_candidates(self, trade_date: date) -> List[QuantCandidate]:
        """Return candidate signals for a given trade date."""
        board_map = self.get_board_stage_map(trade_date)
        board_name_exact_map, board_name_alias_map = self.build_board_name_lookup(board_map)
        fallback_board_name_exact_map, fallback_board_name_alias_map = self.build_recent_board_name_lookup(trade_date)
        rows = self.repo.list_stock_features(trade_date=trade_date, eligible_only=True)
        
        fund_flow_cache: Dict[str, Dict[str, float]] = {}
        
        candidates: List[QuantCandidate] = []
        for row in rows:
            if (row.board_name or "") in self.EXCLUDED_BOARD_NAMES:
                continue
            board_meta, match_source = self.resolve_board_meta(
                board_code=row.board_code,
                board_name=row.board_name,
                board_map=board_map,
                board_name_exact_lookup=board_name_exact_map,
                board_name_alias_lookup=board_name_alias_map,
                recent_board_name_exact_lookup=fallback_board_name_exact_map,
                recent_board_name_alias_lookup=fallback_board_name_alias_map,
            )
            if not bool(board_meta.get("trade_allowed", False)):
                continue
            stage = str(board_meta.get("stage") or row.stage or "IGNORE")
            raw = self._load_raw_payload(row.raw_payload_json)
            setup = self._build_stock_setup_snapshot(row=row, raw=raw)
            entry_module = choose_entry_module(setup, stage=stage)
            entry_module_source = "core"
            if not entry_module:
                stored_module = self._as_str(getattr(row, "trigger_module", None))
                if stored_module and stage != "IGNORE":
                    entry_module = stored_module
                    entry_module_source = "stored_feature"
            if not entry_module:
                continue
            entry_plan, entry_plan_module = self._build_entry_plan_with_fallback(
                setup=setup,
                stage=stage,
                entry_module=entry_module,
            )
            if entry_plan is None:
                continue
            
            code = str(getattr(row, "code", "") or "")
            fund_flow_stats = None
            if code and hasattr(self, "fund_flow_repo"):
                if code not in fund_flow_cache:
                    try:
                        fund_flow_cache[code] = self.fund_flow_repo.get_rolling_stats(
                            code=code,
                            trade_date=trade_date,
                            window=5,
                        )
                    except Exception:
                        fund_flow_cache[code] = {}
                fund_flow_stats = fund_flow_cache.get(code)
            
            signal_score = self._score_candidate(row, board_meta, entry_module, setup=setup, fund_flow_stats=fund_flow_stats)
            planned_entry_price = self._safe_float(getattr(entry_plan, "planned_entry_price", 0.0), 0.0)
            initial_stop_price = self._safe_float(getattr(entry_plan, "initial_stop_price", 0.0), 0.0)
            stop_buffer_pct = 0.0
            if planned_entry_price > 0:
                stop_buffer_pct = max((planned_entry_price - initial_stop_price) / planned_entry_price * 100.0, 0.0)
            
            fund_flow_info = {}
            if fund_flow_stats:
                fund_flow_info = {
                    "main_net_inflow_5d": fund_flow_stats.get("main_net_inflow_5d", 0.0),
                    "main_net_inflow_pct": fund_flow_stats.get("latest_main_inflow_pct", 0.0),
                }
            
            candidates.append(
                QuantCandidate(
                    code=row.code,
                    board_code=row.board_code,
                    board_name=row.board_name,
                    stage=stage,
                    entry_module=entry_module,
                    signal_score=signal_score,
                    planned_entry_price=planned_entry_price,
                    initial_stop_price=initial_stop_price,
                    reason={
                        "board_theme_score": board_meta.get("theme_score"),
                        "stage": stage,
                        "stage_cycle_label": board_meta.get("stage_cycle_label"),
                        "stage_source": "board_feature" if board_meta.get("stage") else "stock_feature",
                        "trade_allowed": board_meta.get("trade_allowed", False),
                        "board_match_source": match_source,
                        "entry_module": entry_module,
                        "entry_module_family": self._entry_module_family(entry_module),
                        "entry_module_source": entry_module_source,
                        "entry_plan_module": entry_plan_module,
                        "trigger_price": self._safe_float(getattr(entry_plan, "trigger_price", 0.0), 0.0),
                        "stop_reference": str(getattr(entry_plan, "stop_reference", "") or ""),
                        "planned_entry_price": planned_entry_price,
                        "initial_stop_price": initial_stop_price,
                        "stop_buffer_pct": round(stop_buffer_pct, 4),
                        "feature_trigger_module": self._as_str(getattr(row, "trigger_module", None)),
                        "board_feature_trade_date": board_meta.get("feature_trade_date"),
                        **fund_flow_info,
                    },
                )
            )
        return sorted(candidates, key=lambda item: item.signal_score, reverse=True)

    def _build_stock_setup_snapshot(self, *, row: Any, raw: Dict[str, Any]) -> StockSetupSnapshot:
        """Build a StockSetupSnapshot with forward-compatible payload passthrough."""
        close = self._safe_float(getattr(row, "close", 0.0), 0.0)
        ma5 = self._safe_float(getattr(row, "ma5", 0.0), 0.0)
        ma10 = self._safe_float(getattr(row, "ma10", 0.0), 0.0)
        ma20 = self._safe_float(getattr(row, "ma20", 0.0), 0.0)
        ma60 = self._safe_float(getattr(row, "ma60", 0.0), 0.0)
        base_values: Dict[str, Any] = {
            "code": str(getattr(row, "code", "") or ""),
            "board_code": str(getattr(row, "board_code", "") or ""),
            "board_name": str(getattr(row, "board_name", "") or ""),
            "close": close,
            "open": self._safe_float(self._payload_or_attr(raw, row, "open", close), close),
            "high": self._safe_float(self._payload_or_attr(raw, row, "high", close), close),
            "low": self._safe_float(self._payload_or_attr(raw, row, "low", close), close),
            "ma5": ma5,
            "ma10": ma10,
            "ma20": ma20,
            "ma60": ma60,
            "ret20": self._safe_float(getattr(row, "ret20", 0.0), 0.0),
            "ret60": self._safe_float(getattr(row, "ret60", 0.0), 0.0),
            "median_amount_20": self._safe_float(getattr(row, "median_amount_20", 0.0), 0.0),
            "median_turnover_20": self._safe_float(getattr(row, "median_turnover_20", 0.0), 0.0),
            "listed_days": self._safe_int(self._payload_or_attr(raw, row, "listed_days", 9999), 9999),
            "is_main_board": self._safe_bool(self._payload_or_attr(raw, row, "is_main_board", True), True),
            "is_st": self._safe_bool(self._payload_or_attr(raw, row, "is_st", False), False),
            "is_suspended": self._safe_bool(self._payload_or_attr(raw, row, "is_suspended", False), False),
            "close_above_ma20_ratio": self._safe_float(
                self._payload_or_attr(raw, row, "close_above_ma20_ratio", 1.0),
                1.0,
            ),
            "platform_width_pct": self._safe_float(self._payload_or_attr(raw, row, "platform_width_pct", 0.0), 0.0),
            "breakout_pct": self._safe_float(self._payload_or_attr(raw, row, "breakout_pct", 0.0), 0.0),
            "amount_ratio_5": self._safe_float(self._payload_or_attr(raw, row, "amount_ratio_5", 1.0), 1.0),
            "close_position_ratio": self._safe_float(self._payload_or_attr(raw, row, "close_position_ratio", 1.0), 1.0),
            "upper_shadow_pct": self._safe_float(self._payload_or_attr(raw, row, "upper_shadow_pct", 0.0), 0.0),
            "peer_confirm_count": self._safe_int(self._payload_or_attr(raw, row, "peer_confirm_count", 0), 0),
            "pullback_pct_5d": self._safe_float(self._payload_or_attr(raw, row, "pullback_pct_5d", 0.0), 0.0),
            "pullback_amount_ratio": self._safe_float(
                self._payload_or_attr(raw, row, "pullback_amount_ratio", 1.0),
                1.0,
            ),
            "low_vs_ma20_pct": self._safe_float(self._payload_or_attr(raw, row, "low_vs_ma20_pct", 1.0), 1.0),
            "low_vs_ma60_pct": self._safe_float(self._payload_or_attr(raw, row, "low_vs_ma60_pct", 1.0), 1.0),
            "lower_shadow_body_ratio": self._safe_float(
                self._payload_or_attr(raw, row, "lower_shadow_body_ratio", 0.0),
                0.0,
            ),
            "close_ge_open": self._safe_bool(self._payload_or_attr(raw, row, "close_ge_open", False), False),
            "rebound_break_prev_high": self._safe_bool(
                self._payload_or_attr(raw, row, "rebound_break_prev_high", False),
                False,
            ),
            "ret5": self._safe_float(self._payload_or_attr(raw, row, "ret5", 0.0), 0.0),
            "limit_up_count_5d": self._safe_int(self._payload_or_attr(raw, row, "limit_up_count_5d", 0), 0),
            "prev_close_below_ma5": self._safe_bool(
                self._payload_or_attr(raw, row, "prev_close_below_ma5", False),
                False,
            ),
            "close_above_ma5": self._safe_bool(
                self._payload_or_attr(raw, row, "close_above_ma5", close >= ma5),
                close >= ma5,
            ),
            "close_above_prev_high": self._safe_bool(
                self._payload_or_attr(raw, row, "close_above_prev_high", False),
                False,
            ),
            "weak_to_strong_amount_ratio": self._safe_float(
                self._payload_or_attr(raw, row, "weak_to_strong_amount_ratio", 1.0),
                1.0,
            ),
            "close_vs_ma5_pct": self._safe_float(self._payload_or_attr(raw, row, "close_vs_ma5_pct", 0.0), 0.0),
            "platform_high": self._safe_float(
                self._payload_or_attr(raw, row, "platform_high", self._payload_or_attr(raw, row, "breakout_high", 0.0)),
                0.0,
            ),
            "platform_low": self._safe_float(
                self._payload_or_attr(raw, row, "platform_low", self._payload_or_attr(raw, row, "swing_low", 0.0)),
                0.0,
            ),
            "prev_high": self._safe_float(self._payload_or_attr(raw, row, "prev_high", 0.0), 0.0),
            "prev_low": self._safe_float(self._payload_or_attr(raw, row, "prev_low", 0.0), 0.0),
        }

        snapshot_kwargs = dict(base_values)
        for setup_field in fields(StockSetupSnapshot):
            if setup_field.name in snapshot_kwargs:
                continue
            passthrough = self._payload_or_attr(raw, row, setup_field.name, MISSING)
            if passthrough is MISSING:
                if setup_field.default is not MISSING or setup_field.default_factory is not MISSING:
                    continue
                passthrough = self._fallback_by_annotation(setup_field.type)
            snapshot_kwargs[setup_field.name] = self._coerce_by_annotation(
                passthrough,
                setup_field.type,
                self._fallback_by_annotation(setup_field.type),
            )
        return StockSetupSnapshot(**snapshot_kwargs)

    def _build_entry_plan_with_fallback(
        self,
        *,
        setup: StockSetupSnapshot,
        stage: str,
        entry_module: str,
    ) -> Tuple[Optional[Any], str]:
        """Build entry plan with module alias fallback for forward-compatible modules."""
        plan = build_entry_plan(setup, stage=stage, module=entry_module)
        if plan is not None:
            return plan, str(getattr(plan, "module", entry_module) or entry_module)
        module_family = self._entry_module_family(entry_module)
        if module_family != entry_module:
            fallback_plan = build_entry_plan(setup, stage=stage, module=module_family)
            if fallback_plan is not None:
                return fallback_plan, str(getattr(fallback_plan, "module", module_family) or module_family)
        return None, entry_module

    def _build_board_meta(self, row: Any) -> Dict[str, Any]:
        """Normalize persisted board feature rows into runtime metadata."""
        raw = self._load_raw_payload(getattr(row, "raw_payload_json", None))
        leader_raw = raw.get("leader", {})
        from src.core.quant_features import BoardLeaderSnapshot, ConceptBoardSnapshot

        leader = None
        if getattr(row, "leader_stock_code", None):
            leader = BoardLeaderSnapshot(
                stock_code=row.leader_stock_code,
                stock_name=row.leader_stock_name or "",
                ret20=float(leader_raw.get("ret20", 0.0)),
                amount_5d=float(leader_raw.get("amount_5d", 0.0)),
                breakout_count_3d=int(leader_raw.get("breakout_count_3d", 0)),
                return_2d=float(getattr(row, "leader_2d_return", 0.0) or 0.0),
                limit_up_count_3d=int(getattr(row, "leader_limit_up_3d", 0) or 0),
                consecutive_new_high_3d=int(leader_raw.get("consecutive_new_high_3d", 0)),
                close_vs_ma5_pct=float(leader_raw.get("close_vs_ma5_pct", 0.0)),
                close_above_ma10=bool(leader_raw.get("close_above_ma10", False)),
                low_above_ma20=bool(leader_raw.get("low_above_ma20", False)),
                pullback_volume_ratio=float(leader_raw.get("pullback_volume_ratio", 1.0)),
                single_day_drop_pct=float(leader_raw.get("single_day_drop_pct", 0.0)),
                broke_ma10_with_volume=bool(leader_raw.get("broke_ma10_with_volume", False)),
                broke_ma20=bool(leader_raw.get("broke_ma20", False)),
                is_limit_down=bool(leader_raw.get("is_limit_down", False)),
                close_to_5d_high_drawdown_pct=float(leader_raw.get("close_to_5d_high_drawdown_pct", 0.0)),
            )
        member_count = int(raw.get("member_count", 0) or 0)
        strong_count = int(getattr(row, "strong_stock_count", 0) or 0)
        snapshot = ConceptBoardSnapshot(
            board_code=str(getattr(row, "board_code", "") or ""),
            board_name=str(getattr(row, "board_name", "") or ""),
            amount=float(getattr(row, "amount", 0.0) or 0.0),
            turnover_rank_pct=float(getattr(row, "turnover_rank_pct", 1.0) or 1.0),
            limit_up_count=int(getattr(row, "limit_up_count", 0) or 0),
            strong_stock_count=strong_count,
            member_count=member_count,
            strong_stock_ratio=float(getattr(row, "breadth_ratio", 0.0) or 0.0),
            change_3d_pct=float(raw.get("change_3d_pct", 0.0)),
            up_days_3d=int(raw.get("up_days_3d", 0)),
            top5_avg_pct=float(raw.get("top5_avg_pct", 0.0)),
            big_drop_ratio=float(raw.get("big_drop_ratio", 1.0)),
            limit_down_count=int(raw.get("limit_down_count", 0)),
            leader=leader,
            prev_limit_up_count=int(raw.get("prev_limit_up_count", 0)),
            member_fall20_ratio=float(raw.get("member_fall20_ratio", 0.0)),
        )
        score = compute_theme_score(snapshot)
        trade_allowed = is_board_trade_allowed(score)
        stage = apply_stage_demotion(
            classify_board_stage(snapshot, theme_score=score.theme_score),
            snapshot,
            theme_score=score.theme_score,
        )
        if not trade_allowed:
            stage = "IGNORE"
        trade_date = getattr(row, "trade_date", None)
        return {
            "board_name": snapshot.board_name,
            "theme_score": score.theme_score,
            "stage": stage,
            "stage_cycle_label": get_stage_cycle_label(stage),
            "trade_allowed": trade_allowed,
            "components": asdict(score),
            "feature_trade_date": trade_date.isoformat() if hasattr(trade_date, "isoformat") else str(trade_date or ""),
        }

    def _board_name_aliases(self, board_name: str) -> List[str]:
        """Return tolerant alias keys for board-name matching."""
        name = str(board_name or "").strip()
        if not name or name in self.EXCLUDED_BOARD_NAMES:
            return []
        aliases: List[str] = []
        for candidate in [name, *self.BOARD_NAME_ALIAS_MAP.get(name, ())]:
            text = str(candidate or "").strip()
            if not text:
                continue
            aliases.append(text)
            aliases.append(self._normalize_board_name(text))
            if text.endswith("概念"):
                trimmed = text[:-2].strip()
                if trimmed:
                    aliases.append(trimmed)
                    aliases.append(self._normalize_board_name(trimmed))
            else:
                concept_name = f"{text}概念"
                aliases.append(concept_name)
                aliases.append(self._normalize_board_name(concept_name))
        deduped: List[str] = []
        seen = set()
        for alias in aliases:
            if not alias:
                continue
            if alias in seen:
                continue
            seen.add(alias)
            deduped.append(alias)
        return deduped

    @staticmethod
    def _normalize_board_name(board_name: str) -> str:
        """Normalize board names for tolerant alias matching."""
        text = str(board_name or "").strip()
        text = text.replace("（", "(").replace("）", ")")
        text = re.sub(r"[\s\-_·、/&]+", "", text)
        text = re.sub(r"[()]", "", text)
        if text.endswith("概念"):
            text = text[:-2]
        return text

    @staticmethod
    def _score_candidate(
        row: StockDailyFeature,
        board_meta: Dict[str, Any],
        entry_module: str,
        *,
        setup: Optional[StockSetupSnapshot] = None,
        fund_flow_stats: Optional[Dict[str, float]] = None,
    ) -> float:
        board_score = float(board_meta.get("theme_score", 0.0))
        stage_weight = {
            "EMERGING": 12.0,
            "TREND": 15.0,
            "CLIMAX": 10.0,
        }.get(str(board_meta.get("stage", "")), 0.0)
        module_weight = QuantFeatureService.MODULE_SCORE_WEIGHTS.get(entry_module, 0.0)
        base = float(row.signal_score or 0.0)
        total_score = base + board_score * 10.0 + stage_weight + module_weight
        if entry_module == "BREAKOUT":
            metric_source = setup if setup is not None else row
            breakout_pct = QuantFeatureService._safe_float(getattr(metric_source, "breakout_pct", 0.0), 0.0)
            amount_ratio_5 = QuantFeatureService._safe_float(getattr(metric_source, "amount_ratio_5", 1.0), 1.0)
            close_position_ratio = QuantFeatureService._safe_float(
                getattr(metric_source, "close_position_ratio", 1.0),
                1.0,
            )
            platform_width_pct = QuantFeatureService._safe_float(getattr(metric_source, "platform_width_pct", 0.0), 0.0)
            close_vs_ma5_pct = QuantFeatureService._safe_float(getattr(metric_source, "close_vs_ma5_pct", 0.0), 0.0)
            prior_breakout_count_20d = QuantFeatureService._safe_int(
                getattr(metric_source, "prior_breakout_count_20d", 0),
                0,
            )
            breakout_quality_adjustment = 0.0
            breakout_quality_adjustment += min(max(breakout_pct - 1.0, 0.0) * 1.2, 2.5)
            breakout_quality_adjustment += min(max(amount_ratio_5 - 1.5, 0.0) * 3.0, 2.0)
            breakout_quality_adjustment += min(max(close_position_ratio - 0.7, 0.0) * 6.0, 1.5)
            if 0 < platform_width_pct <= 8.0:
                breakout_quality_adjustment += 1.0
            elif platform_width_pct > 10.0:
                breakout_quality_adjustment -= min((platform_width_pct - 10.0) * 0.7, 1.5)
            if close_vs_ma5_pct > 4.0:
                breakout_quality_adjustment -= min((close_vs_ma5_pct - 4.0) * 0.8, 2.0)
            if prior_breakout_count_20d == 1:
                breakout_quality_adjustment -= 1.0
            total_score += breakout_quality_adjustment
        
        if fund_flow_stats:
            main_inflow_5d = fund_flow_stats.get("main_net_inflow_5d", 0.0)
            main_inflow_pct = fund_flow_stats.get("latest_main_inflow_pct", 0.0)
            turnover_rate = fund_flow_stats.get("turnover_rate", 0.0)
            fund_score = compute_fund_flow_score(
                main_net_inflow_5d=main_inflow_5d,
                main_net_inflow_pct=main_inflow_pct,
                turnover_rate=turnover_rate,
                entry_module=entry_module,
            )
            total_score += fund_score
        
        return round(total_score, 2)

    @classmethod
    def _entry_module_family(cls, module_name: Optional[str]) -> str:
        text = str(module_name or "")
        if not text:
            return ""
        return cls.MODULE_FAMILY_MAP.get(text, text)

    @staticmethod
    def _payload_or_attr(raw: Dict[str, Any], row: Any, key: str, default: Any) -> Any:
        if key in raw and raw.get(key) is not None:
            return raw.get(key)
        value = getattr(row, key, MISSING)
        if value is not MISSING and value is not None:
            return value
        return default

    @staticmethod
    def _fallback_by_annotation(annotation: Any) -> Any:
        text = str(annotation)
        if "bool" in text:
            return False
        if "int" in text:
            return 0
        if "float" in text:
            return 0.0
        if "str" in text:
            return ""
        return None

    @classmethod
    def _coerce_by_annotation(cls, value: Any, annotation: Any, default: Any) -> Any:
        text = str(annotation)
        if "bool" in text:
            return cls._safe_bool(value, bool(default))
        if "int" in text and "bool" not in text:
            return cls._safe_int(value, int(default) if default is not None else 0)
        if "float" in text:
            return cls._safe_float(value, float(default) if default is not None else 0.0)
        if "str" in text:
            return str(value or "")
        return value if value is not None else default

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        if value is None:
            return float(default)
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        if value is None:
            return int(default)
        try:
            return int(value)
        except (TypeError, ValueError):
            return int(default)

    @staticmethod
    def _safe_bool(value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return bool(default)
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if text in {"true", "1", "yes", "y", "on"}:
            return True
        if text in {"false", "0", "no", "n", "off", ""}:
            return False
        return bool(default)

    @staticmethod
    def _as_str(value: Any) -> Optional[str]:
        text = str(value).strip() if value is not None else ""
        return text or None

    @staticmethod
    def _load_raw_payload(raw_payload_json: Optional[str]) -> Dict[str, Any]:
        if not raw_payload_json:
            return {}
        try:
            payload = json.loads(raw_payload_json)
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}
