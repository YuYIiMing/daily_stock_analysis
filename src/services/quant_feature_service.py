# -*- coding: utf-8 -*-
"""Service layer for quant feature queries and candidate generation."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
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
    compute_theme_score,
    get_stage_cycle_label,
    is_board_trade_allowed,
)
from src.repositories.quant_feature_repo import QuantFeatureRepository
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

    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        repository: Optional[QuantFeatureRepository] = None,
    ):
        self.db = db_manager or DatabaseManager.get_instance()
        self.repo = repository or QuantFeatureRepository(self.db)

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

    def build_board_name_lookup(self, board_meta_map: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
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
        candidates: List[QuantCandidate] = []
        for row in rows:
            if (row.board_name or "") in self.EXCLUDED_BOARD_NAMES:
                continue
            board_meta, _ = self.resolve_board_meta(
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
            setup = StockSetupSnapshot(
                code=row.code,
                board_code=row.board_code or "",
                board_name=row.board_name or "",
                close=float(row.close or 0.0),
                open=float(raw.get("open", row.close or 0.0)),
                high=float(raw.get("high", row.close or 0.0)),
                low=float(raw.get("low", row.close or 0.0)),
                ma5=float(row.ma5 or 0.0),
                ma10=float(row.ma10 or 0.0),
                ma20=float(row.ma20 or 0.0),
                ma60=float(row.ma60 or 0.0),
                ret20=float(row.ret20 or 0.0),
                ret60=float(row.ret60 or 0.0),
                median_amount_20=float(row.median_amount_20 or 0.0),
                median_turnover_20=float(row.median_turnover_20 or 0.0),
                listed_days=int(raw.get("listed_days", 9999)),
                is_main_board=bool(raw.get("is_main_board", True)),
                is_st=bool(raw.get("is_st", False)),
                is_suspended=bool(raw.get("is_suspended", False)),
                close_above_ma20_ratio=float(raw.get("close_above_ma20_ratio", 1.0)),
                platform_width_pct=float(raw.get("platform_width_pct", 0.0)),
                breakout_pct=float(raw.get("breakout_pct", 0.0)),
                amount_ratio_5=float(raw.get("amount_ratio_5", 1.0)),
                close_position_ratio=float(raw.get("close_position_ratio", 1.0)),
                upper_shadow_pct=float(raw.get("upper_shadow_pct", 0.0)),
                peer_confirm_count=int(raw.get("peer_confirm_count", 0)),
                pullback_pct_5d=float(raw.get("pullback_pct_5d", 0.0)),
                pullback_amount_ratio=float(raw.get("pullback_amount_ratio", 1.0)),
                low_vs_ma20_pct=float(raw.get("low_vs_ma20_pct", 1.0)),
                low_vs_ma60_pct=float(raw.get("low_vs_ma60_pct", 1.0)),
                lower_shadow_body_ratio=float(raw.get("lower_shadow_body_ratio", 0.0)),
                close_ge_open=bool(raw.get("close_ge_open", False)),
                rebound_break_prev_high=bool(raw.get("rebound_break_prev_high", False)),
                ret5=float(raw.get("ret5", 0.0)),
                limit_up_count_5d=int(raw.get("limit_up_count_5d", 0)),
                prev_close_below_ma5=bool(raw.get("prev_close_below_ma5", False)),
                close_above_ma5=bool(raw.get("close_above_ma5", False)),
                close_above_prev_high=bool(raw.get("close_above_prev_high", False)),
                weak_to_strong_amount_ratio=float(raw.get("weak_to_strong_amount_ratio", 1.0)),
                close_vs_ma5_pct=float(raw.get("close_vs_ma5_pct", 0.0)),
                platform_high=float(raw.get("platform_high", raw.get("breakout_high", 0.0))),
                platform_low=float(raw.get("platform_low", raw.get("swing_low", 0.0))),
                prev_high=float(raw.get("prev_high", 0.0)),
                prev_low=float(raw.get("prev_low", 0.0)),
            )
            entry_module = choose_entry_module(setup, stage=stage)
            if not entry_module:
                continue
            entry_plan = build_entry_plan(setup, stage=stage, module=entry_module)
            if entry_plan is None:
                continue
            signal_score = self._score_candidate(row, board_meta, entry_module)
            candidates.append(
                QuantCandidate(
                    code=row.code,
                    board_code=row.board_code,
                    board_name=row.board_name,
                    stage=stage,
                    entry_module=entry_module,
                    signal_score=signal_score,
                    planned_entry_price=entry_plan.planned_entry_price,
                    initial_stop_price=entry_plan.initial_stop_price,
                    reason={
                        "board_theme_score": board_meta.get("theme_score"),
                        "stage": stage,
                        "stage_cycle_label": board_meta.get("stage_cycle_label"),
                        "trade_allowed": board_meta.get("trade_allowed", False),
                        "entry_module": entry_module,
                        "trigger_price": entry_plan.trigger_price,
                        "stop_reference": entry_plan.stop_reference,
                        "board_feature_trade_date": board_meta.get("feature_trade_date"),
                    },
                )
            )
        return sorted(candidates, key=lambda item: item.signal_score, reverse=True)

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
        stage = apply_stage_demotion(classify_board_stage(snapshot, theme_score=score.theme_score), snapshot, theme_score=score.theme_score)
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
    def _score_candidate(row: StockDailyFeature, board_meta: Dict[str, Any], entry_module: str) -> float:
        board_score = float(board_meta.get("theme_score", 0.0))
        stage_weight = {
            "EMERGING": 12.0,
            "TREND": 15.0,
            "CLIMAX": 10.0,
        }.get(str(board_meta.get("stage", "")), 0.0)
        module_weight = {
            "BREAKOUT": 12.0,
            "PULLBACK": 10.0,
            "LATE_WEAK_TO_STRONG": 8.0,
        }.get(entry_module, 0.0)
        base = float(row.signal_score or 0.0)
        return round(base + board_score * 10.0 + stage_weight + module_weight, 2)

    @staticmethod
    def _load_raw_payload(raw_payload_json: Optional[str]) -> Dict[str, Any]:
        if not raw_payload_json:
            return {}
        try:
            payload = json.loads(raw_payload_json)
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}
