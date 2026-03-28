# -*- coding: utf-8 -*-
"""Pure feature logic for the quant concept-trend strategy."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import mean
from typing import Iterable, List, Optional, Sequence


RISK_ON = "RiskOn"
NEUTRAL = "Neutral"
RISK_OFF = "RiskOff"

STAGE_IGNORE = "IGNORE"
STAGE_EMERGING = "EMERGING"
STAGE_TREND = "TREND"
STAGE_CLIMAX = "CLIMAX"

MODULE_BREAKOUT = "BREAKOUT"
MODULE_PULLBACK = "PULLBACK"
MODULE_CLIMAX_PULLBACK = "CLIMAX_PULLBACK"
MODULE_CLIMAX_WEAK_TO_STRONG = "CLIMAX_WEAK_TO_STRONG"

STAGE_CYCLE_LABELS = {
    STAGE_EMERGING: "初期",
    STAGE_TREND: "中期",
    STAGE_CLIMAX: "后期",
    STAGE_IGNORE: "震荡",
}


@dataclass(frozen=True)
class IndexSnapshot:
    index_code: str
    close: float
    ma5: float
    ma10: float
    ma20: float
    ma250: float
    up_day_count_10: int


@dataclass(frozen=True)
class IndexRegimeResult:
    index_code: str
    score: int
    above_ma250: bool
    ma_bull: bool
    short_term_breadth: bool


@dataclass(frozen=True)
class MarketRegimeResult:
    score: float
    regime: str
    max_exposure_pct: float
    index_results: Sequence[IndexRegimeResult]


@dataclass(frozen=True)
class BoardLeaderSnapshot:
    stock_code: str
    stock_name: str = ""
    ret20: float = 0.0
    amount_5d: float = 0.0
    breakout_count_3d: int = 0
    return_2d: float = 0.0
    limit_up_count_3d: int = 0
    consecutive_new_high_3d: int = 0
    close_vs_ma5_pct: float = 0.0
    close_above_ma10: bool = False
    low_above_ma20: bool = False
    pullback_volume_ratio: float = 1.0
    single_day_drop_pct: float = 0.0
    broke_ma10_with_volume: bool = False
    broke_ma20: bool = False
    is_limit_down: bool = False
    close_to_5d_high_drawdown_pct: float = 0.0


@dataclass(frozen=True)
class ConceptBoardSnapshot:
    board_code: str
    board_name: str
    amount: float
    turnover_rank_pct: float
    limit_up_count: int
    strong_stock_count: int
    member_count: int
    strong_stock_ratio: float
    change_3d_pct: float
    up_days_3d: int
    top5_avg_pct: float
    big_drop_ratio: float
    limit_down_count: int = 0
    leader: Optional[BoardLeaderSnapshot] = None
    prev_limit_up_count: int = 0
    member_fall20_ratio: float = 0.0


@dataclass(frozen=True)
class ThemeScoreResult:
    turnover_rank_score: int
    leader_momentum_score: int
    breadth_score: int
    consistency_score: int
    theme_score: int


@dataclass(frozen=True)
class EntryPlanResult:
    module: str
    planned_entry_price: float
    initial_stop_price: float
    trigger_price: float
    stop_reference: str


@dataclass(frozen=True)
class StockSetupSnapshot:
    code: str
    board_code: str
    board_name: str
    close: float
    open: float
    high: float
    low: float
    ma5: float
    ma10: float
    ma20: float
    ma60: float
    ret20: float
    ret60: float
    median_amount_20: float
    median_turnover_20: float
    listed_days: int = 9999
    is_main_board: bool = True
    is_st: bool = False
    is_suspended: bool = False
    close_above_ma20_ratio: float = 1.0
    platform_width_pct: float = 0.0
    platform_days: int = 0
    breakout_pct: float = 0.0
    prior_breakout_count_20d: int = 0
    amount_ratio_5: float = 1.0
    close_position_ratio: float = 1.0
    upper_shadow_pct: float = 0.0
    peer_confirm_count: int = 0
    pullback_pct_5d: float = 0.0
    pullback_amount_ratio: float = 1.0
    low_vs_ma20_pct: float = 1.0
    low_vs_ma60_pct: float = 1.0
    lower_shadow_body_ratio: float = 0.0
    close_ge_open: bool = False
    rebound_break_prev_high: bool = False
    ret5: float = 0.0
    limit_up_count_5d: int = 0
    prev_close_below_ma5: bool = False
    close_above_ma5: bool = False
    close_above_prev_high: bool = False
    weak_to_strong_amount_ratio: float = 1.0
    close_vs_ma5_pct: float = 0.0
    platform_high: float = 0.0
    platform_low: float = 0.0
    prev_high: float = 0.0
    prev_low: float = 0.0
    volatility_20d: float = 0.0
    main_net_inflow_5d: float = 0.0
    main_net_inflow_pct: float = 0.0
    turnover_rate: float = 0.0


def _get_adaptive_stop_cap(
    *,
    stage: str,
    module: str,
    volatility_20d: float = 0.0,
) -> float:
    base_caps = {
        STAGE_EMERGING: 0.10,
        STAGE_TREND: 0.06,
        STAGE_CLIMAX: 0.05,
    }
    module_factors = {
        MODULE_BREAKOUT: 1.0,
        MODULE_PULLBACK: 0.9,
        MODULE_CLIMAX_PULLBACK: 0.8,
        MODULE_CLIMAX_WEAK_TO_STRONG: 0.8,
    }
    base_cap = base_caps.get(stage, 0.08)
    factor = module_factors.get(module, 1.0)
    if volatility_20d > 0.035:
        factor = min(factor * 1.15, 1.3)
    elif volatility_20d > 0.025:
        factor = min(factor * 1.05, 1.2)
    adaptive_cap = base_cap * factor
    return max(0.03, min(0.12, adaptive_cap))


def compute_index_regime(snapshot: IndexSnapshot) -> IndexRegimeResult:
    """Score one index snapshot."""
    above_ma250 = snapshot.close > snapshot.ma250
    ma_bull = snapshot.ma5 > snapshot.ma10 > snapshot.ma20
    short_term_breadth = snapshot.up_day_count_10 >= 6
    score = sum((above_ma250, ma_bull, short_term_breadth))
    return IndexRegimeResult(
        index_code=snapshot.index_code,
        score=int(score),
        above_ma250=above_ma250,
        ma_bull=ma_bull,
        short_term_breadth=short_term_breadth,
    )


def classify_market_regime(snapshots: Sequence[IndexSnapshot]) -> MarketRegimeResult:
    """Classify overall market regime from index snapshots."""
    results = [compute_index_regime(item) for item in snapshots]
    score = mean([result.score for result in results]) if results else 0.0
    if score >= 2.0:
        return MarketRegimeResult(score=score, regime=RISK_ON, max_exposure_pct=70.0, index_results=results)
    if score >= 1.0:
        return MarketRegimeResult(score=score, regime=NEUTRAL, max_exposure_pct=30.0, index_results=results)
    return MarketRegimeResult(score=score, regime=RISK_OFF, max_exposure_pct=20.0, index_results=results)


def select_board_leader(leaders: Sequence[BoardLeaderSnapshot]) -> Optional[BoardLeaderSnapshot]:
    """Pick a deterministic board leader."""
    if not leaders:
        return None
    return max(
        leaders,
        key=lambda item: (
            item.ret20,
            item.amount_5d,
            item.breakout_count_3d,
            item.return_2d,
            item.stock_code,
        ),
    )


def compute_theme_score(snapshot: ConceptBoardSnapshot) -> ThemeScoreResult:
    """Compute concept-board theme score."""
    leader = snapshot.leader
    turnover_rank_score = int(snapshot.turnover_rank_pct <= 0.10)
    leader_momentum_score = int(
        leader is not None and (leader.return_2d >= 7.0 or leader.limit_up_count_3d >= 1)
    )
    breadth_score = int(snapshot.strong_stock_count >= 3 and snapshot.strong_stock_ratio >= 0.05)
    consistency_score = int(snapshot.change_3d_pct > 0 and snapshot.up_days_3d >= 2)
    theme_score = turnover_rank_score + leader_momentum_score + breadth_score + consistency_score
    return ThemeScoreResult(
        turnover_rank_score=turnover_rank_score,
        leader_momentum_score=leader_momentum_score,
        breadth_score=breadth_score,
        consistency_score=consistency_score,
        theme_score=theme_score,
    )


def is_board_trade_allowed(score: ThemeScoreResult) -> bool:
    """A board is tradable when at least two gate conditions are satisfied."""
    return score.theme_score >= 2


def get_stage_cycle_label(stage: str) -> str:
    """Map internal stage enum to cycle label used by the original strategy."""
    return STAGE_CYCLE_LABELS.get(stage, STAGE_CYCLE_LABELS[STAGE_IGNORE])


def classify_board_stage(snapshot: ConceptBoardSnapshot, *, theme_score: Optional[int] = None) -> str:
    """Classify board lifecycle stage deterministically."""
    leader = snapshot.leader
    current_score = compute_theme_score(snapshot).theme_score if theme_score is None else theme_score
    if leader is None or current_score <= 1:
        return STAGE_IGNORE

    min_strong_required = max(3, int(snapshot.member_count * 0.12 + 0.999))
    leader_acceleration_core = (
        leader.return_2d >= 15.0
        and leader.close_vs_ma5_pct >= 8.0
        and (
            leader.limit_up_count_3d >= 2
            or (leader.breakout_count_3d >= 2 and leader.consecutive_new_high_3d >= 2)
        )
    )
    board_consensus_core = (
        snapshot.strong_stock_count >= min_strong_required
        and snapshot.top5_avg_pct >= 7.0
        and snapshot.big_drop_ratio <= 0.10
        and (
            snapshot.limit_up_count >= 3
            or snapshot.turnover_rank_pct <= 0.05
            or snapshot.strong_stock_ratio >= 0.12
        )
    )
    overheat_confirmation = any(
        (
            leader.close_vs_ma5_pct >= 10.0,
            snapshot.limit_up_count >= 5,
            snapshot.top5_avg_pct >= 9.0,
            snapshot.strong_stock_ratio >= 0.18,
        )
    )
    if leader_acceleration_core and board_consensus_core and overheat_confirmation:
        return STAGE_CLIMAX

    trend_evidence = (
        leader.consecutive_new_high_3d >= 3
        or (leader.consecutive_new_high_3d >= 2 and leader.breakout_count_3d >= 2)
    )
    trend_structure_intact = (
        leader.close_above_ma10
        and leader.low_above_ma20
        and not leader.broke_ma20
        and leader.close_to_5d_high_drawdown_pct <= 8.0
        and leader.pullback_volume_ratio <= 1.0
    )
    trend_board_confirmations = sum(
        (
            snapshot.up_days_3d >= 2,
            snapshot.change_3d_pct > 0,
            snapshot.limit_up_count >= 1,
            snapshot.strong_stock_count >= 3,
            snapshot.strong_stock_ratio >= 0.05,
        )
    )
    if (
        current_score >= 2
        and trend_evidence
        and trend_structure_intact
        and trend_board_confirmations >= 2
    ):
        return STAGE_TREND

    front_row_turnover_dominance = (
        snapshot.turnover_rank_pct <= 0.08
        and snapshot.amount > 0
        and snapshot.strong_stock_count >= 2
    )
    front_row_concentration = (
        snapshot.top5_avg_pct >= 3.0
        and snapshot.strong_stock_count >= 2
        and 0.03 <= snapshot.strong_stock_ratio <= 0.12
    )
    emerging_confirmations = sum(
        (
            front_row_turnover_dominance,
            front_row_concentration,
            snapshot.change_3d_pct > 0,
            snapshot.up_days_3d >= 2,
            snapshot.strong_stock_count >= 2 or snapshot.strong_stock_ratio >= 0.03,
            0.0 <= leader.close_to_5d_high_drawdown_pct <= 6.0,
            -2.0 <= leader.close_vs_ma5_pct <= 6.0,
        )
    )
    if (
        current_score >= 2
        and (leader.breakout_count_3d >= 1 or leader.consecutive_new_high_3d >= 1)
        and 1 <= snapshot.limit_up_count <= 3
        and (front_row_turnover_dominance or front_row_concentration)
        and emerging_confirmations >= 2
    ):
        return STAGE_EMERGING

    return STAGE_IGNORE


def apply_stage_demotion(current_stage: str, snapshot: ConceptBoardSnapshot, *, theme_score: Optional[int] = None) -> str:
    """Apply deterministic demotion rules to a classified stage."""
    leader = snapshot.leader
    current_score = compute_theme_score(snapshot).theme_score if theme_score is None else theme_score
    if leader is None:
        return STAGE_IGNORE
    if leader.is_limit_down or leader.broke_ma20 or snapshot.limit_down_count >= 2 or current_score <= 1:
        return STAGE_IGNORE
    if current_stage in (STAGE_TREND, STAGE_CLIMAX):
        should_demote = any(
            (
                leader.single_day_drop_pct <= -7.0,
                leader.broke_ma10_with_volume,
                snapshot.member_fall20_ratio >= 0.30,
                snapshot.prev_limit_up_count > 0 and snapshot.limit_up_count <= snapshot.prev_limit_up_count * 0.5 and leader.limit_up_count_3d == 0,
            )
        )
        if should_demote:
            return STAGE_TREND if current_stage == STAGE_CLIMAX else STAGE_EMERGING
    return current_stage


def passes_universe_filter(snapshot: StockSetupSnapshot) -> bool:
    """Determine if a stock is inside the candidate universe."""
    if not snapshot.is_main_board or snapshot.is_st or snapshot.is_suspended:
        return False
    if snapshot.close <= snapshot.ma60:
        return False
    if snapshot.ret20 < 10.0 and snapshot.ret60 < 10.0:
        return False
    # Keep a warm liquidity floor for practical execution, but avoid over-filtering.
    return snapshot.median_amount_20 >= 1e8 and snapshot.median_turnover_20 >= 0.30


def detect_breakout_module(snapshot: StockSetupSnapshot, *, stage: str) -> bool:
    platform_days = _resolve_platform_days(snapshot)
    breakout_quality_ok = _passes_breakout_quality_gate(snapshot)
    base_conditions = (
        stage in (STAGE_EMERGING, STAGE_TREND)
        and 5 <= platform_days <= 15
        and snapshot.close_above_ma20_ratio >= 0.80
        and snapshot.upper_shadow_pct <= 3.0
        and snapshot.peer_confirm_count >= 1
        and breakout_quality_ok
    )
    if not base_conditions:
        return False
    if stage == STAGE_EMERGING:
        return (
            snapshot.platform_width_pct <= 8.0
            and snapshot.breakout_pct >= 2.0
            and snapshot.amount_ratio_5 >= 2.0
            and snapshot.close_position_ratio >= 0.80
            and snapshot.close_vs_ma5_pct <= 4.0
        )
    if stage == STAGE_TREND:
        return (
            snapshot.platform_width_pct <= 12.0
            and snapshot.breakout_pct >= 1.0
            and snapshot.amount_ratio_5 >= 1.50
            and snapshot.close_position_ratio >= 0.70
        )
    return False


def detect_pullback_module(snapshot: StockSetupSnapshot, *, stage: str) -> bool:
    has_stop_signal = (
        (snapshot.lower_shadow_body_ratio >= 0.50 and snapshot.close_ge_open)
        or (snapshot.rebound_break_prev_high and snapshot.amount_ratio_5 >= 1.20)
    )
    prior_breakout_ready = _has_prior_breakout_evidence(snapshot)
    return (
        stage == STAGE_TREND
        and prior_breakout_ready
        and (snapshot.low_vs_ma20_pct >= 0.99 or snapshot.low_vs_ma60_pct >= 0.99)
        and -8.0 <= snapshot.pullback_pct_5d <= -2.0
        and snapshot.pullback_amount_ratio <= 0.80
        and has_stop_signal
    )


def detect_climax_pullback_module(snapshot: StockSetupSnapshot, *, stage: str) -> bool:
    """Detect strong-trend pullback continuation setup in climax."""
    if stage != STAGE_CLIMAX:
        return False
    return (
        (snapshot.ret5 >= 15.0 or snapshot.limit_up_count_5d >= 2)
        and (snapshot.low <= snapshot.ma5 * 1.01 or snapshot.low <= snapshot.ma10 * 1.01)
        and snapshot.pullback_amount_ratio <= 0.90
        and snapshot.close_above_ma5
        and snapshot.upper_shadow_pct <= 4.0
        and snapshot.amount_ratio_5 <= 1.20
    )


def detect_climax_weak_to_strong_module(snapshot: StockSetupSnapshot, *, stage: str) -> bool:
    """Detect climax re-acceleration after a brief weakness."""
    if stage != STAGE_CLIMAX:
        return False
    return (
        (snapshot.ret5 >= 15.0 or snapshot.limit_up_count_5d >= 2)
        and snapshot.prev_close_below_ma5
        and snapshot.close_above_ma5
        and snapshot.close_above_prev_high
        and snapshot.rebound_break_prev_high
        and snapshot.weak_to_strong_amount_ratio >= 1.20
        and snapshot.amount_ratio_5 >= 1.20
        and snapshot.amount_ratio_5 <= 2.50
        and snapshot.low <= snapshot.ma5 * 1.005
        and snapshot.upper_shadow_pct <= 3.0
        and -0.5 <= snapshot.close_vs_ma5_pct <= 5.0
    )


def choose_entry_module(snapshot: StockSetupSnapshot, *, stage: str) -> Optional[str]:
    """Pick one entry module with deterministic priority."""
    if not passes_universe_filter(snapshot):
        return None
    if stage == STAGE_CLIMAX:
        if detect_climax_weak_to_strong_module(snapshot, stage=stage):
            return MODULE_CLIMAX_WEAK_TO_STRONG
        if detect_climax_pullback_module(snapshot, stage=stage):
            return MODULE_CLIMAX_PULLBACK
        return None
    if detect_breakout_module(snapshot, stage=stage):
        return MODULE_BREAKOUT
    if detect_pullback_module(snapshot, stage=stage):
        return MODULE_PULLBACK
    return None


def build_entry_plan(
    snapshot: StockSetupSnapshot,
    *,
    stage: str,
    module: Optional[str] = None,
) -> Optional[EntryPlanResult]:
    """Build module-specific entry/stop levels from daily-bar features."""
    chosen_module = module or choose_entry_module(snapshot, stage=stage)
    if not chosen_module:
        return None

    if chosen_module == MODULE_BREAKOUT:
        return _build_breakout_entry_plan(snapshot, stage=stage)
    if chosen_module == MODULE_PULLBACK:
        return _build_pullback_entry_plan(snapshot, stage=stage)
    if chosen_module == MODULE_CLIMAX_PULLBACK:
        return _build_climax_pullback_entry_plan(snapshot, stage=stage)
    if chosen_module == MODULE_CLIMAX_WEAK_TO_STRONG:
        return _build_climax_w2s_entry_plan(snapshot, stage=stage)
    return None


def _build_breakout_entry_plan(snapshot: StockSetupSnapshot, *, stage: str = STAGE_TREND) -> Optional[EntryPlanResult]:
    highs = [value for value in (snapshot.platform_high, snapshot.prev_high, snapshot.high) if value > 0]
    lows = [value for value in (snapshot.platform_low, snapshot.low, snapshot.ma20) if value > 0]
    if not highs or not lows:
        return None
    trigger_price = max(highs)
    entry_price = trigger_price * 1.001
    stop_price = _apply_capped_stop(entry_price=entry_price, structural_stop=min(lows) * 0.995, cap_pct=0.08)
    stop_price = _normalize_stop(entry_price, stop_price)
    return EntryPlanResult(
        module=MODULE_BREAKOUT,
        planned_entry_price=round(entry_price, 4),
        initial_stop_price=round(stop_price, 4),
        trigger_price=round(trigger_price, 4),
        stop_reference="platform_or_low_or_ma20",
    )


def _build_pullback_entry_plan(snapshot: StockSetupSnapshot, *, stage: str = STAGE_TREND) -> Optional[EntryPlanResult]:
    if snapshot.rebound_break_prev_high and snapshot.prev_high > 0:
        trigger_price = max(snapshot.prev_high, snapshot.high, snapshot.close)
    else:
        trigger_price = max(snapshot.close, snapshot.open)

    stop_candidates = [snapshot.low]
    if snapshot.low_vs_ma20_pct >= 0.99 and snapshot.ma20 > 0:
        stop_candidates.append(snapshot.ma20)
    if snapshot.low_vs_ma60_pct >= 0.99 and snapshot.ma60 > 0:
        stop_candidates.append(snapshot.ma60)
    stop_candidates = [value for value in stop_candidates if value > 0]
    if trigger_price <= 0 or not stop_candidates:
        return None

    stop_price = _apply_capped_stop(entry_price=trigger_price, structural_stop=min(stop_candidates) * 0.995, cap_pct=0.06)
    stop_price = _normalize_stop(trigger_price, stop_price)
    return EntryPlanResult(
        module=MODULE_PULLBACK,
        planned_entry_price=round(trigger_price, 4),
        initial_stop_price=round(stop_price, 4),
        trigger_price=round(trigger_price, 4),
        stop_reference="ma20_or_ma60_or_signal_low",
    )


def _build_climax_pullback_entry_plan(snapshot: StockSetupSnapshot, *, stage: str = STAGE_CLIMAX) -> Optional[EntryPlanResult]:
    highs = [value for value in (snapshot.ma5, snapshot.ma10, snapshot.high) if value > 0]
    lows = [value for value in (snapshot.ma5, snapshot.ma10, snapshot.low) if value > 0]
    if not highs or not lows:
        return None
    trigger_price = max(highs)
    stop_price = _apply_capped_stop(entry_price=trigger_price, structural_stop=min(lows) * 0.995, cap_pct=0.05)
    stop_price = _normalize_stop(trigger_price, stop_price)
    return EntryPlanResult(
        module=MODULE_CLIMAX_PULLBACK,
        planned_entry_price=round(trigger_price, 4),
        initial_stop_price=round(stop_price, 4),
        trigger_price=round(trigger_price, 4),
        stop_reference="ma5_or_ma10_or_pullback_low",
    )


def _build_climax_w2s_entry_plan(snapshot: StockSetupSnapshot, *, stage: str = STAGE_CLIMAX) -> Optional[EntryPlanResult]:
    highs = [value for value in (snapshot.prev_high, snapshot.high, snapshot.close) if value > 0]
    lows = [value for value in (snapshot.ma5, snapshot.low) if value > 0]
    if not highs or not lows:
        return None

    trigger_price = max(highs)
    stop_price = _apply_capped_stop(entry_price=trigger_price, structural_stop=min(lows) * 0.995, cap_pct=0.05)
    stop_price = _normalize_stop(trigger_price, stop_price)
    return EntryPlanResult(
        module=MODULE_CLIMAX_WEAK_TO_STRONG,
        planned_entry_price=round(trigger_price, 4),
        initial_stop_price=round(stop_price, 4),
        trigger_price=round(trigger_price, 4),
        stop_reference="ma5_or_signal_low",
    )


def _normalize_stop(entry_price: float, stop_price: float) -> float:
    if entry_price <= 0:
        return 0.0
    if stop_price <= 0:
        return entry_price * 0.97
    if stop_price >= entry_price:
        return entry_price * 0.97
    return stop_price


def _apply_capped_stop(*, entry_price: float, structural_stop: float, cap_pct: float) -> float:
    """Choose the closer stop between structural level and module cap."""
    if entry_price <= 0:
        return max(structural_stop, 0.0)
    cap_stop = entry_price * (1.0 - max(cap_pct, 0.0))
    return max(structural_stop, cap_stop)


def _resolve_platform_days(snapshot: StockSetupSnapshot) -> int:
    """Resolve platform days with backward-compatible proxies when raw field is unavailable."""
    if snapshot.platform_days > 0:
        return int(snapshot.platform_days)
    ratio_days = int(round(snapshot.close_above_ma20_ratio * 10))
    if ratio_days > 0:
        return ratio_days
    if snapshot.platform_width_pct <= 12.0 and snapshot.close_above_ma20_ratio >= 0.80:
        # Backward-compatible proxy for old payloads without explicit platform_days.
        return 5
    return 0


def _has_prior_breakout_evidence(snapshot: StockSetupSnapshot) -> bool:
    """Check prior-breakout evidence with explicit field first, then robust proxies."""
    if snapshot.prior_breakout_count_20d > 0:
        return True
    # Fallback proxies for older feature payloads.
    return (
        snapshot.ret20 >= 12.0
        and snapshot.close_above_ma20_ratio >= 0.85
        and snapshot.prev_high > 0
    )


def _passes_breakout_quality_gate(snapshot: StockSetupSnapshot) -> bool:
    """Filter low-quality breakouts: avoid overextension and stale repeated breakouts."""
    if snapshot.close_vs_ma5_pct > 6.0:
        return False
    if snapshot.prior_breakout_count_20d > 1:
        return False
    return True


def compute_fund_flow_score(
    main_net_inflow_5d: float,
    main_net_inflow_pct: float,
    turnover_rate: float,
    entry_module: str,
) -> float:
    """Compute fund flow factor score (0~3 points).

    Scoring rules:
    - Same-day main net inflow > 0: +1 point
    - 5-day cumulative net inflow > 0: +1 point
    - Net inflow / turnover ratio > 0.5: +1 point (bonus)
    """
    if entry_module not in ("BREAKOUT", "PULLBACK", "CLIMAX_PULLBACK", "CLIMAX_WEAK_TO_STRONG"):
        return 0.0

    score = 0.0

    if main_net_inflow_pct > 0:
        score += 1.0

    if main_net_inflow_5d > 0:
        score += 1.0
        if turnover_rate > 0:
            ratio = abs(main_net_inflow_5d) / turnover_rate
            if ratio > 0.5:
                score += 1.0

    return min(score, 3.0)
