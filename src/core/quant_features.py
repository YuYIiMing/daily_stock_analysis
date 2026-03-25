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
MODULE_LATE_WEAK_TO_STRONG = "LATE_WEAK_TO_STRONG"

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
    breakout_pct: float = 0.0
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
    # Risk-off keeps a defensive probe bucket to align with <=20% exposure rule.
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

    climax_hits = sum(
        [
            leader.return_2d >= 20.0 or leader.ret20 >= 20.0,
            leader.limit_up_count_3d >= 2,
            snapshot.limit_up_count >= 5,
            snapshot.top5_avg_pct >= 3.0,
            leader.close_vs_ma5_pct >= 3.0,
            snapshot.big_drop_ratio <= 0.03,
        ]
    )
    if climax_hits >= 4:
        return STAGE_CLIMAX

    if (
        leader.consecutive_new_high_3d >= 2
        and leader.close_above_ma10
        and leader.low_above_ma20
        and leader.pullback_volume_ratio <= 1.0
        and snapshot.up_days_3d >= 2
        and 1 <= snapshot.limit_up_count <= 4
    ):
        return STAGE_TREND

    if (
        current_score >= 2
        and leader.breakout_count_3d >= 1
        and 1 <= snapshot.limit_up_count <= 3
        and snapshot.change_3d_pct > 0
        and 0.0 <= leader.close_to_5d_high_drawdown_pct <= 4.0
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
        degrade = any(
            (
                leader.single_day_drop_pct <= -7.0,
                leader.broke_ma10_with_volume,
                snapshot.member_fall20_ratio >= 0.30,
                snapshot.prev_limit_up_count > 0 and snapshot.limit_up_count <= snapshot.prev_limit_up_count * 0.5 and leader.limit_up_count_3d == 0,
            )
        )
        if degrade:
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
    return (
        stage in (STAGE_EMERGING, STAGE_TREND)
        and snapshot.platform_width_pct <= 20.0
        and snapshot.close_above_ma20_ratio >= 0.60
        and snapshot.breakout_pct >= 0.2
        and snapshot.amount_ratio_5 >= 1.05
        and snapshot.close_position_ratio >= 0.55
        and snapshot.upper_shadow_pct <= 6.0
        and snapshot.peer_confirm_count >= 0
    )


def detect_pullback_module(snapshot: StockSetupSnapshot, *, stage: str) -> bool:
    has_stop_signal = (
        (snapshot.lower_shadow_body_ratio >= 0.30 and snapshot.close_ge_open)
        or (snapshot.rebound_break_prev_high and snapshot.amount_ratio_5 >= 1.05)
    )
    return (
        stage == STAGE_TREND
        and (snapshot.low_vs_ma20_pct >= 0.985 or snapshot.low_vs_ma60_pct >= 0.985)
        and -12.0 <= snapshot.pullback_pct_5d <= -1.0
        and snapshot.pullback_amount_ratio <= 1.1
        and has_stop_signal
    )


def detect_late_weak_to_strong_module(snapshot: StockSetupSnapshot, *, stage: str) -> bool:
    # CLIMAX is handled as "strong trend or no-trade" in this phase.
    # Keep an escape hatch only for very rare extreme re-acceleration setups.
    if stage != STAGE_CLIMAX:
        return False
    return (
        snapshot.ret5 >= 20.0
        and snapshot.limit_up_count_5d >= 2
        and snapshot.prev_close_below_ma5
        and snapshot.close_above_ma5
        and snapshot.close_above_prev_high
        and snapshot.rebound_break_prev_high
        and snapshot.weak_to_strong_amount_ratio >= 1.30
        and snapshot.amount_ratio_5 >= 1.15
        and snapshot.amount_ratio_5 <= 2.20
        and snapshot.low <= snapshot.ma5 * 1.005
        and snapshot.upper_shadow_pct <= 2.5
        and -0.5 <= snapshot.close_vs_ma5_pct <= 3.0
    )


def choose_entry_module(snapshot: StockSetupSnapshot, *, stage: str) -> Optional[str]:
    """Pick one entry module with deterministic priority."""
    if not passes_universe_filter(snapshot):
        return None
    if stage == STAGE_CLIMAX:
        return MODULE_LATE_WEAK_TO_STRONG if detect_late_weak_to_strong_module(snapshot, stage=stage) else None
    if detect_breakout_module(snapshot, stage=stage):
        return MODULE_BREAKOUT
    if detect_pullback_module(snapshot, stage=stage):
        return MODULE_PULLBACK
    if detect_late_weak_to_strong_module(snapshot, stage=stage):
        return MODULE_LATE_WEAK_TO_STRONG
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
        return _build_breakout_entry_plan(snapshot)
    if chosen_module == MODULE_PULLBACK:
        return _build_pullback_entry_plan(snapshot)
    if chosen_module == MODULE_LATE_WEAK_TO_STRONG:
        return _build_late_w2s_entry_plan(snapshot)
    return None


def _build_breakout_entry_plan(snapshot: StockSetupSnapshot) -> Optional[EntryPlanResult]:
    highs = [value for value in (snapshot.platform_high, snapshot.prev_high, snapshot.high, snapshot.close) if value > 0]
    lows = [value for value in (snapshot.platform_low, snapshot.low, snapshot.ma20) if value > 0]
    if not highs or not lows:
        return None
    trigger_price = max(highs)
    entry_price = trigger_price
    stop_price = min(lows) * 0.995
    stop_price = _normalize_stop(entry_price, stop_price)
    return EntryPlanResult(
        module=MODULE_BREAKOUT,
        planned_entry_price=round(entry_price, 4),
        initial_stop_price=round(stop_price, 4),
        trigger_price=round(trigger_price, 4),
        stop_reference="platform_or_low_or_ma20",
    )


def _build_pullback_entry_plan(snapshot: StockSetupSnapshot) -> Optional[EntryPlanResult]:
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

    stop_price = min(stop_candidates) * 0.995
    stop_price = _normalize_stop(trigger_price, stop_price)
    return EntryPlanResult(
        module=MODULE_PULLBACK,
        planned_entry_price=round(trigger_price, 4),
        initial_stop_price=round(stop_price, 4),
        trigger_price=round(trigger_price, 4),
        stop_reference="ma20_or_ma60_or_signal_low",
    )


def _build_late_w2s_entry_plan(snapshot: StockSetupSnapshot) -> Optional[EntryPlanResult]:
    highs = [value for value in (snapshot.close, snapshot.ma5, snapshot.prev_high if snapshot.close_above_prev_high else 0.0) if value > 0]
    lows = [value for value in (snapshot.ma5, snapshot.low) if value > 0]
    if not highs or not lows:
        return None

    trigger_price = max(highs)
    stop_price = min(lows) * 0.995
    stop_price = _normalize_stop(trigger_price, stop_price)
    return EntryPlanResult(
        module=MODULE_LATE_WEAK_TO_STRONG,
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
