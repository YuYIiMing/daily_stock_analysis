# -*- coding: utf-8 -*-
"""Signal ranking and position sizing for the quant strategy."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

@dataclass(frozen=True)
class MarketRegimeSnapshot:
    regime: str
    max_exposure_pct: float
    score: float = 0.0


@dataclass(frozen=True)
class TradePlanCandidate:
    signal_date: object
    code: str
    board_code: Optional[str]
    board_name: Optional[str]
    stage: str
    entry_module: str
    signal_score: float
    planned_entry_price: float
    initial_stop_price: float
    reason: Dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class TradePlanItem:
    signal_date: object
    code: str
    board_code: Optional[str]
    board_name: Optional[str]
    stage: str
    entry_module: str
    signal_score: float
    planned_entry_price: float
    initial_stop_price: float
    planned_position_pct: float
    blocked_reason: Optional[str] = None
    risk_budget_pct: float = 0.0
    reason: Dict[str, object] = field(default_factory=dict)


@dataclass
class StrategyRiskState:
    consecutive_stop_losses: int = 0
    recent_stop_flags: List[bool] = field(default_factory=list)
    cooldown_days_remaining: int = 0

    def register_stop(self, is_stop_loss: bool) -> None:
        self.recent_stop_flags.append(is_stop_loss)
        self.recent_stop_flags = self.recent_stop_flags[-10:]
        if is_stop_loss:
            self.consecutive_stop_losses += 1
            if self.consecutive_stop_losses >= 3:
                self.cooldown_days_remaining = 5
        else:
            self.consecutive_stop_losses = 0

    def step_day(self) -> None:
        if self.cooldown_days_remaining > 0:
            self.cooldown_days_remaining -= 1


class QuantStrategyEngine:
    """Build actionable daily trade plans from ranked candidates."""

    def __init__(
        self,
        *,
        single_stock_cap_pct: float = 20.0,
        board_cap_pct: float = 40.0,
        max_positions: int = 5,
    ):
        self.single_stock_cap_pct = single_stock_cap_pct
        self.board_cap_pct = board_cap_pct
        self.max_positions = max_positions

    def get_risk_budget_pct(self, risk_state: StrategyRiskState) -> float:
        """Return risk budget per new trade in percent of equity."""
        if risk_state.cooldown_days_remaining > 0 or risk_state.consecutive_stop_losses >= 3:
            return 0.0
        if risk_state.recent_stop_flags:
            stop_rate = sum(1 for item in risk_state.recent_stop_flags if item) / len(risk_state.recent_stop_flags)
            if len(risk_state.recent_stop_flags) >= 4 and stop_rate >= 0.5:
                return 0.25
        return 0.5

    def build_trade_plan(
        self,
        *,
        candidates: Sequence[TradePlanCandidate],
        market_regime: MarketRegimeSnapshot,
        risk_state: Optional[StrategyRiskState] = None,
        current_total_exposure_pct: float = 0.0,
        current_board_exposure_pct: Optional[Dict[str, float]] = None,
    ) -> List[TradePlanItem]:
        """Rank candidates and allocate position targets."""
        risk_state = risk_state or StrategyRiskState()
        board_exposure = dict(current_board_exposure_pct or {})
        remaining_total = max(market_regime.max_exposure_pct - current_total_exposure_pct, 0.0)
        risk_budget_pct = self.get_risk_budget_pct(risk_state)

        items: List[TradePlanItem] = []
        for candidate in sorted(candidates, key=lambda item: item.signal_score, reverse=True)[: self.max_positions]:
            if remaining_total <= 0:
                items.append(self._blocked_item(candidate, "capacity_exhausted", risk_budget_pct))
                continue
            if risk_budget_pct <= 0:
                items.append(self._blocked_item(candidate, "cooldown_active", risk_budget_pct))
                continue

            stop_distance_pct = self._calc_stop_distance_pct(candidate.planned_entry_price, candidate.initial_stop_price)
            if stop_distance_pct <= 0:
                items.append(self._blocked_item(candidate, "invalid_stop_distance", risk_budget_pct))
                continue

            risk_based_pct = (risk_budget_pct / stop_distance_pct) * 100.0
            board_code = candidate.board_code or "__none__"
            board_remaining = max(self.board_cap_pct - board_exposure.get(board_code, 0.0), 0.0)
            target_pct = min(self.single_stock_cap_pct, board_remaining, remaining_total, risk_based_pct)
            if target_pct < 1.0:
                items.append(self._blocked_item(candidate, "capacity_exhausted", risk_budget_pct))
                continue

            target_pct = round(target_pct, 2)
            items.append(
                TradePlanItem(
                    signal_date=candidate.signal_date,
                    code=candidate.code,
                    board_code=candidate.board_code,
                    board_name=candidate.board_name,
                    stage=candidate.stage,
                    entry_module=candidate.entry_module,
                    signal_score=candidate.signal_score,
                    planned_entry_price=candidate.planned_entry_price,
                    initial_stop_price=candidate.initial_stop_price,
                    planned_position_pct=target_pct,
                    risk_budget_pct=risk_budget_pct,
                    reason=candidate.reason,
                )
            )
            remaining_total -= target_pct
            board_exposure[board_code] = board_exposure.get(board_code, 0.0) + target_pct
            if remaining_total <= 0:
                break
        return items

    @staticmethod
    def _calc_stop_distance_pct(entry_price: float, stop_price: float) -> float:
        if entry_price <= 0 or stop_price <= 0 or stop_price >= entry_price:
            return 0.0
        return ((entry_price - stop_price) / entry_price) * 100.0

    @staticmethod
    def _blocked_item(candidate: TradePlanCandidate, blocked_reason: str, risk_budget_pct: float) -> TradePlanItem:
        return TradePlanItem(
            signal_date=candidate.signal_date,
            code=candidate.code,
            board_code=candidate.board_code,
            board_name=candidate.board_name,
            stage=candidate.stage,
            entry_module=candidate.entry_module,
            signal_score=candidate.signal_score,
            planned_entry_price=candidate.planned_entry_price,
            initial_stop_price=candidate.initial_stop_price,
            planned_position_pct=0.0,
            blocked_reason=blocked_reason,
            risk_budget_pct=risk_budget_pct,
            reason=candidate.reason,
        )
