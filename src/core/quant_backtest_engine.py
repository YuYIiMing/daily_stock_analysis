# -*- coding: utf-8 -*-
"""Portfolio backtest engine for the quant strategy."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, Iterable, List, Optional, Sequence

from src.core.quant_strategy_engine import MarketRegimeSnapshot, QuantStrategyEngine, StrategyRiskState, TradePlanCandidate


@dataclass(frozen=True)
class DailyBar:
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    upper_limit_price: Optional[float] = None
    lower_limit_price: Optional[float] = None

    @property
    def is_one_word_limit_up(self) -> bool:
        return (
            self.upper_limit_price is not None
            and self.open >= self.upper_limit_price
            and self.high >= self.upper_limit_price
            and self.low >= self.upper_limit_price
            and self.close >= self.upper_limit_price
        )

    @property
    def is_locked_limit_down(self) -> bool:
        return (
            self.lower_limit_price is not None
            and self.open <= self.lower_limit_price
            and self.high <= self.lower_limit_price
            and self.low <= self.lower_limit_price
            and self.close <= self.lower_limit_price
        )


@dataclass
class OpenPosition:
    code: str
    board_code: Optional[str]
    board_name: Optional[str]
    entry_date: date
    entry_price: float
    shares: int
    initial_shares: int
    initial_stop_price: float
    current_stop_price: float
    entry_module: str
    stage: str
    planned_position_pct: float
    r_value: float
    max_high: float
    tp1_done: bool = False
    tp2_done: bool = False
    max_high_since_entry: float = 0.0
    prev_close: float = 0.0
    down_close_streak: int = 0
    close_history: List[float] = field(default_factory=list)
    weak_stop_armed: bool = False
    weak_stop_remaining_days: int = 0
    weak_stop_reference: float = 0.0


@dataclass(frozen=True)
class ExecutedTrade:
    code: str
    board_code: Optional[str]
    board_name: Optional[str]
    entry_date: date
    exit_date: date
    entry_price: float
    exit_price: float
    shares: int
    entry_module: str
    stage: str
    pnl_pct: float
    pnl_amount: float
    exit_reason: str
    blocked_exit: bool = False


@dataclass(frozen=True)
class EquityPoint:
    trade_date: date
    cash: float
    market_value: float
    equity: float
    drawdown_pct: float
    exposure_pct: float


@dataclass(frozen=True)
class BacktestRunResult:
    summary: Dict[str, float]
    trades: Sequence[ExecutedTrade]
    equity_curve: Sequence[EquityPoint]
    trade_plans: Dict[date, Sequence]


class QuantBacktestEngine:
    """Daily long-only backtest engine."""

    TP1_PCT = 0.10
    TP2_PCT = 0.20
    BREAKOUT_MIN_FOLLOW_THROUGH = 0.04
    PULLBACK_MIN_FOLLOW_THROUGH = 0.03
    CLIMAX_MIN_FOLLOW_THROUGH = 0.02

    def __init__(
        self,
        *,
        commission_rate: float = 0.0003,
        tax_rate: float = 0.001,
        slippage_rate: float = 0.0005,
    ):
        self.commission_rate = commission_rate
        self.tax_rate = tax_rate
        self.slippage_rate = slippage_rate
        self.strategy_engine = QuantStrategyEngine()

    def run(
        self,
        *,
        trading_dates: Sequence[date],
        market_regimes: Dict[date, MarketRegimeSnapshot],
        candidates_by_signal_date: Dict[date, Sequence[TradePlanCandidate]],
        bars_by_code: Dict[str, Dict[date, DailyBar]],
        initial_capital: float,
    ) -> BacktestRunResult:
        """Run portfolio simulation from daily signals and bars."""
        sorted_dates = sorted(trading_dates)
        next_date_map = {sorted_dates[i]: sorted_dates[i + 1] for i in range(len(sorted_dates) - 1)}
        pending_orders: Dict[date, List] = {}
        open_positions: Dict[str, OpenPosition] = {}
        trades: List[ExecutedTrade] = []
        equity_curve: List[EquityPoint] = []
        risk_state = StrategyRiskState()
        cash = initial_capital
        peak_equity = initial_capital
        trade_plan_records: Dict[date, Sequence] = {}

        for current_date in sorted_dates:
            risk_state.step_day()
            current_bars = {code: series[current_date] for code, series in bars_by_code.items() if current_date in series}

            cash, closed_trades = self._process_exits(current_date, open_positions, current_bars, cash)
            trades.extend(closed_trades)
            for trade in closed_trades:
                risk_state.register_stop(trade.exit_reason == "hard_stop" and trade.pnl_amount < 0)

            for plan_item in pending_orders.pop(current_date, []):
                bar = current_bars.get(plan_item.code)
                if bar is None or bar.is_one_word_limit_up:
                    continue
                if plan_item.planned_position_pct <= 0:
                    continue
                shares = self._calc_shares(cash, bar.open, plan_item.planned_position_pct)
                if shares <= 0:
                    continue
                entry_price = bar.open * (1.0 + self.slippage_rate)
                gross_cost = entry_price * shares
                fees = gross_cost * self.commission_rate
                total_cost = gross_cost + fees
                if total_cost > cash:
                    continue
                cash -= total_cost
                r_value = max(entry_price - plan_item.initial_stop_price, entry_price * 0.01)
                open_positions[plan_item.code] = OpenPosition(
                    code=plan_item.code,
                    board_code=plan_item.board_code,
                    board_name=plan_item.board_name,
                    entry_date=current_date,
                    entry_price=entry_price,
                    shares=shares,
                    initial_shares=shares,
                    initial_stop_price=plan_item.initial_stop_price,
                    current_stop_price=plan_item.initial_stop_price,
                    entry_module=plan_item.entry_module,
                    stage=plan_item.stage,
                    planned_position_pct=plan_item.planned_position_pct,
                    r_value=r_value,
                    max_high=bar.high,
                    max_high_since_entry=bar.high,
                    prev_close=bar.close,
                    down_close_streak=0,
                    close_history=[bar.close],
                )

            if current_date in candidates_by_signal_date and current_date in next_date_map:
                market = market_regimes.get(current_date, MarketRegimeSnapshot(regime="RiskOff", max_exposure_pct=0.0))
                trade_plan = self.strategy_engine.build_trade_plan(
                    candidates=candidates_by_signal_date[current_date],
                    market_regime=market,
                    risk_state=risk_state,
                    current_total_exposure_pct=self._current_exposure_pct(open_positions),
                    current_board_exposure_pct=self._current_board_exposure_pct(open_positions),
                )
                trade_plan_records[current_date] = trade_plan
                pending_orders.setdefault(next_date_map[current_date], []).extend(trade_plan)

            market_value = sum(
                position.shares * current_bars[position.code].close
                for position in open_positions.values()
                if position.code in current_bars
            )
            equity = cash + market_value
            peak_equity = max(peak_equity, equity)
            drawdown_pct = ((equity - peak_equity) / peak_equity * 100.0) if peak_equity > 0 else 0.0
            exposure_pct = (market_value / equity * 100.0) if equity > 0 else 0.0
            equity_curve.append(
                EquityPoint(
                    trade_date=current_date,
                    cash=round(cash, 2),
                    market_value=round(market_value, 2),
                    equity=round(equity, 2),
                    drawdown_pct=round(drawdown_pct, 2),
                    exposure_pct=round(exposure_pct, 2),
                )
            )

        if sorted_dates:
            last_date = sorted_dates[-1]
            final_bars = {code: series[last_date] for code, series in bars_by_code.items() if last_date in series}
            for position in list(open_positions.values()):
                bar = final_bars.get(position.code)
                if bar is None:
                    continue
                exit_price = bar.close * (1.0 - self.slippage_rate)
                exit_shares = position.shares
                trades.append(self._close_position(position, last_date, exit_price, "window_end"))
                cash += self._calc_exit_cash(exit_price, exit_shares)
                del open_positions[position.code]
            if equity_curve:
                last_equity = cash
                peak_equity = max(point.equity for point in equity_curve + [EquityPoint(last_date, cash, 0.0, cash, 0.0, 0.0)])
                equity_curve[-1] = EquityPoint(
                    trade_date=last_date,
                    cash=round(cash, 2),
                    market_value=0.0,
                    equity=round(last_equity, 2),
                    drawdown_pct=round(((last_equity - peak_equity) / peak_equity * 100.0) if peak_equity > 0 else 0.0, 2),
                    exposure_pct=0.0,
                )

        summary = self._build_summary(initial_capital=initial_capital, trades=trades, equity_curve=equity_curve)
        return BacktestRunResult(summary=summary, trades=trades, equity_curve=equity_curve, trade_plans=trade_plan_records)

    def _process_exits(
        self,
        current_date: date,
        open_positions: Dict[str, OpenPosition],
        current_bars: Dict[str, DailyBar],
        cash: float,
    ) -> tuple[float, List[ExecutedTrade]]:
        trades: List[ExecutedTrade] = []
        for code, position in list(open_positions.items()):
            bar = current_bars.get(code)
            if bar is None:
                continue
            if position.prev_close > 0 and bar.close < position.prev_close:
                position.down_close_streak += 1
            else:
                position.down_close_streak = 0
            position.prev_close = bar.close
            position.close_history.append(bar.close)
            if len(position.close_history) > 40:
                position.close_history = position.close_history[-40:]
            position.max_high_since_entry = max(position.max_high_since_entry, bar.high)
            position.max_high = position.max_high_since_entry
            if position.entry_date == current_date:
                continue

            if not position.tp1_done and bar.high >= position.entry_price * (1.0 + self.TP1_PCT):
                sell_shares = self._calc_partial_shares(position.shares, fraction=1 / 3)
                if sell_shares > 0:
                    exit_price = position.entry_price * (1.0 + self.TP1_PCT)
                    trades.append(self._close_slice(position, current_date, exit_price, sell_shares, "take_profit_10pct"))
                    cash += self._calc_exit_cash(exit_price, sell_shares)
                    position.shares -= sell_shares
                    position.tp1_done = True
                    position.current_stop_price = max(position.current_stop_price, position.entry_price)

            if position.shares <= 0:
                del open_positions[code]
                continue

            if not position.tp2_done and bar.high >= position.entry_price * (1.0 + self.TP2_PCT):
                sell_shares = self._calc_partial_shares(position.shares, fraction=1 / 2)
                if sell_shares > 0:
                    exit_price = position.entry_price * (1.0 + self.TP2_PCT)
                    trades.append(self._close_slice(position, current_date, exit_price, sell_shares, "take_profit_20pct"))
                    cash += self._calc_exit_cash(exit_price, sell_shares)
                    position.shares -= sell_shares
                    position.tp2_done = True
                    position.current_stop_price = max(position.current_stop_price, position.entry_price * 1.02)

            if position.shares <= 0:
                del open_positions[code]
                continue

            self._update_dynamic_stop(position=position)

            holding_days = (current_date - position.entry_date).days

            if position.weak_stop_armed:
                if bar.close >= position.weak_stop_reference:
                    position.weak_stop_armed = False
                    position.weak_stop_remaining_days = 0
                    position.weak_stop_reference = 0.0
                else:
                    strong_failure = bar.close < position.weak_stop_reference * 0.985
                    if strong_failure and not bar.is_locked_limit_down:
                        exit_price = min(position.weak_stop_reference, bar.open) if bar.open > 0 else position.weak_stop_reference
                        exit_shares = position.shares
                        trades.append(self._close_position(position, current_date, exit_price, "hard_stop"))
                        cash += self._calc_exit_cash(exit_price, exit_shares)
                        del open_positions[code]
                        continue
                    position.weak_stop_remaining_days -= 1
                    if position.weak_stop_remaining_days <= 0 and not bar.is_locked_limit_down:
                        exit_price = bar.open * (1.0 - self.slippage_rate)
                        exit_shares = position.shares
                        trades.append(self._close_position(position, current_date, exit_price, "weak_stop_confirmed"))
                        cash += self._calc_exit_cash(exit_price, exit_shares)
                        del open_positions[code]
                        continue
                    continue
                if position.weak_stop_armed:
                    continue

            if bar.low <= position.current_stop_price:
                if bar.is_locked_limit_down:
                    continue
                if self._should_arm_weak_stop(position=position, bar=bar):
                    position.weak_stop_armed = True
                    position.weak_stop_remaining_days = 2
                    position.weak_stop_reference = position.current_stop_price
                    continue
                exit_price = min(position.current_stop_price, bar.open) if bar.open > 0 else position.current_stop_price
                exit_shares = position.shares
                trades.append(self._close_position(position, current_date, exit_price, "hard_stop"))
                cash += self._calc_exit_cash(exit_price, exit_shares)
                del open_positions[code]
                continue

            if (
                self._is_climax_position(position)
                and self._should_emotion_exit(position=position, bar=bar)
                and not bar.is_locked_limit_down
            ):
                exit_price = bar.open * (1.0 - self.slippage_rate)
                exit_shares = position.shares
                trades.append(self._close_position(position, current_date, exit_price, "emotion_exit"))
                cash += self._calc_exit_cash(exit_price, exit_shares)
                del open_positions[code]
                continue

            if self._should_trend_exit(position=position, bar=bar, holding_days=holding_days) and not bar.is_locked_limit_down:
                exit_price = bar.open * (1.0 - self.slippage_rate)
                exit_shares = position.shares
                exit_reason = "trend_exit_climax" if self._is_climax_position(position) else "trend_exit"
                trades.append(self._close_position(position, current_date, exit_price, exit_reason))
                cash += self._calc_exit_cash(exit_price, exit_shares)
                del open_positions[code]
                continue

            need_time_stop = self._should_time_stop(position=position, bar=bar, holding_days=holding_days)
            if need_time_stop and not bar.is_locked_limit_down:
                exit_price = bar.open * (1.0 - self.slippage_rate)
                exit_shares = position.shares
                trades.append(self._close_position(position, current_date, exit_price, "time_stop"))
                cash += self._calc_exit_cash(exit_price, exit_shares)
                del open_positions[code]
        return cash, trades

    def _update_dynamic_stop(self, *, position: OpenPosition) -> None:
        if not position.tp1_done:
            return
        if self._is_climax_position(position):
            base_protect = position.entry_price * 1.01
            drawdown_floor = position.max_high_since_entry * (0.96 if not position.tp2_done else 0.97)
        else:
            base_protect = position.entry_price
            drawdown_floor = position.max_high_since_entry * (0.93 if not position.tp2_done else 0.95)
        position.current_stop_price = max(position.current_stop_price, base_protect, drawdown_floor)

    def _close_position(self, position: OpenPosition, exit_date: date, exit_price: float, exit_reason: str) -> ExecutedTrade:
        trade = self._close_slice(position, exit_date, exit_price, position.shares, exit_reason)
        position.shares = 0
        return trade

    @staticmethod
    def _close_slice(position: OpenPosition, exit_date: date, exit_price: float, shares: int, exit_reason: str) -> ExecutedTrade:
        pnl_amount = (exit_price - position.entry_price) * shares
        pnl_pct = ((exit_price - position.entry_price) / position.entry_price * 100.0) if position.entry_price > 0 else 0.0
        return ExecutedTrade(
            code=position.code,
            board_code=position.board_code,
            board_name=position.board_name,
            entry_date=position.entry_date,
            exit_date=exit_date,
            entry_price=round(position.entry_price, 4),
            exit_price=round(exit_price, 4),
            shares=shares,
            entry_module=position.entry_module,
            stage=position.stage,
            pnl_pct=round(pnl_pct, 2),
            pnl_amount=round(pnl_amount, 2),
            exit_reason=exit_reason,
        )

    def _build_summary(self, *, initial_capital: float, trades: Sequence[ExecutedTrade], equity_curve: Sequence[EquityPoint]) -> Dict[str, float]:
        final_equity = equity_curve[-1].equity if equity_curve else initial_capital
        total_return_pct = ((final_equity - initial_capital) / initial_capital * 100.0) if initial_capital > 0 else 0.0
        max_drawdown_pct = min([point.drawdown_pct for point in equity_curve], default=0.0)
        closed_trades = [trade for trade in trades if trade.exit_reason not in {"window_end"} or trade.pnl_amount != 0]
        win_count = sum(1 for trade in closed_trades if trade.pnl_amount > 0)
        loss_count = sum(1 for trade in closed_trades if trade.pnl_amount < 0)
        win_rate_pct = (win_count / len(closed_trades) * 100.0) if closed_trades else 0.0
        return {
            "total_return_pct": round(total_return_pct, 2),
            "final_equity": round(final_equity, 2),
            "max_drawdown_pct": round(abs(max_drawdown_pct), 2),
            "trade_count": float(len(closed_trades)),
            "win_count": float(win_count),
            "loss_count": float(loss_count),
            "win_rate_pct": round(win_rate_pct, 2),
        }

    def _calc_shares(self, cash: float, entry_price: float, target_pct: float) -> int:
        budget = cash * (target_pct / 100.0)
        if budget <= 0 or entry_price <= 0:
            return 0
        raw_shares = int(budget // entry_price)
        return max((raw_shares // 100) * 100, 0)

    def _calc_exit_cash(self, exit_price: float, shares: int) -> float:
        gross = exit_price * shares
        fees = gross * (self.commission_rate + self.tax_rate)
        return gross - fees

    @staticmethod
    def _should_time_stop(*, position: OpenPosition, bar: DailyBar, holding_days: int) -> bool:
        """Trigger weak stale-trade exits as a secondary rule."""
        if holding_days <= 0:
            return False

        if position.entry_module == "BREAKOUT":
            follow_through_pct = QuantBacktestEngine._follow_through_pct(position)
            close_return_pct = QuantBacktestEngine._close_return_pct(position=position, close=bar.close)
            if holding_days == 2:
                return follow_through_pct < 0.01 and close_return_pct <= -0.022
            if holding_days == 3:
                return follow_through_pct < 0.025 and QuantBacktestEngine._recent_close_weakness(
                    position=position,
                    lookback=3,
                    threshold_pct=0.010,
                    min_weak=2,
                )
            if holding_days == 4:
                return follow_through_pct < 0.022 and close_return_pct <= -0.012
            return (
                holding_days >= 5
                and position.max_high_since_entry < position.entry_price * 1.04
            )
        if position.entry_module == "PULLBACK":
            return (
                holding_days >= 7
                and position.max_high_since_entry < position.entry_price * 1.03
            )
        if position.entry_module in {"CLIMAX_PULLBACK", "CLIMAX_WEAK_TO_STRONG", "LATE_WEAK_TO_STRONG"}:
            return (
                holding_days >= 3
                and position.max_high_since_entry < position.entry_price * 1.02
            )
        return False

    @staticmethod
    def _follow_through_pct(position: OpenPosition) -> float:
        if position.entry_price <= 0:
            return 0.0
        return position.max_high_since_entry / position.entry_price - 1.0

    @staticmethod
    def _close_return_pct(*, position: OpenPosition, close: float) -> float:
        if position.entry_price <= 0:
            return 0.0
        return close / position.entry_price - 1.0

    @staticmethod
    def _recent_close_weakness(*, position: OpenPosition, lookback: int, threshold_pct: float, min_weak: int) -> bool:
        if position.entry_price <= 0:
            return False
        closes = position.close_history[-max(lookback, 1) :]
        weak_count = sum(1 for value in closes if value <= position.entry_price * (1.0 - threshold_pct))
        return weak_count >= max(min_weak, 1)

    @staticmethod
    def _should_arm_weak_stop(*, position: OpenPosition, bar: DailyBar) -> bool:
        """Allow a short recovery window after weak break for TREND/PULLBACK only."""
        if position.entry_module != "PULLBACK":
            return False
        if position.weak_stop_armed or bar.is_locked_limit_down:
            return False
        if bar.close <= 0 or bar.high < bar.low:
            return False
        # Price-only proxy for shrink-volume style weak break:
        # narrow daily range + mild body + close not far below stop.
        range_pct = (bar.high - bar.low) / bar.close
        body_pct = abs(bar.close - bar.open) / bar.close
        close_gap_pct = max(position.current_stop_price - bar.close, 0.0) / max(position.current_stop_price, 1e-9)
        return range_pct <= 0.045 and body_pct <= 0.02 and close_gap_pct <= 0.015

    @staticmethod
    def _proxy_ma(position: OpenPosition, window: int) -> Optional[float]:
        """Compute MA proxy from available close history when full window is unavailable."""
        history = position.close_history
        min_points = max(3, window // 2)
        if len(history) < min_points:
            return None
        size = min(window, len(history))
        segment = history[-size:]
        if not segment:
            return None
        return sum(segment) / len(segment)

    @staticmethod
    def _should_trend_exit(*, position: OpenPosition, bar: DailyBar, holding_days: int) -> bool:
        ma5 = QuantBacktestEngine._proxy_ma(position, 5)
        ma10 = QuantBacktestEngine._proxy_ma(position, 10)
        ma20 = QuantBacktestEngine._proxy_ma(position, 20)
        drawdown_ratio = (
            (position.max_high_since_entry - bar.close) / position.max_high_since_entry
            if position.max_high_since_entry > 0
            else 0.0
        )
        if QuantBacktestEngine._is_climax_position(position):
            if ma5 is not None and bar.close < ma5:
                if ma10 is not None and bar.close < ma10:
                    return True
                if position.down_close_streak >= 2 and bar.close <= position.entry_price * 1.01:
                    return True
            return (
                (holding_days >= 5 and position.down_close_streak >= 2 and bar.close < position.entry_price * 1.01)
                or (drawdown_ratio >= 0.05 and bar.close < position.entry_price * 1.01)
            )
        if not position.tp1_done:
            return False
        if ma20 is not None and bar.close < ma20:
            return True
        if ma10 is not None and bar.close < ma10 and position.down_close_streak >= 2 and holding_days >= 6:
            return True
        return (
            (holding_days >= 10 and position.down_close_streak >= 3 and bar.close <= position.entry_price * 1.01)
            or (position.tp2_done and drawdown_ratio >= 0.08 and (ma10 is None or bar.close < ma10))
        )

    @staticmethod
    def _should_emotion_exit(*, position: OpenPosition, bar: DailyBar) -> bool:
        spread = max(bar.high - bar.low, 0.0)
        if spread <= 0:
            return False
        close_position_ratio = (bar.close - bar.low) / spread
        upper_shadow_ratio = (bar.high - max(bar.open, bar.close)) / bar.high if bar.high > 0 else 0.0
        failed_limit_up = (
            bar.upper_limit_price is not None
            and bar.high >= bar.upper_limit_price
            and bar.close < bar.upper_limit_price * 0.997
        )
        exhaustion = (
            bar.close < bar.open
            and upper_shadow_ratio >= 0.04
            and close_position_ratio <= 0.35
            and bar.high >= position.max_high_since_entry * 0.995
        )
        return failed_limit_up or exhaustion

    @staticmethod
    def _is_climax_position(position: OpenPosition) -> bool:
        return position.stage == "CLIMAX" or position.entry_module in {"CLIMAX_PULLBACK", "CLIMAX_WEAK_TO_STRONG"}

    @staticmethod
    def _calc_partial_shares(total_shares: int, *, fraction: float) -> int:
        shares = int(total_shares * fraction)
        return max((shares // 100) * 100, 0)

    @staticmethod
    def _current_exposure_pct(open_positions: Dict[str, OpenPosition]) -> float:
        return sum(position.planned_position_pct for position in open_positions.values())

    @staticmethod
    def _current_board_exposure_pct(open_positions: Dict[str, OpenPosition]) -> Dict[str, float]:
        board_exposure: Dict[str, float] = {}
        for position in open_positions.values():
            key = position.board_code or "__none__"
            board_exposure[key] = board_exposure.get(key, 0.0) + position.planned_position_pct
        return board_exposure
