# -*- coding: utf-8 -*-
"""Tests for quant backtest engine."""

from __future__ import annotations

import unittest
from datetime import date

from src.core.quant_backtest_engine import DailyBar, QuantBacktestEngine
from src.core.quant_strategy_engine import MarketRegimeSnapshot, TradePlanCandidate


class QuantBacktestEngineTestCase(unittest.TestCase):
    def test_one_word_limit_up_buy_unfilled(self) -> None:
        engine = QuantBacktestEngine()
        trading_dates = [date(2026, 3, 20), date(2026, 3, 21)]
        result = engine.run(
            trading_dates=trading_dates,
            market_regimes={date(2026, 3, 20): MarketRegimeSnapshot("RiskOn", 70.0), date(2026, 3, 21): MarketRegimeSnapshot("RiskOn", 70.0)},
            candidates_by_signal_date={
                date(2026, 3, 20): [
                    TradePlanCandidate(date(2026, 3, 20), "600001", "BK1", "AI", "TREND", "BREAKOUT", 90.0, 10.0, 9.0)
                ]
            },
            bars_by_code={
                "600001": {
                    date(2026, 3, 21): DailyBar(date(2026, 3, 21), 11.0, 11.0, 11.0, 11.0, upper_limit_price=11.0, lower_limit_price=9.0)
                }
            },
            initial_capital=100000.0,
        )
        self.assertEqual(len(result.trades), 0)

    def test_t_plus_one_and_blocked_limit_down_exit(self) -> None:
        engine = QuantBacktestEngine(slippage_rate=0.0)
        trading_dates = [date(2026, 3, 20), date(2026, 3, 21), date(2026, 3, 24), date(2026, 3, 25)]
        result = engine.run(
            trading_dates=trading_dates,
            market_regimes={day: MarketRegimeSnapshot("RiskOn", 70.0) for day in trading_dates},
            candidates_by_signal_date={
                date(2026, 3, 20): [
                    TradePlanCandidate(date(2026, 3, 20), "600001", "BK1", "AI", "TREND", "BREAKOUT", 90.0, 10.0, 9.5)
                ]
            },
            bars_by_code={
                "600001": {
                    date(2026, 3, 21): DailyBar(date(2026, 3, 21), 10.0, 10.2, 9.0, 9.2, upper_limit_price=11.0, lower_limit_price=9.0),
                    date(2026, 3, 24): DailyBar(date(2026, 3, 24), 8.5, 8.5, 8.5, 8.5, upper_limit_price=10.12, lower_limit_price=8.5),
                    date(2026, 3, 25): DailyBar(date(2026, 3, 25), 9.4, 9.6, 9.2, 9.5, upper_limit_price=9.35, lower_limit_price=7.65),
                }
            },
            initial_capital=100000.0,
        )
        self.assertEqual(len(result.trades), 1)
        self.assertEqual(result.trades[0].exit_reason, "hard_stop")
        self.assertEqual(result.trades[0].exit_date, date(2026, 3, 25))

    def test_full_exit_restores_cash_principal(self) -> None:
        engine = QuantBacktestEngine(commission_rate=0.0, tax_rate=0.0, slippage_rate=0.0)
        trading_dates = [date(2026, 3, 20), date(2026, 3, 21), date(2026, 3, 24)]
        result = engine.run(
            trading_dates=trading_dates,
            market_regimes={day: MarketRegimeSnapshot("RiskOn", 70.0) for day in trading_dates},
            candidates_by_signal_date={
                date(2026, 3, 20): [
                    TradePlanCandidate(date(2026, 3, 20), "600001", "BK1", "AI", "TREND", "BREAKOUT", 90.0, 10.0, 9.9)
                ]
            },
            bars_by_code={
                "600001": {
                    date(2026, 3, 21): DailyBar(date(2026, 3, 21), 10.0, 10.1, 10.0, 10.0, upper_limit_price=11.0, lower_limit_price=9.0),
                    date(2026, 3, 24): DailyBar(date(2026, 3, 24), 9.8, 9.9, 9.7, 9.8, upper_limit_price=11.0, lower_limit_price=9.0),
                }
            },
            initial_capital=100000.0,
        )
        self.assertEqual(len(result.trades), 1)
        self.assertEqual(result.trades[0].shares, 2000)
        self.assertEqual(result.trades[0].exit_reason, "hard_stop")
        self.assertAlmostEqual(result.trades[0].pnl_amount, -400.0, places=2)
        self.assertAlmostEqual(result.equity_curve[-1].equity, 99600.0, places=2)

    def test_breakout_not_time_stopped_too_early(self) -> None:
        engine = QuantBacktestEngine(commission_rate=0.0, tax_rate=0.0, slippage_rate=0.0)
        trading_dates = [
            date(2026, 3, 20),
            date(2026, 3, 21),
            date(2026, 3, 22),
            date(2026, 3, 23),
            date(2026, 3, 24),
            date(2026, 3, 25),
            date(2026, 3, 26),
        ]
        result = engine.run(
            trading_dates=trading_dates,
            market_regimes={day: MarketRegimeSnapshot("RiskOn", 70.0) for day in trading_dates},
            candidates_by_signal_date={
                date(2026, 3, 20): [
                    TradePlanCandidate(date(2026, 3, 20), "600001", "BK1", "AI", "TREND", "BREAKOUT", 90.0, 10.0, 9.5)
                ]
            },
            bars_by_code={
                "600001": {
                    date(2026, 3, 21): DailyBar(date(2026, 3, 21), 10.0, 10.2, 9.9, 10.0),
                    date(2026, 3, 22): DailyBar(date(2026, 3, 22), 9.98, 10.52, 9.9, 10.12),
                    date(2026, 3, 23): DailyBar(date(2026, 3, 23), 10.05, 10.3, 9.96, 10.08),
                    date(2026, 3, 24): DailyBar(date(2026, 3, 24), 9.95, 10.12, 9.86, 9.98),
                    date(2026, 3, 25): DailyBar(date(2026, 3, 25), 9.94, 10.1, 9.85, 9.97),
                    date(2026, 3, 26): DailyBar(date(2026, 3, 26), 9.96, 10.13, 9.9, 10.01),
                }
            },
            initial_capital=100000.0,
        )
        self.assertEqual(len(result.trades), 1)
        self.assertEqual(result.trades[0].exit_reason, "window_end")

    def test_stale_breakout_triggers_time_stop_after_weak_confirmation(self) -> None:
        engine = QuantBacktestEngine(commission_rate=0.0, tax_rate=0.0, slippage_rate=0.0)
        trading_dates = [
            date(2026, 3, 20),
            date(2026, 3, 21),
            date(2026, 3, 22),
            date(2026, 3, 23),
            date(2026, 3, 24),
            date(2026, 3, 25),
            date(2026, 3, 26),
            date(2026, 3, 27),
            date(2026, 3, 28),
            date(2026, 3, 29),
            date(2026, 3, 30),
        ]
        result = engine.run(
            trading_dates=trading_dates,
            market_regimes={day: MarketRegimeSnapshot("RiskOn", 70.0) for day in trading_dates},
            candidates_by_signal_date={
                date(2026, 3, 20): [
                    TradePlanCandidate(date(2026, 3, 20), "600001", "BK1", "AI", "TREND", "BREAKOUT", 90.0, 10.0, 8.0)
                ]
            },
            bars_by_code={
                "600001": {
                    date(2026, 3, 21): DailyBar(date(2026, 3, 21), 10.0, 10.1, 9.9, 9.95),
                    date(2026, 3, 22): DailyBar(date(2026, 3, 22), 9.95, 10.08, 9.82, 9.88),
                    date(2026, 3, 23): DailyBar(date(2026, 3, 23), 9.9, 10.05, 9.75, 9.84),
                    date(2026, 3, 24): DailyBar(date(2026, 3, 24), 9.88, 10.02, 9.7, 9.8),
                    date(2026, 3, 25): DailyBar(date(2026, 3, 25), 9.86, 10.0, 9.68, 9.78),
                    date(2026, 3, 26): DailyBar(date(2026, 3, 26), 9.84, 10.02, 9.66, 9.76),
                    date(2026, 3, 27): DailyBar(date(2026, 3, 27), 9.83, 10.03, 9.65, 9.74),
                    date(2026, 3, 28): DailyBar(date(2026, 3, 28), 9.82, 10.04, 9.64, 9.73),
                    date(2026, 3, 29): DailyBar(date(2026, 3, 29), 9.81, 10.06, 9.63, 9.72),
                    date(2026, 3, 30): DailyBar(date(2026, 3, 30), 9.8, 10.06, 9.62, 9.71),
                }
            },
            initial_capital=100000.0,
        )
        self.assertEqual(len(result.trades), 1)
        self.assertEqual(result.trades[0].exit_reason, "time_stop")
        self.assertEqual(result.trades[0].exit_date, date(2026, 3, 24))

    def test_breakout_day3_early_failure_triggers_time_stop(self) -> None:
        engine = QuantBacktestEngine(commission_rate=0.0, tax_rate=0.0, slippage_rate=0.0)
        trading_dates = [
            date(2026, 3, 20),
            date(2026, 3, 21),
            date(2026, 3, 22),
            date(2026, 3, 23),
            date(2026, 3, 24),
            date(2026, 3, 25),
        ]
        result = engine.run(
            trading_dates=trading_dates,
            market_regimes={day: MarketRegimeSnapshot("RiskOn", 70.0) for day in trading_dates},
            candidates_by_signal_date={
                date(2026, 3, 20): [
                    TradePlanCandidate(date(2026, 3, 20), "600011", "BK1", "AI", "TREND", "BREAKOUT", 90.0, 10.0, 8.8)
                ]
            },
            bars_by_code={
                "600011": {
                    date(2026, 3, 21): DailyBar(date(2026, 3, 21), 10.0, 10.1, 9.9, 9.95),
                    date(2026, 3, 22): DailyBar(date(2026, 3, 22), 9.95, 10.12, 9.82, 9.86),
                    date(2026, 3, 23): DailyBar(date(2026, 3, 23), 9.9, 10.13, 9.78, 9.84),
                    date(2026, 3, 24): DailyBar(date(2026, 3, 24), 9.88, 10.14, 9.76, 9.8),
                    date(2026, 3, 25): DailyBar(date(2026, 3, 25), 9.84, 10.2, 9.78, 9.86),
                }
            },
            initial_capital=100000.0,
        )
        self.assertEqual(len(result.trades), 1)
        self.assertEqual(result.trades[0].exit_reason, "time_stop")
        self.assertEqual(result.trades[0].exit_date, date(2026, 3, 24))

    def test_fixed_threshold_partial_take_profit_10_and_20(self) -> None:
        engine = QuantBacktestEngine(commission_rate=0.0, tax_rate=0.0, slippage_rate=0.0)
        trading_dates = [date(2026, 3, 20), date(2026, 3, 21), date(2026, 3, 22), date(2026, 3, 23)]
        result = engine.run(
            trading_dates=trading_dates,
            market_regimes={day: MarketRegimeSnapshot("RiskOn", 70.0) for day in trading_dates},
            candidates_by_signal_date={
                date(2026, 3, 20): [
                    TradePlanCandidate(date(2026, 3, 20), "600001", "BK1", "AI", "TREND", "BREAKOUT", 90.0, 10.0, 9.8)
                ]
            },
            bars_by_code={
                "600001": {
                    date(2026, 3, 21): DailyBar(date(2026, 3, 21), 10.0, 11.1, 9.95, 10.9),
                    date(2026, 3, 22): DailyBar(date(2026, 3, 22), 11.0, 12.2, 11.7, 12.0),
                    date(2026, 3, 23): DailyBar(date(2026, 3, 23), 12.0, 12.1, 11.8, 12.0),
                }
            },
            initial_capital=100000.0,
        )
        reasons = [trade.exit_reason for trade in result.trades]
        self.assertIn("take_profit_10pct", reasons)
        self.assertIn("take_profit_20pct", reasons)
        self.assertTrue(any(trade.exit_reason == "window_end" for trade in result.trades))

    def test_remaining_position_protected_after_first_partial(self) -> None:
        engine = QuantBacktestEngine(commission_rate=0.0, tax_rate=0.0, slippage_rate=0.0)
        trading_dates = [date(2026, 3, 20), date(2026, 3, 21), date(2026, 3, 22), date(2026, 3, 23)]
        result = engine.run(
            trading_dates=trading_dates,
            market_regimes={day: MarketRegimeSnapshot("RiskOn", 70.0) for day in trading_dates},
            candidates_by_signal_date={
                date(2026, 3, 20): [
                    TradePlanCandidate(date(2026, 3, 20), "600001", "BK1", "AI", "TREND", "BREAKOUT", 90.0, 10.0, 9.8)
                ]
            },
            bars_by_code={
                "600001": {
                    date(2026, 3, 21): DailyBar(date(2026, 3, 21), 10.0, 11.1, 9.95, 10.9),
                    date(2026, 3, 22): DailyBar(date(2026, 3, 22), 10.45, 11.2, 10.3, 10.4),
                    date(2026, 3, 23): DailyBar(date(2026, 3, 23), 10.3, 10.4, 10.2, 10.25),
                }
            },
            initial_capital=100000.0,
        )
        self.assertTrue(any(trade.exit_reason == "take_profit_10pct" for trade in result.trades))
        hard_stop_trade = next((trade for trade in result.trades if trade.exit_reason == "hard_stop"), None)
        self.assertIsNotNone(hard_stop_trade)
        assert hard_stop_trade is not None
        self.assertGreaterEqual(hard_stop_trade.exit_price, 10.0)

    def test_climax_path_uses_tighter_protection_than_trend(self) -> None:
        engine = QuantBacktestEngine(commission_rate=0.0, tax_rate=0.0, slippage_rate=0.0)
        trading_dates = [date(2026, 3, 20), date(2026, 3, 21), date(2026, 3, 22), date(2026, 3, 23)]
        result = engine.run(
            trading_dates=trading_dates,
            market_regimes={day: MarketRegimeSnapshot("RiskOn", 70.0) for day in trading_dates},
            candidates_by_signal_date={
                date(2026, 3, 20): [
                    TradePlanCandidate(date(2026, 3, 20), "600001", "BK1", "AI", "TREND", "BREAKOUT", 92.0, 10.0, 9.7),
                    TradePlanCandidate(
                        date(2026, 3, 20),
                        "600002",
                        "BK2",
                        "POWER",
                        "CLIMAX",
                        "CLIMAX_PULLBACK",
                        91.0,
                        10.0,
                        9.7,
                    ),
                ]
            },
            bars_by_code={
                "600001": {
                    date(2026, 3, 21): DailyBar(date(2026, 3, 21), 10.0, 11.2, 9.95, 11.0),
                    date(2026, 3, 22): DailyBar(date(2026, 3, 22), 10.9, 11.3, 10.75, 10.8),
                    date(2026, 3, 23): DailyBar(date(2026, 3, 23), 10.8, 10.9, 10.7, 10.8),
                },
                "600002": {
                    date(2026, 3, 21): DailyBar(date(2026, 3, 21), 10.0, 11.2, 9.95, 11.0),
                    date(2026, 3, 22): DailyBar(date(2026, 3, 22), 10.9, 11.3, 10.75, 10.8),
                    date(2026, 3, 23): DailyBar(date(2026, 3, 23), 10.8, 10.9, 10.7, 10.8),
                },
            },
            initial_capital=100000.0,
        )
        climax_hard_stop = next((trade for trade in result.trades if trade.code == "600002" and trade.exit_reason == "hard_stop"), None)
        trend_hard_stop = next((trade for trade in result.trades if trade.code == "600001" and trade.exit_reason == "hard_stop"), None)
        self.assertIsNotNone(climax_hard_stop)
        self.assertIsNone(trend_hard_stop)

    def test_trend_weak_stop_recovers_within_two_days(self) -> None:
        engine = QuantBacktestEngine(commission_rate=0.0, tax_rate=0.0, slippage_rate=0.0)
        trading_dates = [
            date(2026, 3, 20),
            date(2026, 3, 21),
            date(2026, 3, 22),
            date(2026, 3, 23),
            date(2026, 3, 24),
        ]
        result = engine.run(
            trading_dates=trading_dates,
            market_regimes={day: MarketRegimeSnapshot("RiskOn", 70.0) for day in trading_dates},
            candidates_by_signal_date={
                date(2026, 3, 20): [
                    TradePlanCandidate(date(2026, 3, 20), "600003", "BK3", "POWER", "TREND", "PULLBACK", 88.0, 10.0, 9.8)
                ]
            },
            bars_by_code={
                "600003": {
                    date(2026, 3, 21): DailyBar(date(2026, 3, 21), 10.0, 10.05, 9.9, 9.95),
                    date(2026, 3, 22): DailyBar(date(2026, 3, 22), 9.92, 9.98, 9.76, 9.81),
                    date(2026, 3, 23): DailyBar(date(2026, 3, 23), 9.82, 9.92, 9.8, 9.86),
                    date(2026, 3, 24): DailyBar(date(2026, 3, 24), 9.88, 9.95, 9.85, 9.9),
                }
            },
            initial_capital=100000.0,
        )
        reasons = [trade.exit_reason for trade in result.trades]
        self.assertNotIn("weak_stop_confirmed", reasons)
        self.assertNotIn("hard_stop", reasons)
        self.assertIn("window_end", reasons)

    def test_trend_weak_stop_confirms_after_two_days_without_recovery(self) -> None:
        engine = QuantBacktestEngine(commission_rate=0.0, tax_rate=0.0, slippage_rate=0.0)
        trading_dates = [
            date(2026, 3, 20),
            date(2026, 3, 21),
            date(2026, 3, 22),
            date(2026, 3, 23),
            date(2026, 3, 24),
        ]
        result = engine.run(
            trading_dates=trading_dates,
            market_regimes={day: MarketRegimeSnapshot("RiskOn", 70.0) for day in trading_dates},
            candidates_by_signal_date={
                date(2026, 3, 20): [
                    TradePlanCandidate(date(2026, 3, 20), "600004", "BK4", "POWER", "TREND", "PULLBACK", 88.0, 10.0, 9.8)
                ]
            },
            bars_by_code={
                "600004": {
                    date(2026, 3, 21): DailyBar(date(2026, 3, 21), 10.0, 10.05, 9.9, 9.95),
                    date(2026, 3, 22): DailyBar(date(2026, 3, 22), 9.93, 9.98, 9.76, 9.81),
                    date(2026, 3, 23): DailyBar(date(2026, 3, 23), 9.8, 9.88, 9.74, 9.79),
                    date(2026, 3, 24): DailyBar(date(2026, 3, 24), 9.78, 9.86, 9.72, 9.76),
                }
            },
            initial_capital=100000.0,
        )
        weak_stop_trade = next((trade for trade in result.trades if trade.exit_reason == "weak_stop_confirmed"), None)
        self.assertIsNotNone(weak_stop_trade)
        assert weak_stop_trade is not None
        self.assertEqual(weak_stop_trade.exit_date, date(2026, 3, 24))


if __name__ == "__main__":
    unittest.main()
