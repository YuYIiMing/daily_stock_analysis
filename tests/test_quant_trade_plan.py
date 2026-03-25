# -*- coding: utf-8 -*-
"""Tests for quant trade-plan sizing and gating."""

from __future__ import annotations

import unittest
from datetime import date

from src.core.quant_strategy_engine import (
    MarketRegimeSnapshot,
    QuantStrategyEngine,
    StrategyRiskState,
    TradePlanCandidate,
)


class QuantTradePlanTestCase(unittest.TestCase):
    def test_non_zero_allocation_sample(self) -> None:
        engine = QuantStrategyEngine()
        items = engine.build_trade_plan(
            candidates=[
                TradePlanCandidate(
                    signal_date=date(2026, 3, 20),
                    code="600000",
                    board_code="BKX",
                    board_name="样例概念",
                    stage="EMERGING",
                    entry_module="BREAKOUT",
                    signal_score=88.0,
                    planned_entry_price=10.0,
                    initial_stop_price=9.5,
                )
            ],
            market_regime=MarketRegimeSnapshot(regime="RiskOn", max_exposure_pct=70.0),
        )
        self.assertEqual(len(items), 1)
        self.assertGreater(items[0].planned_position_pct, 0.0)

    def test_risk_off_keeps_defensive_position_budget(self) -> None:
        engine = QuantStrategyEngine()
        items = engine.build_trade_plan(
            candidates=[
                TradePlanCandidate(
                    signal_date=date(2026, 3, 20),
                    code="600001",
                    board_code="BK1",
                    board_name="AI",
                    stage="TREND",
                    entry_module="BREAKOUT",
                    signal_score=90.0,
                    planned_entry_price=10.0,
                    initial_stop_price=9.0,
                )
            ],
            market_regime=MarketRegimeSnapshot(regime="RiskOff", max_exposure_pct=20.0),
        )
        self.assertGreater(items[0].planned_position_pct, 0.0)
        self.assertLessEqual(items[0].planned_position_pct, 20.0)
        self.assertIsNone(items[0].blocked_reason)

    def test_capacity_and_board_caps(self) -> None:
        engine = QuantStrategyEngine()
        items = engine.build_trade_plan(
            candidates=[
                TradePlanCandidate(date(2026, 3, 20), "600001", "BK1", "AI", "TREND", "BREAKOUT", 90, 10.0, 9.5),
                TradePlanCandidate(date(2026, 3, 20), "600002", "BK1", "AI", "TREND", "BREAKOUT", 80, 12.0, 11.4),
                TradePlanCandidate(date(2026, 3, 20), "600003", "BK2", "机器人", "TREND", "BREAKOUT", 70, 20.0, 19.0),
            ],
            market_regime=MarketRegimeSnapshot(regime="RiskOn", max_exposure_pct=30.0),
        )
        self.assertGreater(items[0].planned_position_pct, 0.0)
        self.assertGreater(items[1].planned_position_pct, 0.0)
        self.assertLessEqual(items[0].planned_position_pct + items[1].planned_position_pct, 30.0)
        self.assertLessEqual(items[0].planned_position_pct + items[1].planned_position_pct, 40.0)

    def test_single_and_board_caps_follow_20_and_40(self) -> None:
        engine = QuantStrategyEngine()
        items = engine.build_trade_plan(
            candidates=[
                TradePlanCandidate(date(2026, 3, 20), "600101", "BK1", "AI", "TREND", "BREAKOUT", 95, 10.0, 9.9),
                TradePlanCandidate(date(2026, 3, 20), "600102", "BK1", "AI", "TREND", "BREAKOUT", 90, 10.0, 9.9),
                TradePlanCandidate(date(2026, 3, 20), "600103", "BK1", "AI", "TREND", "BREAKOUT", 85, 10.0, 9.9),
            ],
            market_regime=MarketRegimeSnapshot(regime="RiskOn", max_exposure_pct=70.0),
        )
        self.assertEqual(round(items[0].planned_position_pct, 2), 20.0)
        self.assertEqual(round(items[1].planned_position_pct, 2), 20.0)
        self.assertEqual(items[2].planned_position_pct, 0.0)
        self.assertEqual(items[2].blocked_reason, "capacity_exhausted")

    def test_cooldown_after_three_stop_losses(self) -> None:
        state = StrategyRiskState()
        state.register_stop(True)
        state.register_stop(True)
        state.register_stop(True)
        engine = QuantStrategyEngine()
        items = engine.build_trade_plan(
            candidates=[
                TradePlanCandidate(date(2026, 3, 20), "600001", "BK1", "AI", "TREND", "BREAKOUT", 90, 10.0, 9.0),
            ],
            market_regime=MarketRegimeSnapshot(regime="RiskOn", max_exposure_pct=70.0),
            risk_state=state,
        )
        self.assertEqual(items[0].blocked_reason, "cooldown_active")


if __name__ == "__main__":
    unittest.main()
