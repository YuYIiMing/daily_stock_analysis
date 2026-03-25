# -*- coding: utf-8 -*-
"""Tests for quant feature logic."""

from __future__ import annotations

import unittest

from src.core.quant_features import (
    IndexSnapshot,
    BoardLeaderSnapshot,
    ConceptBoardSnapshot,
    StockSetupSnapshot,
    STAGE_IGNORE,
    STAGE_CLIMAX,
    STAGE_EMERGING,
    STAGE_TREND,
    MODULE_BREAKOUT,
    MODULE_PULLBACK,
    MODULE_LATE_WEAK_TO_STRONG,
    apply_stage_demotion,
    build_entry_plan,
    choose_entry_module,
    classify_board_stage,
    classify_market_regime,
    compute_theme_score,
    get_stage_cycle_label,
    is_board_trade_allowed,
    passes_universe_filter,
)


class QuantFeaturesTestCase(unittest.TestCase):
    def test_market_regime_risk_on(self) -> None:
        result = classify_market_regime(
            [
                IndexSnapshot("sh000001", 3300, 3310, 3290, 3250, 3000, 7),
                IndexSnapshot("sz399001", 10000, 10020, 9980, 9900, 9000, 6),
                IndexSnapshot("sz399006", 2100, 2110, 2090, 2050, 1800, 8),
            ]
        )
        self.assertEqual(result.regime, "RiskOn")
        self.assertEqual(result.max_exposure_pct, 70.0)

    def test_market_regime_risk_off_keeps_defensive_probe(self) -> None:
        result = classify_market_regime(
            [
                IndexSnapshot("sh000001", 3000, 2990, 3010, 3050, 3200, 3),
                IndexSnapshot("sz399001", 9000, 8950, 9000, 9100, 9800, 2),
                IndexSnapshot("sz399006", 1800, 1780, 1820, 1900, 2200, 1),
            ]
        )
        self.assertEqual(result.regime, "RiskOff")
        self.assertEqual(result.max_exposure_pct, 20.0)

    def test_board_stage_climax_and_demote(self) -> None:
        leader = BoardLeaderSnapshot(
            stock_code="600001",
            ret20=25.0,
            amount_5d=5e9,
            breakout_count_3d=2,
            return_2d=12.0,
            limit_up_count_3d=2,
            close_vs_ma5_pct=4.0,
        )
        board = ConceptBoardSnapshot(
            board_code="BK001",
            board_name="AI概念",
            amount=1e10,
            turnover_rank_pct=0.05,
            limit_up_count=6,
            strong_stock_count=8,
            member_count=50,
            strong_stock_ratio=0.16,
            change_3d_pct=8.0,
            up_days_3d=3,
            top5_avg_pct=4.2,
            big_drop_ratio=0.01,
            leader=leader,
        )
        score = compute_theme_score(board)
        self.assertEqual(score.theme_score, 4)
        self.assertTrue(is_board_trade_allowed(score))
        self.assertEqual(classify_board_stage(board, theme_score=score.theme_score), STAGE_CLIMAX)
        self.assertEqual(get_stage_cycle_label(STAGE_CLIMAX), "后期")
        self.assertEqual(get_stage_cycle_label(STAGE_IGNORE), "震荡")

        weak_leader = BoardLeaderSnapshot(
            stock_code="600001",
            breakout_count_3d=1,
            single_day_drop_pct=-8.0,
        )
        weak_board = ConceptBoardSnapshot(
            board_code="BK001",
            board_name="AI概念",
            amount=1e10,
            turnover_rank_pct=0.05,
            limit_up_count=2,
            strong_stock_count=4,
            member_count=40,
            strong_stock_ratio=0.10,
            change_3d_pct=3.0,
            up_days_3d=2,
            top5_avg_pct=1.0,
            big_drop_ratio=0.20,
            leader=weak_leader,
            prev_limit_up_count=6,
        )
        self.assertEqual(apply_stage_demotion(STAGE_TREND, weak_board, theme_score=3), STAGE_EMERGING)

    def test_board_stage_prefers_trend_when_climax_gate_not_met(self) -> None:
        leader = BoardLeaderSnapshot(
            stock_code="600005",
            ret20=18.0,
            amount_5d=3e9,
            breakout_count_3d=1,
            return_2d=6.0,
            limit_up_count_3d=1,
            consecutive_new_high_3d=2,
            close_vs_ma5_pct=2.6,
            close_above_ma10=True,
            low_above_ma20=True,
            pullback_volume_ratio=1.0,
        )
        board = ConceptBoardSnapshot(
            board_code="BK002",
            board_name="储能概念",
            amount=8e9,
            turnover_rank_pct=0.08,
            limit_up_count=3,
            strong_stock_count=6,
            member_count=60,
            strong_stock_ratio=0.1,
            change_3d_pct=5.0,
            up_days_3d=2,
            top5_avg_pct=2.8,
            big_drop_ratio=0.05,
            leader=leader,
        )
        score = compute_theme_score(board)
        self.assertGreaterEqual(score.theme_score, 3)
        self.assertEqual(classify_board_stage(board, theme_score=score.theme_score), STAGE_TREND)

    def test_choose_modules_and_build_entry_plan(self) -> None:
        breakout = StockSetupSnapshot(
            code="600001",
            board_code="BK001",
            board_name="AI",
            close=12.0,
            open=11.7,
            high=12.1,
            low=11.6,
            ma5=11.5,
            ma10=11.2,
            ma20=10.8,
            ma60=9.8,
            ret20=10.0,
            ret60=20.0,
            median_amount_20=5e8,
            median_turnover_20=2.0,
            platform_width_pct=8.0,
            close_above_ma20_ratio=0.9,
            breakout_pct=1.2,
            amount_ratio_5=1.8,
            close_position_ratio=0.8,
            upper_shadow_pct=2.0,
            peer_confirm_count=1,
            platform_high=11.95,
            platform_low=11.2,
            prev_high=11.9,
        )
        breakout_module = choose_entry_module(breakout, stage=STAGE_EMERGING)
        self.assertEqual(breakout_module, MODULE_BREAKOUT)
        breakout_plan = build_entry_plan(breakout, stage=STAGE_EMERGING, module=breakout_module)
        self.assertIsNotNone(breakout_plan)
        self.assertGreaterEqual(breakout_plan.planned_entry_price, breakout.platform_high)
        self.assertLess(breakout_plan.initial_stop_price, breakout_plan.planned_entry_price)

        pullback = StockSetupSnapshot(
            code="600002",
            board_code="BK001",
            board_name="AI",
            close=22.0,
            open=21.6,
            high=22.1,
            low=21.5,
            ma5=22.5,
            ma10=22.2,
            ma20=21.4,
            ma60=18.0,
            ret20=11.0,
            ret60=19.0,
            median_amount_20=6e8,
            median_turnover_20=2.2,
            pullback_pct_5d=-4.0,
            pullback_amount_ratio=0.7,
            low_vs_ma20_pct=1.0,
            low_vs_ma60_pct=1.1,
            lower_shadow_body_ratio=0.8,
            close_ge_open=True,
            prev_high=22.0,
        )
        pullback_module = choose_entry_module(pullback, stage=STAGE_TREND)
        self.assertEqual(pullback_module, MODULE_PULLBACK)
        pullback_plan = build_entry_plan(pullback, stage=STAGE_TREND, module=pullback_module)
        self.assertIsNotNone(pullback_plan)
        self.assertGreaterEqual(pullback_plan.planned_entry_price, max(pullback.close, pullback.open))
        self.assertLess(pullback_plan.initial_stop_price, pullback_plan.planned_entry_price)

        late = StockSetupSnapshot(
            code="600003",
            board_code="BK001",
            board_name="AI",
            close=33.0,
            open=32.4,
            high=33.1,
            low=32.2,
            ma5=32.5,
            ma10=31.8,
            ma20=30.0,
            ma60=26.0,
            ret20=18.0,
            ret60=35.0,
            median_amount_20=8e8,
            median_turnover_20=2.8,
            ret5=16.0,
            limit_up_count_5d=2,
            prev_close_below_ma5=True,
            close_above_ma5=True,
            close_above_prev_high=True,
            rebound_break_prev_high=True,
            weak_to_strong_amount_ratio=1.3,
            close_vs_ma5_pct=4.0,
            prev_high=32.8,
        )
        late_module = choose_entry_module(late, stage=STAGE_CLIMAX)
        self.assertIsNone(late_module)
        self.assertIsNone(build_entry_plan(late, stage=STAGE_CLIMAX))

    def test_extreme_climax_late_setup_still_has_rare_trigger(self) -> None:
        extreme_late = StockSetupSnapshot(
            code="600099",
            board_code="BK001",
            board_name="AI",
            close=45.2,
            open=44.0,
            high=45.4,
            low=43.95,
            ma5=44.0,
            ma10=42.1,
            ma20=39.8,
            ma60=32.0,
            ret20=32.0,
            ret60=55.0,
            median_amount_20=9e8,
            median_turnover_20=2.5,
            ret5=22.0,
            limit_up_count_5d=2,
            prev_close_below_ma5=True,
            close_above_ma5=True,
            close_above_prev_high=True,
            rebound_break_prev_high=True,
            weak_to_strong_amount_ratio=1.42,
            amount_ratio_5=1.36,
            close_vs_ma5_pct=2.7,
            upper_shadow_pct=1.8,
            prev_high=44.8,
        )
        module = choose_entry_module(extreme_late, stage=STAGE_CLIMAX)
        self.assertEqual(module, MODULE_LATE_WEAK_TO_STRONG)
        plan = build_entry_plan(extreme_late, stage=STAGE_CLIMAX, module=module)
        self.assertIsNotNone(plan)
        self.assertGreater(plan.planned_entry_price, 0.0)

    def test_climax_pullback_reclaim_is_not_enabled_in_baseline_mode(self) -> None:
        pullback_late = StockSetupSnapshot(
            code="600188",
            board_code="BK008",
            board_name="光伏概念",
            close=25.2,
            open=24.9,
            high=25.35,
            low=24.8,
            ma5=24.9,
            ma10=24.7,
            ma20=23.8,
            ma60=21.2,
            ret20=21.0,
            ret60=30.0,
            median_amount_20=6.5e8,
            median_turnover_20=2.1,
            ret5=9.5,
            limit_up_count_5d=1,
            prev_close_below_ma5=False,
            close_above_ma5=True,
            close_above_prev_high=False,
            rebound_break_prev_high=False,
            weak_to_strong_amount_ratio=1.05,
            amount_ratio_5=1.08,
            close_vs_ma5_pct=1.2,
            upper_shadow_pct=2.0,
        )
        module = choose_entry_module(pullback_late, stage=STAGE_CLIMAX)
        self.assertIsNone(module)
        self.assertIsNone(build_entry_plan(pullback_late, stage=STAGE_CLIMAX, module=module))

    def test_universe_filter_does_not_block_short_window_listed_days(self) -> None:
        sample = StockSetupSnapshot(
            code="600010",
            board_code="BK001",
            board_name="AI",
            close=10.5,
            open=10.3,
            high=10.6,
            low=10.2,
            ma5=10.2,
            ma10=10.1,
            ma20=9.8,
            ma60=9.2,
            ret20=10.5,
            ret60=0.0,
            median_amount_20=1.2e8,
            median_turnover_20=0.35,
            listed_days=12,
            is_main_board=True,
            is_st=False,
            is_suspended=False,
        )
        self.assertTrue(passes_universe_filter(sample))

    def test_relaxed_breakout_can_trigger_non_zero_candidate_sample(self) -> None:
        setup = StockSetupSnapshot(
            code="600011",
            board_code="BK001",
            board_name="AI",
            close=11.2,
            open=11.0,
            high=11.25,
            low=10.95,
            ma5=10.9,
            ma10=10.8,
            ma20=10.5,
            ma60=9.7,
            ret20=11.0,
            ret60=12.0,
            median_amount_20=1.5e8,
            median_turnover_20=0.6,
            platform_width_pct=14.5,
            close_above_ma20_ratio=0.78,
            breakout_pct=0.65,
            amount_ratio_5=1.25,
            close_position_ratio=0.66,
            upper_shadow_pct=4.2,
            peer_confirm_count=1,
            platform_high=11.18,
            platform_low=10.7,
            prev_high=11.15,
            prev_low=10.9,
        )
        module = choose_entry_module(setup, stage=STAGE_EMERGING)
        self.assertEqual(module, MODULE_BREAKOUT)
        plan = build_entry_plan(setup, stage=STAGE_EMERGING, module=module)
        self.assertIsNotNone(plan)
        self.assertGreater(plan.planned_entry_price, 0)


if __name__ == "__main__":
    unittest.main()
