# -*- coding: utf-8 -*-
"""Tests for quant feature logic."""

from __future__ import annotations

import unittest

from src.core.quant_features import (
    BoardLeaderSnapshot,
    ConceptBoardSnapshot,
    IndexSnapshot,
    MODULE_BREAKOUT,
    MODULE_CLIMAX_PULLBACK,
    MODULE_CLIMAX_WEAK_TO_STRONG,
    MODULE_PULLBACK,
    STAGE_CLIMAX,
    STAGE_EMERGING,
    STAGE_IGNORE,
    STAGE_TREND,
    StockSetupSnapshot,
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

    def test_stage_priority_prefers_climax_over_trend_and_emerging(self) -> None:
        leader = BoardLeaderSnapshot(
            stock_code="600001",
            ret20=35.0,
            amount_5d=6e9,
            breakout_count_3d=2,
            return_2d=16.0,
            limit_up_count_3d=2,
            consecutive_new_high_3d=3,
            close_vs_ma5_pct=10.5,
            close_above_ma10=True,
            low_above_ma20=True,
            pullback_volume_ratio=0.8,
            close_to_5d_high_drawdown_pct=3.5,
        )
        board = ConceptBoardSnapshot(
            board_code="BK001",
            board_name="AI概念",
            amount=1e10,
            turnover_rank_pct=0.05,
            limit_up_count=6,
            strong_stock_count=10,
            member_count=50,
            strong_stock_ratio=0.20,
            change_3d_pct=9.0,
            up_days_3d=3,
            top5_avg_pct=8.5,
            big_drop_ratio=0.05,
            leader=leader,
        )
        score = compute_theme_score(board)
        self.assertEqual(score.theme_score, 4)
        self.assertTrue(is_board_trade_allowed(score))
        self.assertEqual(classify_board_stage(board, theme_score=score.theme_score), STAGE_CLIMAX)

    def test_stage_trend_allows_orderly_consolidation(self) -> None:
        leader = BoardLeaderSnapshot(
            stock_code="600002",
            ret20=18.0,
            breakout_count_3d=2,
            return_2d=8.0,
            limit_up_count_3d=1,
            consecutive_new_high_3d=2,
            close_vs_ma5_pct=2.0,
            close_above_ma10=True,
            low_above_ma20=True,
            pullback_volume_ratio=0.95,
            close_to_5d_high_drawdown_pct=7.5,
        )
        board = ConceptBoardSnapshot(
            board_code="BK002",
            board_name="储能概念",
            amount=8e9,
            turnover_rank_pct=0.08,
            limit_up_count=2,
            strong_stock_count=5,
            member_count=60,
            strong_stock_ratio=0.09,
            change_3d_pct=4.0,
            up_days_3d=2,
            top5_avg_pct=4.2,
            big_drop_ratio=0.06,
            leader=leader,
        )
        score = compute_theme_score(board)
        self.assertGreaterEqual(score.theme_score, 2)
        self.assertEqual(classify_board_stage(board, theme_score=score.theme_score), STAGE_TREND)

    def test_stage_emerging_boundary(self) -> None:
        leader = BoardLeaderSnapshot(
            stock_code="600021",
            breakout_count_3d=1,
            return_2d=7.5,
            limit_up_count_3d=1,
            close_vs_ma5_pct=1.2,
            close_to_5d_high_drawdown_pct=3.0,
        )
        board = ConceptBoardSnapshot(
            board_code="BK021",
            board_name="低空经济",
            amount=6e9,
            turnover_rank_pct=0.09,
            limit_up_count=2,
            strong_stock_count=3,
            member_count=70,
            strong_stock_ratio=0.05,
            change_3d_pct=2.5,
            up_days_3d=2,
            top5_avg_pct=3.5,
            big_drop_ratio=0.10,
            leader=leader,
        )
        score = compute_theme_score(board)
        self.assertGreaterEqual(score.theme_score, 2)
        self.assertEqual(classify_board_stage(board, theme_score=score.theme_score), STAGE_EMERGING)

    def test_climax_is_strict_and_does_not_swallow_trend(self) -> None:
        weak_leader = BoardLeaderSnapshot(
            stock_code="600003",
            ret20=28.0,
            breakout_count_3d=2,
            return_2d=12.0,
            limit_up_count_3d=1,
            consecutive_new_high_3d=2,
            close_vs_ma5_pct=7.0,
            close_above_ma10=True,
            low_above_ma20=True,
            pullback_volume_ratio=0.9,
            close_to_5d_high_drawdown_pct=5.0,
        )
        weak_board = ConceptBoardSnapshot(
            board_code="BK003",
            board_name="机器人概念",
            amount=1e10,
            turnover_rank_pct=0.05,
            limit_up_count=4,
            strong_stock_count=8,
            member_count=50,
            strong_stock_ratio=0.16,
            change_3d_pct=6.0,
            up_days_3d=3,
            top5_avg_pct=6.8,
            big_drop_ratio=0.06,
            leader=weak_leader,
        )
        score = compute_theme_score(weak_board)
        self.assertEqual(classify_board_stage(weak_board, theme_score=score.theme_score), STAGE_TREND)

    def test_stage_demotion_path(self) -> None:
        leader = BoardLeaderSnapshot(
            stock_code="600004",
            ret20=25.0,
            breakout_count_3d=1,
            return_2d=9.0,
            limit_up_count_3d=0,
            consecutive_new_high_3d=1,
            close_vs_ma5_pct=1.5,
            close_above_ma10=True,
            low_above_ma20=True,
            pullback_volume_ratio=1.0,
            single_day_drop_pct=-8.0,
            broke_ma10_with_volume=True,
        )
        board = ConceptBoardSnapshot(
            board_code="BK004",
            board_name="芯片概念",
            amount=8e9,
            turnover_rank_pct=0.08,
            limit_up_count=1,
            strong_stock_count=3,
            member_count=60,
            strong_stock_ratio=0.06,
            change_3d_pct=1.5,
            up_days_3d=2,
            top5_avg_pct=2.0,
            big_drop_ratio=0.08,
            leader=leader,
            prev_limit_up_count=4,
        )
        self.assertEqual(apply_stage_demotion(STAGE_CLIMAX, board, theme_score=3), STAGE_TREND)
        self.assertEqual(apply_stage_demotion(STAGE_TREND, board, theme_score=3), STAGE_EMERGING)
        severe = ConceptBoardSnapshot(**{**board.__dict__, "limit_down_count": 2})
        self.assertEqual(apply_stage_demotion(STAGE_TREND, severe, theme_score=3), STAGE_IGNORE)
        self.assertEqual(get_stage_cycle_label(STAGE_CLIMAX), "后期")
        self.assertEqual(get_stage_cycle_label(STAGE_IGNORE), "震荡")

    def test_breakout_and_pullback_modules(self) -> None:
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
            platform_days=8,
            close_above_ma20_ratio=0.90,
            breakout_pct=1.2,
            amount_ratio_5=1.8,
            close_position_ratio=0.8,
            upper_shadow_pct=2.0,
            peer_confirm_count=1,
            platform_high=11.95,
            platform_low=11.2,
            prev_high=11.9,
        )
        breakout_module = choose_entry_module(breakout, stage=STAGE_TREND)
        self.assertEqual(breakout_module, MODULE_BREAKOUT)
        breakout_plan = build_entry_plan(breakout, stage=STAGE_TREND, module=breakout_module)
        self.assertIsNotNone(breakout_plan)
        self.assertGreater(breakout_plan.planned_entry_price, breakout.platform_high)
        self.assertLess(breakout_plan.initial_stop_price, breakout_plan.planned_entry_price)
        breakout_stop_pct = (breakout_plan.planned_entry_price - breakout_plan.initial_stop_price) / breakout_plan.planned_entry_price
        self.assertLessEqual(breakout_stop_pct, 0.0801)

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
            prior_breakout_count_20d=1,
            pullback_pct_5d=-4.0,
            pullback_amount_ratio=0.7,
            low_vs_ma20_pct=1.0,
            low_vs_ma60_pct=1.1,
            lower_shadow_body_ratio=0.8,
            close_ge_open=True,
            prev_high=22.0,
            amount_ratio_5=1.25,
            rebound_break_prev_high=True,
        )
        pullback_module = choose_entry_module(pullback, stage=STAGE_TREND)
        self.assertEqual(pullback_module, MODULE_PULLBACK)
        pullback_plan = build_entry_plan(pullback, stage=STAGE_TREND, module=pullback_module)
        self.assertIsNotNone(pullback_plan)
        self.assertGreaterEqual(pullback_plan.planned_entry_price, max(pullback.close, pullback.open))
        self.assertLess(pullback_plan.initial_stop_price, pullback_plan.planned_entry_price)
        pullback_stop_pct = (pullback_plan.planned_entry_price - pullback_plan.initial_stop_price) / pullback_plan.planned_entry_price
        self.assertLessEqual(pullback_stop_pct, 0.0601)

    def test_breakout_requires_platform_days_window(self) -> None:
        short_platform = StockSetupSnapshot(
            code="600011",
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
            platform_days=3,
            close_above_ma20_ratio=0.90,
            breakout_pct=1.2,
            amount_ratio_5=1.8,
            close_position_ratio=0.8,
            upper_shadow_pct=2.0,
            peer_confirm_count=1,
        )
        self.assertIsNone(choose_entry_module(short_platform, stage=STAGE_EMERGING))

    def test_breakout_rejects_overextended_price_deviation(self) -> None:
        overextended = StockSetupSnapshot(
            code="600013",
            board_code="BK001",
            board_name="AI",
            close=13.0,
            open=12.3,
            high=13.2,
            low=12.2,
            ma5=12.0,
            ma10=11.6,
            ma20=11.1,
            ma60=9.8,
            ret20=11.0,
            ret60=21.0,
            median_amount_20=5e8,
            median_turnover_20=2.0,
            platform_width_pct=8.5,
            platform_days=9,
            close_above_ma20_ratio=0.90,
            breakout_pct=1.3,
            amount_ratio_5=1.7,
            close_position_ratio=0.80,
            upper_shadow_pct=1.9,
            peer_confirm_count=1,
            close_vs_ma5_pct=8.5,
        )
        self.assertIsNone(choose_entry_module(overextended, stage=STAGE_TREND))

    def test_breakout_rejects_stale_repeated_breakout(self) -> None:
        stale_breakout = StockSetupSnapshot(
            code="600014",
            board_code="BK001",
            board_name="AI",
            close=12.6,
            open=12.2,
            high=12.8,
            low=12.1,
            ma5=12.1,
            ma10=11.8,
            ma20=11.2,
            ma60=9.9,
            ret20=10.8,
            ret60=18.0,
            median_amount_20=5e8,
            median_turnover_20=1.8,
            platform_width_pct=7.8,
            platform_days=10,
            close_above_ma20_ratio=0.90,
            breakout_pct=1.2,
            amount_ratio_5=1.6,
            close_position_ratio=0.78,
            upper_shadow_pct=2.2,
            peer_confirm_count=1,
            prior_breakout_count_20d=2,
        )
        self.assertIsNone(choose_entry_module(stale_breakout, stage=STAGE_TREND))

    def test_pullback_requires_prior_breakout_evidence(self) -> None:
        no_prior_breakout = StockSetupSnapshot(
            code="600012",
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
            ret20=10.0,
            ret60=19.0,
            median_amount_20=6e8,
            median_turnover_20=2.2,
            prior_breakout_count_20d=0,
            pullback_pct_5d=-4.0,
            pullback_amount_ratio=0.7,
            low_vs_ma20_pct=1.0,
            low_vs_ma60_pct=1.1,
            lower_shadow_body_ratio=0.8,
            close_ge_open=True,
            prev_high=22.0,
            amount_ratio_5=1.25,
            rebound_break_prev_high=True,
            close_above_ma20_ratio=0.80,
        )
        self.assertIsNone(choose_entry_module(no_prior_breakout, stage=STAGE_TREND))

    def test_climax_modules_detection_and_entry_plans(self) -> None:
        climax_pullback = StockSetupSnapshot(
            code="600003",
            board_code="BK001",
            board_name="AI",
            close=33.2,
            open=33.0,
            high=33.4,
            low=32.8,
            ma5=33.0,
            ma10=32.7,
            ma20=30.0,
            ma60=26.0,
            ret20=24.0,
            ret60=35.0,
            median_amount_20=8e8,
            median_turnover_20=2.8,
            ret5=16.5,
            limit_up_count_5d=2,
            prev_close_below_ma5=False,
            close_above_ma5=True,
            close_above_prev_high=False,
            rebound_break_prev_high=False,
            weak_to_strong_amount_ratio=1.05,
            close_vs_ma5_pct=0.6,
            amount_ratio_5=1.10,
            pullback_amount_ratio=0.75,
            prev_high=33.6,
        )
        module_pullback = choose_entry_module(climax_pullback, stage=STAGE_CLIMAX)
        self.assertEqual(module_pullback, MODULE_CLIMAX_PULLBACK)
        plan_pullback = build_entry_plan(climax_pullback, stage=STAGE_CLIMAX, module=module_pullback)
        self.assertIsNotNone(plan_pullback)
        self.assertEqual(plan_pullback.module, MODULE_CLIMAX_PULLBACK)
        self.assertLess(plan_pullback.initial_stop_price, plan_pullback.planned_entry_price)

        climax_w2s = StockSetupSnapshot(
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
        module_w2s = choose_entry_module(climax_w2s, stage=STAGE_CLIMAX)
        self.assertEqual(module_w2s, MODULE_CLIMAX_WEAK_TO_STRONG)
        plan = build_entry_plan(climax_w2s, stage=STAGE_CLIMAX, module=module_w2s)
        self.assertIsNotNone(plan)
        self.assertEqual(plan.module, MODULE_CLIMAX_WEAK_TO_STRONG)
        self.assertLess(plan.initial_stop_price, plan.planned_entry_price)

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


if __name__ == "__main__":
    unittest.main()
