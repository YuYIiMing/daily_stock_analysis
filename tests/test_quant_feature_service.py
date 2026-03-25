# -*- coding: utf-8 -*-
"""Tests for quant feature service behavior."""

from __future__ import annotations

import unittest
from datetime import date
from types import SimpleNamespace

from src.services.quant_feature_service import QuantFeatureService


class FakeQuantFeatureRepository:
    """Minimal repository stub for quant feature service tests."""

    def __init__(self, rows, stock_rows=None, board_rows=None):
        self._rows = rows
        self._stock_rows = stock_rows or []
        self._board_rows = board_rows or []

    def list_index_features(self, *, trade_date=None):
        return list(self._rows)

    def list_stock_features(self, *, trade_date=None, eligible_only=False):
        return list(self._stock_rows)

    def list_board_features(self, *, trade_date=None):
        rows = list(self._board_rows)
        if trade_date is not None:
            rows = [row for row in rows if getattr(row, "trade_date", None) == trade_date]
        return rows

    def list_concept_board_features(self, *, board_code=None, trade_date=None, start_date=None, end_date=None, limit=None):
        rows = list(self._board_rows)
        if board_code is not None:
            rows = [row for row in rows if getattr(row, "board_code", None) == board_code]
        if trade_date is not None:
            rows = [row for row in rows if getattr(row, "trade_date", None) == trade_date]
        if start_date is not None:
            rows = [row for row in rows if getattr(row, "trade_date", None) >= start_date]
        if end_date is not None:
            rows = [row for row in rows if getattr(row, "trade_date", None) <= end_date]
        rows.sort(key=lambda row: (getattr(row, "trade_date", date.min), getattr(row, "theme_score", 0)), reverse=True)
        return rows[:limit] if limit else rows


class QuantFeatureServiceTestCase(unittest.TestCase):
    def test_market_regime_ignores_incomplete_index_snapshot(self) -> None:
        repo = FakeQuantFeatureRepository(
            [
                SimpleNamespace(
                    index_code="sh000001",
                    close=3931.83,
                    ma5=3918.00,
                    ma10=4001.22,
                    ma20=4064.77,
                    ma250=3733.17,
                    up_day_count_10=3,
                ),
                SimpleNamespace(
                    index_code="sz399001",
                    close=13801.00,
                    ma5=13690.16,
                    ma10=13964.16,
                    ma20=14109.74,
                    ma250=12169.35,
                    up_day_count_10=4,
                ),
                SimpleNamespace(
                    index_code="sz399006",
                    close=3316.97,
                    ma5=3316.97,
                    ma10=3316.97,
                    ma20=3316.97,
                    ma250=3316.97,
                    up_day_count_10=0,
                ),
            ]
        )
        service = QuantFeatureService(repository=repo)

        regime = service.get_market_regime(date(2026, 3, 25))

        self.assertEqual(regime.regime, "Neutral")
        self.assertEqual(regime.max_exposure_pct, 30.0)
        self.assertAlmostEqual(regime.score, 1.0)
        self.assertEqual(len(regime.index_results), 2)

    def test_trade_candidates_fallback_to_board_name_when_board_code_differs(self) -> None:
        stock_rows = [
            SimpleNamespace(
                code="002281",
                board_code="BK1134",
                board_name="算力概念",
                stage=None,
                close=12.6,
                ma5=12.1,
                ma10=11.8,
                ma20=11.2,
                ma60=10.0,
                ret20=15.0,
                ret60=18.0,
                median_amount_20=3.2e8,
                median_turnover_20=1.5,
                signal_score=61.0,
                raw_payload_json=(
                    '{"open":12.2,"high":12.8,"low":12.0,"listed_days":1200,'
                    '"is_main_board":true,"is_st":false,"is_suspended":false,'
                    '"close_above_ma20_ratio":0.8,"platform_width_pct":12.0,'
                    '"breakout_pct":2.3,"amount_ratio_5":1.25,"close_position_ratio":0.82,'
                    '"upper_shadow_pct":1.2,"peer_confirm_count":1,'
                    '"platform_high":12.6,"platform_low":11.7,"prev_high":12.4,"prev_low":11.9}'
                ),
            )
        ]
        repo = FakeQuantFeatureRepository([], stock_rows=stock_rows)
        service = QuantFeatureService(repository=repo)
        service.get_board_stage_map = lambda trade_date: {
            "EM_FAKE_001": {
                "board_name": "算力概念",
                "theme_score": 3,
                "stage": "TREND",
                "stage_cycle_label": "中期",
                "trade_allowed": True,
                "components": {},
            }
        }

        candidates = service.get_trade_candidates(date(2026, 3, 25))

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].code, "002281")
        self.assertEqual(candidates[0].board_name, "算力概念")
        self.assertEqual(candidates[0].stage, "TREND")
        self.assertEqual(candidates[0].entry_module, "BREAKOUT")

    def test_trade_candidates_use_recent_board_feature_fallback(self) -> None:
        stock_rows = [
            SimpleNamespace(
                code="002281",
                board_code="BK1134",
                board_name="算力概念",
                stage=None,
                close=12.6,
                ma5=12.1,
                ma10=11.8,
                ma20=11.2,
                ma60=10.0,
                ret20=15.0,
                ret60=18.0,
                median_amount_20=3.2e8,
                median_turnover_20=1.5,
                signal_score=61.0,
                raw_payload_json=(
                    '{"open":12.2,"high":12.8,"low":12.0,"listed_days":1200,'
                    '"is_main_board":true,"is_st":false,"is_suspended":false,'
                    '"close_above_ma20_ratio":0.8,"platform_width_pct":12.0,'
                    '"breakout_pct":2.3,"amount_ratio_5":1.25,"close_position_ratio":0.82,'
                    '"upper_shadow_pct":1.2,"peer_confirm_count":1,'
                    '"platform_high":12.6,"platform_low":11.7,"prev_high":12.4,"prev_low":11.9}'
                ),
            )
        ]
        board_rows = [
            SimpleNamespace(
                board_code="EM_FAKE_001",
                board_name="算力概念",
                trade_date=date(2026, 3, 24),
                amount=100.0,
                turnover_rank_pct=0.05,
                limit_up_count=1,
                strong_stock_count=4,
                breadth_ratio=0.1,
                leader_stock_code="002281",
                leader_stock_name="002281",
                leader_2d_return=8.5,
                leader_limit_up_3d=1,
                raw_payload_json=(
                    '{"member_count":40,"change_3d_pct":6.5,"up_days_3d":2,'
                    '"top5_avg_pct":4.1,"big_drop_ratio":0.01,"limit_down_count":0,'
                    '"prev_limit_up_count":1,"member_fall20_ratio":0.02,'
                    '"leader":{"ret20":18.0,"amount_5d":100000000.0,"breakout_count_3d":2,'
                    '"consecutive_new_high_3d":2,"close_vs_ma5_pct":1.8,"close_above_ma10":true,'
                    '"low_above_ma20":true,"pullback_volume_ratio":0.9,"single_day_drop_pct":-1.0,'
                    '"broke_ma10_with_volume":false,"broke_ma20":false,"is_limit_down":false,'
                    '"close_to_5d_high_drawdown_pct":2.0,"return_2d":8.5,"limit_up_count_3d":1}}'
                ),
            )
        ]
        repo = FakeQuantFeatureRepository([], stock_rows=stock_rows, board_rows=board_rows)
        service = QuantFeatureService(repository=repo)
        service.get_board_stage_map = lambda trade_date: {}

        candidates = service.get_trade_candidates(date(2026, 3, 25))

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].code, "002281")
        self.assertEqual(candidates[0].reason["board_feature_trade_date"], "2026-03-24")

    def test_trade_candidates_match_board_alias_name(self) -> None:
        stock_rows = [
            SimpleNamespace(
                code="002025",
                board_code="BK1128",
                board_name="CPO概念",
                stage=None,
                close=18.6,
                ma5=18.1,
                ma10=17.8,
                ma20=17.2,
                ma60=15.0,
                ret20=19.0,
                ret60=25.0,
                median_amount_20=3.5e8,
                median_turnover_20=1.6,
                signal_score=62.0,
                raw_payload_json=(
                    '{"open":18.1,"high":18.9,"low":17.9,"listed_days":1500,'
                    '"is_main_board":true,"is_st":false,"is_suspended":false,'
                    '"close_above_ma20_ratio":0.9,"platform_width_pct":9.0,'
                    '"breakout_pct":2.1,"amount_ratio_5":1.35,"close_position_ratio":0.85,'
                    '"upper_shadow_pct":1.1,"peer_confirm_count":1,'
                    '"platform_high":18.4,"platform_low":17.1,"prev_high":18.2,"prev_low":17.8}'
                ),
            )
        ]
        board_rows = [
            SimpleNamespace(
                board_code="EM_CPO",
                board_name="共封装光学(CPO)",
                trade_date=date(2026, 3, 24),
                amount=110.0,
                turnover_rank_pct=0.04,
                limit_up_count=1,
                strong_stock_count=4,
                breadth_ratio=0.12,
                leader_stock_code="002025",
                leader_stock_name="002025",
                leader_2d_return=8.8,
                leader_limit_up_3d=1,
                raw_payload_json=(
                    '{"member_count":28,"change_3d_pct":7.0,"up_days_3d":2,'
                    '"top5_avg_pct":4.2,"big_drop_ratio":0.01,"limit_down_count":0,'
                    '"prev_limit_up_count":1,"member_fall20_ratio":0.02,'
                    '"leader":{"ret20":22.0,"amount_5d":120000000.0,"breakout_count_3d":2,'
                    '"consecutive_new_high_3d":2,"close_vs_ma5_pct":1.5,"close_above_ma10":true,'
                    '"low_above_ma20":true,"pullback_volume_ratio":0.8,"single_day_drop_pct":-0.5,'
                    '"broke_ma10_with_volume":false,"broke_ma20":false,"is_limit_down":false,'
                    '"close_to_5d_high_drawdown_pct":1.8,"return_2d":8.8,"limit_up_count_3d":1}}'
                ),
            )
        ]
        repo = FakeQuantFeatureRepository([], stock_rows=stock_rows, board_rows=board_rows)
        service = QuantFeatureService(repository=repo)
        service.get_board_stage_map = lambda trade_date: {}

        candidates = service.get_trade_candidates(date(2026, 3, 25))

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].code, "002025")
        self.assertEqual(candidates[0].reason["board_feature_trade_date"], "2026-03-24")

    def test_recent_board_fallback_ignores_unclassified_placeholder(self) -> None:
        board_rows = [
            SimpleNamespace(
                board_code="EM_UNKNOWN",
                board_name="未归类概念",
                trade_date=date(2026, 3, 25),
                amount=100.0,
                turnover_rank_pct=0.05,
                limit_up_count=3,
                strong_stock_count=5,
                breadth_ratio=0.1,
                leader_stock_code="000001",
                leader_stock_name="000001",
                leader_2d_return=8.0,
                leader_limit_up_3d=1,
                raw_payload_json='{"member_count":40}',
            ),
            SimpleNamespace(
                board_code="EM_REAL",
                board_name="储能概念",
                trade_date=date(2026, 3, 24),
                amount=80.0,
                turnover_rank_pct=0.08,
                limit_up_count=1,
                strong_stock_count=4,
                breadth_ratio=0.1,
                leader_stock_code="002112",
                leader_stock_name="002112",
                leader_2d_return=7.2,
                leader_limit_up_3d=1,
                raw_payload_json='{"member_count":30,"change_3d_pct":4.0,"up_days_3d":2}',
            ),
        ]
        repo = FakeQuantFeatureRepository([], board_rows=board_rows)
        service = QuantFeatureService(repository=repo)

        recent = service.get_recent_board_stage_by_name(date(2026, 3, 25))

        self.assertNotIn("未归类概念", recent)
        self.assertIn("储能概念", recent)

    def test_same_day_board_map_ignores_unclassified_placeholder(self) -> None:
        board_rows = [
            SimpleNamespace(
                board_code="EM_UNKNOWN",
                board_name="未归类概念",
                trade_date=date(2026, 3, 25),
                amount=100.0,
                turnover_rank_pct=0.05,
                limit_up_count=3,
                strong_stock_count=5,
                breadth_ratio=0.1,
                leader_stock_code="000001",
                leader_stock_name="000001",
                leader_2d_return=8.0,
                leader_limit_up_3d=1,
                raw_payload_json='{"member_count":40}',
            )
        ]
        repo = FakeQuantFeatureRepository([], board_rows=board_rows)
        service = QuantFeatureService(repository=repo)

        board_map = service.get_board_stage_map(date(2026, 3, 25))

        self.assertEqual(board_map, {})


if __name__ == "__main__":
    unittest.main()
