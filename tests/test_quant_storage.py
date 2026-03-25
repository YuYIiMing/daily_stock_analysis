# -*- coding: utf-8 -*-
"""Tests for quant persistence models and repositories."""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import date

from sqlalchemy import inspect

from src.config import Config
from src.repositories.quant_backtest_repo import QuantBacktestRepository
from src.repositories.quant_feature_repo import QuantFeatureRepository
from src.storage import DatabaseManager


class QuantStorageTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._temp_dir.name, "test_quant_storage.db")

        os.environ["DATABASE_PATH"] = self._db_path
        Config._instance = None
        DatabaseManager.reset_instance()
        self.db = DatabaseManager.get_instance()

    def tearDown(self) -> None:
        DatabaseManager.reset_instance()
        Config._instance = None
        self._temp_dir.cleanup()

    def test_quant_tables_exist(self) -> None:
        inspector = inspect(self.db._engine)
        tables = set(inspector.get_table_names())
        expected = {
            "index_daily_features",
            "concept_board_daily_features",
            "stock_daily_features",
            "stock_concept_membership_daily",
            "quant_backtest_runs",
            "quant_daily_trade_signals",
            "quant_trade_ledger",
            "quant_portfolio_equity_curve",
        }
        self.assertTrue(expected.issubset(tables))

    def test_quant_feature_repository_round_trip(self) -> None:
        repo = QuantFeatureRepository(self.db)
        trade_day = date(2026, 3, 20)

        saved = repo.upsert_index_features(
            [
                {
                    "index_code": "sh000001",
                    "trade_date": trade_day,
                    "close": 3333.3,
                    "ma5": 3300.0,
                    "ma10": 3280.0,
                    "ma20": 3250.0,
                    "ma250": 3000.0,
                    "up_day_count_10": 7,
                    "regime_score": 3.0,
                    "data_source": "unit_test",
                }
            ]
        )
        self.assertEqual(saved, 1)

        saved = repo.upsert_index_features(
            [
                {
                    "index_code": "sh000001",
                    "trade_date": trade_day,
                    "close": 3344.4,
                    "ma5": 3310.0,
                    "ma10": 3290.0,
                    "ma20": 3260.0,
                    "ma250": 3005.0,
                    "up_day_count_10": 8,
                    "regime_score": 2.5,
                    "data_source": "unit_test",
                }
            ]
        )
        self.assertEqual(saved, 1)

        index_rows = repo.list_index_features(index_code="sh000001")
        self.assertEqual(len(index_rows), 1)
        self.assertAlmostEqual(index_rows[0].close, 3344.4)
        self.assertEqual(index_rows[0].up_day_count_10, 8)

        saved = repo.upsert_concept_board_features(
            [
                {
                    "board_code": "BK1234",
                    "board_name": "AI算力",
                    "trade_date": trade_day,
                    "pct_chg": 3.2,
                    "amount": 15000000000.0,
                    "turnover_rank_pct": 0.08,
                    "limit_up_count": 5,
                    "strong_stock_count": 7,
                    "breadth_ratio": 0.21,
                    "consistency_score": 1.0,
                    "theme_score": 4,
                    "leader_stock_code": "600001",
                    "leader_stock_name": "测试龙头",
                    "stage": "TREND",
                    "data_source": "unit_test",
                }
            ]
        )
        self.assertEqual(saved, 1)
        board_rows = repo.list_concept_board_features(board_code="BK1234")
        self.assertEqual(len(board_rows), 1)
        self.assertEqual(board_rows[0].theme_score, 4)

        saved = repo.upsert_stock_features(
            [
                {
                    "code": "600001",
                    "trade_date": trade_day,
                    "board_code": "BK1234",
                    "board_name": "AI算力",
                    "close": 12.3,
                    "ma5": 12.1,
                    "ma10": 11.8,
                    "ma20": 11.2,
                    "ma60": 10.2,
                    "ret20": 12.5,
                    "ret60": 23.4,
                    "median_amount_20": 600000000.0,
                    "median_turnover_20": 2.3,
                    "above_ma60": True,
                    "eligible_universe": True,
                    "trigger_module": "BREAKOUT",
                    "signal_score": 88.0,
                }
            ]
        )
        self.assertEqual(saved, 1)
        stock_rows = repo.list_stock_features(code="600001", eligible_only=True)
        self.assertEqual(len(stock_rows), 1)
        self.assertEqual(stock_rows[0].trigger_module, "BREAKOUT")

        saved = repo.upsert_stock_concept_memberships(
            [
                {
                    "code": "600001",
                    "trade_date": trade_day,
                    "board_code": "BK1234",
                    "board_name": "AI算力",
                    "is_primary": True,
                }
            ]
        )
        self.assertEqual(saved, 1)
        memberships = repo.list_stock_concept_memberships(code="600001", trade_date=trade_day)
        self.assertEqual(len(memberships), 1)
        self.assertTrue(memberships[0].is_primary)

    def test_quant_backtest_repository_round_trip(self) -> None:
        repo = QuantBacktestRepository(self.db)
        trade_day = date(2026, 3, 20)

        run = repo.create_run(
            strategy_name="concept_trend_v1",
            market_scope="cn_main_board",
            board_source="eastmoney_concept",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 3, 20),
            initial_capital=1000000.0,
        )
        self.assertIsNotNone(run.id)

        updated = repo.update_run_status(run.id, status="running")
        self.assertTrue(updated)
        self.assertEqual(repo.get_run(run.id).status, "running")

        saved = repo.save_signals(
            run.id,
            [
                {
                    "trade_date": trade_day,
                    "code": "600001",
                    "board_code": "BK1234",
                    "stage": "TREND",
                    "entry_module": "BREAKOUT",
                    "direction": "long",
                    "signal_score": 88.0,
                    "planned_entry_price": 12.3,
                    "initial_stop_price": 11.6,
                    "planned_position_pct": 0.15,
                    "reason_json": "{\"why\":\"unit\"}",
                }
            ],
        )
        self.assertEqual(saved, 1)

        saved = repo.save_signals(
            run.id,
            [
                {
                    "trade_date": trade_day,
                    "code": "600001",
                    "board_code": "BK1234",
                    "stage": "TREND",
                    "entry_module": "BREAKOUT",
                    "direction": "long",
                    "signal_score": 90.0,
                    "planned_entry_price": 12.5,
                    "initial_stop_price": 11.7,
                    "planned_position_pct": 0.2,
                    "blocked_reason": "gap_too_high",
                }
            ],
        )
        self.assertEqual(saved, 1)
        signals = repo.list_signals(run.id)
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].blocked_reason, "gap_too_high")
        self.assertAlmostEqual(signals[0].signal_score, 90.0)

        saved = repo.append_trades(
            run.id,
            [
                {
                    "code": "600001",
                    "board_code": "BK1234",
                    "entry_date": trade_day,
                    "entry_price": 12.5,
                    "shares": 12000,
                    "entry_module": "BREAKOUT",
                    "stage": "TREND",
                    "status": "open",
                }
            ],
        )
        self.assertEqual(saved, 1)

        saved = repo.append_trades(
            run.id,
            [
                {
                    "code": "600001",
                    "board_code": "BK1234",
                    "entry_date": trade_day,
                    "entry_price": 12.5,
                    "exit_date": date(2026, 3, 24),
                    "exit_price": 13.8,
                    "shares": 12000,
                    "entry_module": "BREAKOUT",
                    "stage": "TREND",
                    "status": "closed",
                    "pnl_pct": 10.4,
                    "pnl_amount": 15600.0,
                    "exit_reason": "take_profit",
                }
            ],
        )
        self.assertEqual(saved, 1)
        trades = repo.list_trades(run.id)
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].status, "closed")
        self.assertAlmostEqual(trades[0].pnl_amount, 15600.0)

        saved = repo.save_equity_curve(
            run.id,
            [
                {
                    "trade_date": trade_day,
                    "cash": 850000.0,
                    "market_value": 150000.0,
                    "equity": 1000000.0,
                    "drawdown_pct": 0.0,
                    "exposure_pct": 0.15,
                },
                {
                    "trade_date": date(2026, 3, 24),
                    "cash": 1015600.0,
                    "market_value": 0.0,
                    "equity": 1015600.0,
                    "drawdown_pct": 0.0,
                    "exposure_pct": 0.0,
                },
            ],
        )
        self.assertEqual(saved, 2)

        curve = repo.get_equity_curve(run.id)
        self.assertEqual(len(curve), 2)
        self.assertAlmostEqual(curve[-1].equity, 1015600.0)


if __name__ == "__main__":
    unittest.main()
