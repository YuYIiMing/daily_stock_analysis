# -*- coding: utf-8 -*-
"""Tests for quant backtest service summary consistency."""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.config import Config
from src.core.quant_backtest_engine import BacktestRunResult, EquityPoint, ExecutedTrade
from src.services.quant_backtest_service import QuantBacktestService, _resolve_stock_name
from src.storage import DatabaseManager, StockDirectory


class QuantBacktestServiceSummaryTestCase(unittest.TestCase):
    def test_run_backtest_uses_merged_trade_basis_for_summary(self) -> None:
        repo = MagicMock()
        repo.create_run.return_value = SimpleNamespace(id=7)

        feature_service = MagicMock()
        feature_service.get_market_regime.return_value = SimpleNamespace(regime="RiskOn", max_exposure_pct=70.0, score=3.0)
        feature_service.get_trade_candidates.return_value = []

        raw_summary = {
            "total_return_pct": 1.2,
            "final_equity": 1012000.0,
            "max_drawdown_pct": 2.1,
            "trade_count": 2.0,
            "win_count": 1.0,
            "loss_count": 1.0,
            "win_rate_pct": 50.0,
        }
        trades = [
            ExecutedTrade(
                code="600001",
                board_code="BK1",
                board_name="AI",
                entry_date=date(2026, 3, 20),
                exit_date=date(2026, 3, 21),
                entry_price=10.0,
                exit_price=10.5,
                shares=100,
                entry_module="BREAKOUT",
                stage="TREND",
                pnl_pct=5.0,
                pnl_amount=50.0,
                exit_reason="time_stop",
            ),
            ExecutedTrade(
                code="600001",
                board_code="BK1",
                board_name="AI",
                entry_date=date(2026, 3, 20),
                exit_date=date(2026, 3, 22),
                entry_price=10.0,
                exit_price=9.8,
                shares=100,
                entry_module="BREAKOUT",
                stage="TREND",
                pnl_pct=-2.0,
                pnl_amount=-20.0,
                exit_reason="hard_stop",
            ),
        ]
        engine = MagicMock()
        engine.run.return_value = BacktestRunResult(
            summary=raw_summary,
            trades=trades,
            equity_curve=[
                EquityPoint(
                    trade_date=date(2026, 3, 21),
                    cash=1000000.0,
                    market_value=0.0,
                    equity=1012000.0,
                    drawdown_pct=0.0,
                    exposure_pct=0.0,
                )
            ],
            trade_plans={},
        )

        service = QuantBacktestService(
            db_manager=MagicMock(),
            feature_service=feature_service,
            repository=repo,
            engine=engine,
        )
        service._get_trading_dates = MagicMock(return_value=[date(2026, 3, 20), date(2026, 3, 21)])
        service._load_price_bars = MagicMock(return_value={})

        result = service.run_backtest(start_date=date(2026, 3, 20), end_date=date(2026, 3, 21))

        merged_trades = repo.append_trades.call_args[0][1]
        self.assertEqual(len(merged_trades), 1)
        self.assertEqual(result["trade_count"], 1)
        self.assertEqual(result["summary"]["trade_count"], 1.0)
        self.assertEqual(result["summary"]["win_count"], 1.0)
        self.assertEqual(result["summary"]["loss_count"], 0.0)
        self.assertEqual(result["summary"]["win_rate_pct"], 100.0)

        status_summary = repo.update_run_status.call_args.kwargs["summary"]
        self.assertEqual(status_summary["trade_count"], 1.0)
        self.assertEqual(status_summary["win_count"], 1.0)
        self.assertEqual(status_summary["loss_count"], 0.0)
        self.assertEqual(status_summary["win_rate_pct"], 100.0)

    def test_trade_row_uses_stable_stock_name_fallback_and_amounts(self) -> None:
        trade_row = SimpleNamespace(
            id=1,
            code="002291",
            board_code="BK1",
            board_name="测试概念",
            entry_date=date(2025, 11, 27),
            exit_date=date(2025, 12, 2),
            entry_price=9.2746,
            exit_price=7.4663,
            shares=2600,
            entry_module="BREAKOUT",
            stage="TREND",
            status="closed",
            pnl_pct=-19.50,
            pnl_amount=-4701.76,
            exit_reason="time_stop",
            blocked_exit=False,
        )

        with patch("src.services.quant_backtest_service._resolve_stock_name", return_value=None):
            payload = QuantBacktestService._trade_row_to_dict(trade_row)

        self.assertEqual(payload["stock_name"], "股票002291")
        self.assertEqual(payload["entry_amount"], round(9.2746 * 2600, 2))
        self.assertEqual(payload["exit_amount"], round(7.4663 * 2600, 2))


class QuantBacktestStockDirectoryResolutionTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        os.environ["DATABASE_PATH"] = str(Path(self.temp_dir.name) / "quant_backtest_service.db")
        Config.reset_instance()
        DatabaseManager.reset_instance()
        self.db = DatabaseManager.get_instance()
        with self.db.session_scope() as session:
            session.add(
                StockDirectory(
                    code="002291",
                    name="星期六",
                    exchange="SZSE",
                    market="A-share",
                    is_main_board=True,
                    list_status="listed",
                    data_source="test",
                )
            )

    def tearDown(self) -> None:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        os.environ.pop("DATABASE_PATH", None)
        self.temp_dir.cleanup()

    def test_resolve_stock_name_prefers_local_stock_directory(self) -> None:
        trade_row = SimpleNamespace(
            id=1,
            code="002291",
            board_code="BK1",
            board_name="测试概念",
            entry_date=date(2025, 11, 27),
            exit_date=date(2025, 12, 2),
            entry_price=9.2746,
            exit_price=7.4663,
            shares=2600,
            entry_module="BREAKOUT",
            stage="TREND",
            status="closed",
            pnl_pct=-19.50,
            pnl_amount=-4701.76,
            exit_reason="time_stop",
            blocked_exit=False,
        )

        self.assertEqual(_resolve_stock_name("002291"), "星期六")

        payload = QuantBacktestService._trade_row_to_dict(trade_row)

        self.assertEqual(payload["stock_name"], "星期六")


if __name__ == "__main__":
    unittest.main()
