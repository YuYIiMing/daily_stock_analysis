# -*- coding: utf-8 -*-
"""API tests for quant strategy endpoints."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

try:
    import litellm  # noqa: F401
except ModuleNotFoundError:
    sys.modules["litellm"] = MagicMock()

from api.app import create_app
from src.config import Config
from src.storage import (
    ConceptBoardDailyFeature,
    DatabaseManager,
    IndexDailyFeature,
    StockDaily,
    StockDailyFeature,
)


class QuantApiTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temp_dir.name)
        os.environ["DATABASE_PATH"] = str(self.data_dir / "quant_api.db")
        os.environ["ADMIN_AUTH_ENABLED"] = "false"
        Config.reset_instance()
        DatabaseManager.reset_instance()
        self.db = DatabaseManager.get_instance()
        self.client = TestClient(create_app(static_dir=self.data_dir / "empty-static"), raise_server_exceptions=False)
        self._seed_data()

    def tearDown(self) -> None:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("ADMIN_AUTH_ENABLED", None)
        self.temp_dir.cleanup()

    def _seed_data(self) -> None:
        with self.db.session_scope() as session:
            for index_code in ("sh000001", "sz399001", "sz399006"):
                session.add(
                    IndexDailyFeature(
                        index_code=index_code,
                        trade_date=date(2026, 3, 20),
                        close=100.0,
                        ma5=101.0,
                        ma10=100.5,
                        ma20=99.0,
                        ma250=80.0,
                        up_day_count_10=7,
                        regime_score=3,
                        data_source="test",
                    )
                )

            session.add(
                ConceptBoardDailyFeature(
                    board_code="BK1",
                    board_name="AI概念",
                    trade_date=date(2026, 3, 20),
                    pct_chg=3.2,
                    amount=1e10,
                    turnover_rank_pct=0.05,
                    limit_up_count=2,
                    strong_stock_count=4,
                    breadth_ratio=0.1,
                    consistency_score=1,
                    theme_score=3,
                    leader_stock_code="600001",
                    leader_stock_name="测试龙头",
                    leader_2d_return=8.0,
                    leader_limit_up_3d=1,
                    stage="TREND",
                    raw_payload_json='{"member_count": 40, "change_3d_pct": 5.0, "up_days_3d": 2, "top5_avg_pct": 2.5, "big_drop_ratio": 0.02, "leader": {"ret20": 12.0, "amount_5d": 1000000000, "breakout_count_3d": 2, "close_above_ma10": true, "low_above_ma20": true, "pullback_volume_ratio": 0.8}}',
                )
            )

            session.add(
                StockDailyFeature(
                    code="600001",
                    trade_date=date(2026, 3, 20),
                    board_code="BK1",
                    board_name="AI概念",
                    close=10.0,
                    ma5=9.8,
                    ma10=9.6,
                    ma20=9.3,
                    ma60=8.0,
                    ret20=10.0,
                    ret60=22.0,
                    median_amount_20=5e8,
                    median_turnover_20=2.0,
                    above_ma60=True,
                    eligible_universe=True,
                    signal_score=85.0,
                    raw_payload_json='{"platform_width_pct": 8.0, "close_above_ma20_ratio": 0.9, "breakout_pct": 1.2, "amount_ratio_5": 1.8, "close_position_ratio": 0.8, "upper_shadow_pct": 2.0, "peer_confirm_count": 1}',
                )
            )

            session.add_all(
                [
                    StockDaily(code="600001", date=date(2026, 3, 20), open=9.9, high=10.1, low=9.8, close=10.0),
                    StockDaily(code="600001", date=date(2026, 3, 21), open=10.1, high=10.6, low=10.0, close=10.5),
                    StockDaily(code="600001", date=date(2026, 3, 24), open=10.6, high=11.0, low=10.5, close=10.9),
                ]
            )

    def test_trade_plan_endpoint(self) -> None:
        response = self.client.get("/api/v1/quant-strategy/trade-plan", params={"as_of_date": "2026-03-20"})
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["regime"], "RiskOn")
        self.assertGreaterEqual(len(data["items"]), 1)
        self.assertIn("diagnostics", data)
        self.assertGreaterEqual(data["diagnostics"]["eligible_stock_count"], 1)
        self.assertGreaterEqual(data["diagnostics"]["candidate_stock_count"], 1)

    def test_trade_plan_risk_off_message(self) -> None:
        response = self.client.get("/api/v1/quant-strategy/trade-plan", params={"as_of_date": "2026-03-19"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["regime"], "RiskOff")
        self.assertIn("市场防守无新开仓", payload.get("message") or "")
        self.assertIn("diagnostics", payload)

    def test_latest_backtest_detail_endpoint(self) -> None:
        run_response = self.client.post(
            "/api/v1/quant-strategy/backtests/run",
            json={"start_date": "2026-03-20", "end_date": "2026-03-24", "initial_capital": 100000.0},
        )
        self.assertEqual(run_response.status_code, 200)
        latest = self.client.get("/api/v1/quant-strategy/backtests/latest")
        self.assertEqual(latest.status_code, 200)
        payload = latest.json()
        self.assertEqual(payload["status"], "completed")
        self.assertEqual(payload["strategy_name"], "concept_trend_v1")

    def test_run_backtest_endpoint(self) -> None:
        response = self.client.post(
            "/api/v1/quant-strategy/backtests/run",
            json={"start_date": "2026-03-20", "end_date": "2026-03-24", "initial_capital": 100000.0},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("run_id", payload)
        self.assertEqual(payload["trade_count"], int(payload["summary"].get("trade_count", 0)))
        detail = self.client.get(f"/api/v1/quant-strategy/backtests/{payload['run_id']}")
        self.assertEqual(detail.status_code, 200)
        detail_payload = detail.json()
        self.assertEqual(
            int(detail_payload.get("summary", {}).get("trade_count", 0)),
            int(payload["summary"].get("trade_count", 0)),
        )
        trades = self.client.get(f"/api/v1/quant-strategy/backtests/{payload['run_id']}/trades")
        self.assertEqual(trades.status_code, 200)
        trade_items = trades.json().get("items", [])
        self.assertEqual(len(trade_items), int(payload["summary"].get("trade_count", 0)))
        if trade_items:
            first_trade = trade_items[0]
            self.assertIn("stock_name", first_trade)
            self.assertIn("entry_amount", first_trade)
            self.assertIn("exit_amount", first_trade)
            self.assertTrue(first_trade["stock_name"])

    def test_empty_trades_message(self) -> None:
        response = self.client.get("/api/v1/quant-strategy/backtests/999/trades")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["run_id"], 999)
        self.assertEqual(payload["items"], [])
        self.assertIn("无成交交易", payload.get("message") or "")

    def test_sync_endpoint_success(self) -> None:
        fake_summary = {"as_of_date": "2026-03-24", "errors": []}
        with patch(
            "api.v1.endpoints.quant_strategy.QuantDataService.refresh_quant_dataset",
            return_value=fake_summary,
        ) as refresh_mock:
            response = self.client.post(
                "/api/v1/quant-strategy/sync",
                json={"history_days": 130, "include_ranked_boards": False, "as_of_date": "2026-03-24"},
            )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "ok")
        self.assertIn("同步完成", payload["message"])
        refresh_mock.assert_called_once_with(
            as_of_date="2026-03-24",
            history_days=130,
            include_ranked_boards=False,
            latest_feature_only=False,
        )

    def test_sync_endpoint_latest_feature_only(self) -> None:
        fake_summary = {"as_of_date": "2026-03-24", "errors": []}
        with patch(
            "api.v1.endpoints.quant_strategy.QuantDataService.refresh_quant_dataset",
            return_value=fake_summary,
        ) as refresh_mock:
            response = self.client.post(
                "/api/v1/quant-strategy/sync",
                json={
                    "history_days": 130,
                    "include_ranked_boards": True,
                    "as_of_date": "2026-03-24",
                    "latest_feature_only": True,
                },
            )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "ok")
        refresh_mock.assert_called_once_with(
            as_of_date="2026-03-24",
            history_days=130,
            include_ranked_boards=True,
            latest_feature_only=True,
        )

    def test_sync_endpoint_partial(self) -> None:
        fake_summary = {"as_of_date": "2026-03-24", "errors": ["mock error"]}
        with patch(
            "api.v1.endpoints.quant_strategy.QuantDataService.refresh_quant_dataset",
            return_value=fake_summary,
        ) as refresh_mock:
            response = self.client.post(
                "/api/v1/quant-strategy/sync",
                json={"history_days": 130, "include_ranked_boards": True},
            )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "partial")
        self.assertIn("部分失败", payload["message"])
        refresh_mock.assert_called_once_with(
            as_of_date=None,
            history_days=130,
            include_ranked_boards=True,
            latest_feature_only=False,
        )

    def test_sync_status_endpoint_success(self) -> None:
        fake_summary = {
            "as_of_date": "2026-03-24",
            "stock_pool_size": 128,
            "stock_history_sync": {"records": 3200, "saved": 3200, "model_available": True, "errors": []},
            "index_sync": {"records": 780, "saved": 780, "model_available": True, "errors": []},
            "membership_sync": {"records": 256, "saved": 256, "model_available": True, "errors": []},
            "board_history_sync": {"records": 640, "saved": 640, "model_available": True, "errors": []},
            "board_feature_build": {"records": 120, "saved": 120, "model_available": True, "errors": []},
            "stock_feature_build": {"records": 960, "saved": 960, "model_available": True, "errors": []},
            "errors": [],
        }
        with patch(
            "api.v1.endpoints.quant_strategy.QuantDataService.get_sync_status_summary",
            return_value=fake_summary,
            create=True,
        ):
            response = self.client.get("/api/v1/quant-strategy/sync-status")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["as_of_date"], "2026-03-24")
        self.assertEqual(payload["stock_pool_size"], 128)
        self.assertTrue(payload["datasets"]["concept_membership"]["ready"])
        self.assertTrue(payload["datasets"]["stock_feature"]["ready"])
        self.assertEqual(payload["datasets"]["stock_feature"]["records"], 960)
        self.assertEqual(payload["errors"], [])

    def test_sync_status_endpoint_direct_summary_shape(self) -> None:
        fake_summary = {
            "as_of_date": "2026-03-24",
            "latest_stock_daily_date": "2026-03-24",
            "earliest_stock_daily_date": "2025-11-01",
            "stock_daily_distinct_codes": 54,
            "main_board_stock_pool_size": 3189,
            "membership_distinct_codes": 2802,
            "latest_membership_date": "2026-03-25",
            "latest_membership_count": 3379,
            "stock_feature_distinct_codes": 54,
            "latest_stock_feature_date": "2026-03-24",
            "latest_stock_feature_count": 51,
            "concept_board_distinct_count": 389,
            "latest_concept_board_date": "2026-03-25",
            "latest_concept_board_count": 292,
            "index_feature_latest_date": "2026-03-25",
            "index_feature_count": 3,
        }
        with patch(
            "api.v1.endpoints.quant_strategy.QuantDataService.get_sync_status_summary",
            return_value=fake_summary,
            create=True,
        ):
            response = self.client.get("/api/v1/quant-strategy/sync-status")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["concept_board_coverage_count"], 389)
        self.assertEqual(payload["latest_board_date"], "2026-03-25")
        self.assertEqual(payload["latest_board_count"], 292)
        self.assertEqual(payload["stock_pool_size"], 3189)
        self.assertEqual(payload["membership_distinct_codes"], 2802)
        self.assertTrue(payload["datasets"]["concept_membership"]["ready"])
        self.assertEqual(payload["latest_stock_feature_date"], "2026-03-24")
        self.assertEqual(payload["latest_stock_feature_count"], 51)
        self.assertEqual(payload["latest_index_feature_date"], "2026-03-25")
        self.assertEqual(payload["stock_daily_distinct_codes"], 54)

    def test_sync_status_endpoint_service_error_returns_empty(self) -> None:
        with patch(
            "api.v1.endpoints.quant_strategy.QuantDataService.get_sync_status_summary",
            side_effect=RuntimeError("database unavailable"),
            create=True,
        ):
            response = self.client.get("/api/v1/quant-strategy/sync-status")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "empty")
        self.assertEqual(payload["stock_pool_size"], 0)
        self.assertFalse(payload["datasets"]["stock_history"]["ready"])
        self.assertEqual(payload["errors"], [])


if __name__ == "__main__":
    unittest.main()
