# -*- coding: utf-8 -*-
"""Integration tests for rebuilt trend-system API endpoints."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from api.app import create_app
from api.v1.endpoints.trend_system import get_trend_system_service
from src.config import Config
from src.services.trend_system_service import TrendSystemService
from src.storage import DatabaseManager
from tests.test_trend_system_service import (
    _FakeFetcherManager,
    _FakeMarketService,
    _make_history,
)


class TrendSystemApiTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        env_path = Path(self.temp_dir.name) / ".env"
        db_path = Path(self.temp_dir.name) / "trend_api.db"
        env_path.write_text(
            "\n".join(
                [
                    "STOCK_LIST=159985,516020",
                    f"DATABASE_PATH={db_path}",
                    "TREND_SYSTEM_ENABLED=true",
                    "ADMIN_AUTH_ENABLED=false",
                ]
            ) + "\n",
            encoding="utf-8",
        )
        os.environ["ENV_FILE"] = str(env_path)
        os.environ["DATABASE_PATH"] = str(db_path)
        Config.reset_instance()
        DatabaseManager.reset_instance()
        TrendSystemService._daily_snapshot_cache = None
        TrendSystemService._snapshot_rebuilding = False
        TrendSystemService._last_recompute_started_at = None
        TrendSystemService._last_recompute_finished_at = None
        TrendSystemService._last_recompute_error = None
        TrendSystemService._symbol_map_cache = {"as_of": None, "data": {}}
        TrendSystemService._sector_membership_cache = {}
        TrendSystemService._float_cap_cache = {}

        db = DatabaseManager.get_instance()
        db.save_daily_data(_make_history(260, 3000, 1.1), "sh000001", "test")
        db.save_daily_data(_make_history(80, 10, 10.0, 2.5), "600001", "test")
        db.save_daily_data(_make_history(80, 8, 6.5, 1.8), "600002", "test")
        db.save_daily_data(_make_history(80, 7.5, 5.8, 1.6), "600003", "test")

        names = {"600001": "算力龙一", "600002": "算力龙二", "600003": "算力跟风"}
        industries = {code: "软件服务" for code in names}
        boards = {
            code: [
                {"板块名称": "软件服务", "板块类型": "行业板块"},
                {"板块名称": "AI算力", "板块类型": "概念板块"},
            ]
            for code in names
        }
        caps = {code: "150亿" for code in names}
        stock_list = [{"code": code, "name": name} for code, name in names.items()]

        self.service = TrendSystemService(
            config=Config.get_instance(),
            db=db,
            fetcher_manager=_FakeFetcherManager(names, industries, boards, caps, stock_list),
            market_service=_FakeMarketService(),
        )

        app = create_app(static_dir=Path(self.temp_dir.name) / "static")
        app.dependency_overrides[get_trend_system_service] = lambda: self.service
        self.client = TestClient(app)

    def tearDown(self) -> None:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        os.environ.pop("ENV_FILE", None)
        os.environ.pop("DATABASE_PATH", None)
        self.temp_dir.cleanup()

    def test_snapshot_and_market_endpoints(self) -> None:
        self.client.post("/api/v1/trend-system/recompute", json={"snapshot_type": "manual_recompute"})
        overview = self.client.get("/api/v1/trend-system/overview")
        sectors = self.client.get("/api/v1/trend-system/sectors", params={"view": "concept"})
        diagnostics = self.client.get("/api/v1/trend-system/diagnostics")
        status = self.client.get("/api/v1/trend-system/status")

        self.assertEqual(overview.status_code, 200)
        self.assertEqual(sectors.status_code, 200)
        self.assertEqual(diagnostics.status_code, 200)
        self.assertEqual(status.status_code, 200)
        self.assertTrue(overview.json()["trade_allowed"])

    def test_positions_and_recompute_endpoints(self) -> None:
        self.client.post("/api/v1/trend-system/recompute", json={"snapshot_type": "manual_recompute"})
        create_position = self.client.post(
            "/api/v1/trend-system/positions",
            json={
                "code": "600001",
                "name": "算力龙一",
                "sector_view": "concept",
                "sector_key": "concept:AI算力",
                "sector_name": "AI算力",
                "open_date": "2026-03-20",
                "open_type": "breakout",
                "entry_price": 12.3,
                "initial_stop_loss": 11.5,
                "position_pct": 10,
            },
        )
        self.assertEqual(create_position.status_code, 200)
        position_id = create_position.json()["id"]

        portfolio = self.client.get("/api/v1/trend-system/portfolio")
        self.assertEqual(portfolio.status_code, 200)
        self.assertEqual(portfolio.json()["summary"]["open_count"], 1)

        update_position = self.client.patch(
            f"/api/v1/trend-system/positions/{position_id}",
            json={"status": "closed", "close_date": "2026-03-21", "exit_reason": "manual_exit"},
        )
        self.assertEqual(update_position.status_code, 200)

        recompute = self.client.post("/api/v1/trend-system/recompute", json={"snapshot_type": "manual_recompute"})
        self.assertEqual(recompute.status_code, 200)
        self.assertEqual(recompute.json()["status"], "ready")

    def test_stage_override_and_alerts_endpoints(self) -> None:
        self.client.post("/api/v1/trend-system/recompute", json={"snapshot_type": "manual_recompute"})
        sectors = self.client.get("/api/v1/trend-system/sectors", params={"view": "concept"})
        sector = sectors.json()[0]

        override = self.client.post(
            "/api/v1/trend-system/stage-override",
            json={
                "sector_view": "concept",
                "sector_key": sector["sector_key"],
                "sector_name": sector["sector_name"],
                "original_stage": sector["quant_stage"],
                "target_stage": "choppy",
                "reason": "manual override",
            },
        )
        self.assertEqual(override.status_code, 200)

        alerts = self.client.get("/api/v1/trend-system/alerts")
        self.assertEqual(alerts.status_code, 200)


if __name__ == "__main__":
    unittest.main()
