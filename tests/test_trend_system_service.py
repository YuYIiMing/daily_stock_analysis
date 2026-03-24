# -*- coding: utf-8 -*-
"""Unit tests for the rebuilt trend-system service."""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from src.config import Config
from src.services.trend_system_service import TrendSystemService
from src.storage import DatabaseManager


class _FakeBoardFetcher:
    name = "FakeBoardFetcher"

    def __init__(self, stock_list, boards, caps):
        self._stock_list = stock_list
        self._boards = boards
        self._caps = caps

    def get_stock_list(self):
        return pd.DataFrame(self._stock_list)

    def get_belong_board(self, code: str):
        rows = self._boards.get(code, [])
        if not rows:
            return None
        return pd.DataFrame(rows)

    def get_base_info(self, code: str):
        cap = self._caps.get(code)
        if cap is None:
            return None
        return {"流通市值": cap}


class _FakeFetcherManager:
    def __init__(self, names, industries, boards, caps, stock_list):
        self._names = names
        self._industries = industries
        self._fetchers = [_FakeBoardFetcher(stock_list, boards, caps)]

    def get_stock_name(self, code: str) -> str:
        return self._names.get(code, code)

    def get_stock_sectors(self, code: str):
        value = self._industries.get(code)
        return [value] if value else []


class _FakeMarketService:
    def get_market_context(self, region: str = "cn", use_cache: bool = True):
        return {
            "top_sectors": [{"name": "AI算力"}],
            "bottom_sectors": [],
            "indices": [],
            "market_breadth": {},
            "regime": "bull",
            "strength_score": 80,
        }


def _make_history(days: int, base_price: float, last_pct: float, last_amount_mult: float = 1.0) -> pd.DataFrame:
    rows = []
    start_date = date.today() - timedelta(days=days - 1)
    price = base_price
    for idx in range(days):
        trade_date = start_date + timedelta(days=idx)
        pct = 0.6 if idx < days - 3 else 1.2
        if idx == days - 1:
            pct = last_pct
        close = round(price * (1 + pct / 100), 2)
        open_price = round(price * (1 + max(pct - 0.5, -1.0) / 100), 2)
        high = round(max(close, open_price) * (1.015 if idx == days - 1 else 1.01), 2)
        low = round(min(close, open_price) * 0.995, 2)
        amount = 1_000_000_000 + idx * 10_000_000
        if idx == days - 1:
            amount *= last_amount_mult
        rows.append(
            {
                "date": pd.Timestamp(trade_date),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": 10_000_000 + idx * 10_000,
                "amount": amount,
                "pct_chg": pct,
                "ma5": close * 0.98,
                "ma10": close * 0.96,
                "ma20": close * 0.93,
                "volume_ratio": 1.0,
            }
        )
        price = close
    return pd.DataFrame(rows)


class TrendSystemServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.env_path = Path(self.temp_dir.name) / ".env"
        self.db_path = Path(self.temp_dir.name) / "trend.db"
        self.env_path.write_text(
            "\n".join(
                [
                    "STOCK_LIST=159985,516020",
                    f"DATABASE_PATH={self.db_path}",
                    "TREND_SYSTEM_ENABLED=true",
                    "ADMIN_AUTH_ENABLED=false",
                ]
            ) + "\n",
            encoding="utf-8",
        )
        os.environ["ENV_FILE"] = str(self.env_path)
        os.environ["DATABASE_PATH"] = str(self.db_path)
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

        self.db = DatabaseManager.get_instance()
        self._seed_market_data()

        names = {
            "600001": "算力龙一",
            "600002": "算力龙二",
            "600003": "算力跟风",
            "600004": "医药股",
        }
        industries = {
            "600001": "软件服务",
            "600002": "软件服务",
            "600003": "软件服务",
            "600004": "医药商业",
        }
        boards = {
            "600001": [
                {"板块名称": "软件服务", "板块类型": "行业板块"},
                {"板块名称": "AI算力", "板块类型": "概念板块"},
            ],
            "600002": [
                {"板块名称": "软件服务", "板块类型": "行业板块"},
                {"板块名称": "AI算力", "板块类型": "概念板块"},
            ],
            "600003": [
                {"板块名称": "软件服务", "板块类型": "行业板块"},
                {"板块名称": "AI算力", "板块类型": "概念板块"},
            ],
            "600004": [
                {"板块名称": "医药商业", "板块类型": "行业板块"},
                {"板块名称": "创新药", "板块类型": "概念板块"},
            ],
        }
        caps = {
            "600001": "150亿",
            "600002": "120亿",
            "600003": "110亿",
            "600004": "80亿",
        }
        stock_list = [{"code": code, "name": name} for code, name in names.items()] + [
            {"code": "159985", "name": "豆粕ETF"}
        ]

        self.service = TrendSystemService(
            config=Config.get_instance(),
            db=self.db,
            fetcher_manager=_FakeFetcherManager(names, industries, boards, caps, stock_list),
            market_service=_FakeMarketService(),
        )

    def tearDown(self) -> None:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        os.environ.pop("ENV_FILE", None)
        os.environ.pop("DATABASE_PATH", None)
        self.temp_dir.cleanup()

    def _seed_market_data(self) -> None:
        self.db.save_daily_data(_make_history(260, 3000, 1.1), "sh000001", "test")
        self.db.save_daily_data(_make_history(80, 10, 10.0, 2.5), "600001", "test")
        self.db.save_daily_data(_make_history(80, 8, 6.5, 1.8), "600002", "test")
        self.db.save_daily_data(_make_history(80, 7.5, 5.8, 1.6), "600003", "test")
        self.db.save_daily_data(_make_history(80, 6, 2.0, 1.1), "600004", "test")

    def test_daily_snapshot_is_full_market_based(self) -> None:
        before = self.service.get_overview()
        self.assertEqual(before["snapshot_status"], "missing")

        self.service.recompute()
        overview = self.service.get_overview()
        diagnostics = self.service.get_diagnostics()
        sectors = self.service.get_sectors("concept")
        candidates = self.service.get_candidates()

        self.assertEqual(overview["position"]["recommended_position_pct"], 70)
        self.assertGreaterEqual(diagnostics["scanned_symbols"], 4)
        self.assertGreaterEqual(diagnostics["etf_excluded"], 1)
        self.assertTrue(any(item["sector_name"] == "AI算力" and item["trade_allowed"] for item in sectors))
        self.assertTrue(any(item["signal_type"] in {"breakout", "compensation"} for item in candidates))

    def test_positions_portfolio_and_risk_state(self) -> None:
        self.service.recompute()
        created = self.service.create_position(
            {
                "code": "600001",
                "name": "算力龙一",
                "sector_view": "concept",
                "sector_key": "concept:AI算力",
                "sector_name": "AI算力",
                "open_date": date.today() - timedelta(days=4),
                "open_type": "breakout",
                "entry_price": 10.0,
                "initial_stop_loss": 9.0,
                "position_pct": 10.0,
                "take_profit_stage": 0,
            }
        )
        self.assertEqual(created["code"], "600001")

        portfolio = self.service.get_portfolio()
        self.assertEqual(portfolio["summary"]["open_count"], 1)
        self.assertEqual(portfolio["items"][0]["code"], "600001")

        for idx in range(3):
            trade = self.service.create_trade(
                {
                    "code": "600001",
                    "name": "算力龙一",
                    "sector_view": "concept",
                    "sector_key": "concept:AI算力",
                    "sector_name": "AI算力",
                    "open_date": date.today() - timedelta(days=idx + 4),
                    "open_type": "breakout",
                    "entry_price": 10 + idx,
                    "position_pct": 10,
                }
            )
            self.service.update_trade(
                trade["id"],
                {
                    "close_date": date.today() - timedelta(days=idx + 3),
                    "exit_price": 9 + idx,
                    "exit_reason": "stop_loss",
                    "is_stop_loss": True,
                    "breakout_failed": True,
                },
            )

        risk_state = self.service.get_risk_state()
        self.assertEqual(risk_state["current_mode"], "cooldown")
        self.assertTrue(risk_state["flags"]["reduced_risk"])
        self.assertTrue(risk_state["flags"]["elite_disabled"])
        self.assertTrue(risk_state["flags"]["breakout_paused"])

    def test_candidate_allocation_respects_remaining_total_position_cap(self) -> None:
        self.service.create_position(
            {
                "code": "600004",
                "name": "医药股",
                "sector_view": "concept",
                "sector_key": "concept:创新药",
                "sector_name": "创新药",
                "open_date": date.today() - timedelta(days=2),
                "open_type": "pullback",
                "entry_price": 6.0,
                "initial_stop_loss": 5.6,
                "position_pct": 60.0,
                "take_profit_stage": 0,
            }
        )

        self.service.recompute()
        overview = self.service.get_overview()
        candidates = self.service.get_candidates()
        actionable = [item for item in candidates if item["actionable"]]

        remaining = max(float(overview["position"]["recommended_position_pct"]) - 60.0, 0.0)
        total_recommended = sum(float(item.get("recommended_position_pct") or 0.0) for item in actionable)

        self.assertLessEqual(total_recommended, remaining)
        self.assertLessEqual(sum(1 for item in actionable if item["recommended_position_pct"] > 0), 1)

    def test_breakout_paused_blocks_breakout_and_compensation_candidates(self) -> None:
        self.service.recompute()
        for idx in range(2):
            trade = self.service.create_trade(
                {
                    "code": "600001",
                    "name": "算力龙一",
                    "sector_view": "concept",
                    "sector_key": "concept:AI算力",
                    "sector_name": "AI算力",
                    "open_date": date.today() - timedelta(days=idx + 3),
                    "open_type": "breakout",
                    "entry_price": 10 + idx,
                    "position_pct": 10,
                }
            )
            self.service.update_trade(
                trade["id"],
                {
                    "close_date": date.today() - timedelta(days=idx + 2),
                    "exit_price": 9 + idx,
                    "exit_reason": "breakout_failed",
                    "is_stop_loss": True,
                    "breakout_failed": True,
                },
            )

        self.service.recompute()
        risk_state = self.service.get_risk_state()
        candidates = self.service.get_candidates()

        self.assertTrue(risk_state["flags"]["breakout_paused"])
        self.assertFalse(risk_state["flags"]["cooldown"])
        paused_items = [item for item in candidates if item["signal_type"] in {"breakout", "compensation"}]
        self.assertTrue(paused_items)
        self.assertTrue(all(not item["actionable"] for item in paused_items))
        self.assertTrue(all(item["action_block_reason"] == "breakout_paused" for item in paused_items))

    def test_emotion_exit_triggers_for_sector_positions_when_leader_breaks(self) -> None:
        leader_history = _make_history(80, 10, 6.0, 2.0)
        last_idx = len(leader_history) - 1
        leader_history.loc[last_idx, "high"] = float(leader_history.loc[last_idx, "close"]) * 1.08
        self.db.save_daily_data(leader_history, "600001", "test")

        self.service.create_position(
            {
                "code": "600003",
                "name": "算力跟风",
                "sector_view": "concept",
                "sector_key": "concept:AI算力",
                "sector_name": "AI算力",
                "open_date": date.today() - timedelta(days=2),
                "open_type": "pullback",
                "entry_price": 12.0,
                "initial_stop_loss": 10.0,
                "position_pct": 10.0,
                "take_profit_stage": 0,
            }
        )

        self.service.recompute()
        portfolio = self.service.get_portfolio()
        target = next(item for item in portfolio["items"] if item["code"] == "600003")

        self.assertTrue(target["signals"]["emotion_exit"])
        self.assertEqual(target["action"], "exit")

    def test_recompute_and_status(self) -> None:
        result = self.service.recompute()
        status = self.service.get_status()
        plan = self.service.get_plan()

        self.assertEqual(result["status"], "ready")
        self.assertEqual(status["daily_snapshot"]["status"], "ready")
        self.assertIn("discipline_notes", plan)

    def test_legacy_snapshot_is_rebuilt(self) -> None:
        self.db.save_trend_daily_snapshot(
            date.today(),
            {
                "overview": {
                    "as_of": date.today().isoformat(),
                    "position": {
                        "recommended_position_pct": 20,
                        "matched_rules": 0,
                        "rules": [],
                        "index_code": "sh000001",
                    },
                    "trade_allowed": False,
                    "primary_stage": "choppy",
                    "main_sectors": [],
                    "risk_state": {
                        "current_mode": "normal",
                        "flags": {},
                        "consecutive_stop_losses": 0,
                        "consecutive_non_stop_losses": 0,
                        "recent_breakout_failures": 0,
                        "reasons": ["normal"],
                    },
                    "candidate_count": 0,
                },
                "concept_sectors": [],
                "risk_state": {
                    "current_mode": "normal",
                    "flags": {},
                    "consecutive_stop_losses": 0,
                    "consecutive_non_stop_losses": 0,
                    "recent_breakout_failures": 0,
                    "reasons": ["normal"],
                },
            },
        )

        overview = self.service.get_overview()
        self.assertEqual(overview["snapshot_status"], "legacy")
        self.assertIn("auto_invalidate", self.service.get_status()["daily_snapshot"].get("source", ""))

        rebuilt = self.service.recompute()
        self.assertEqual(rebuilt["status"], "ready")
        overview_after = self.service.get_overview()
        self.assertIn("trade_gate", overview_after)
        self.assertIn("generated_at", overview_after)


if __name__ == "__main__":
    unittest.main()
