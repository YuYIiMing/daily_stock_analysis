# -*- coding: utf-8 -*-
"""Tests for quant data snapshot sync service."""

import json
import inspect
import sys
import unittest
from collections import defaultdict
from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from src.services.quant_data_service import QuantDataService


class FakeFetcherManager:
    """In-memory fetcher manager used by unit tests."""

    def __init__(self):
        self.membership_calls = []

    def get_index_history(self, index_code, start_date=None, end_date=None, days=260):
        if index_code == "sh000001":
            return pd.DataFrame(
                {
                    "date": pd.to_datetime(["2026-01-02", "2026-01-03", "2026-01-06"]),
                    "close": [100.0, 101.0, 99.5],
                }
            )
        return None

    def get_concept_board_history(self, board_name, start_date=None, end_date=None, days=60):
        if board_name == "AI算力":
            return pd.DataFrame(
                {
                    "date": pd.to_datetime(["2026-01-02", "2026-01-03"]),
                    "pct_chg": [1.2, -0.8],
                    "amount": [1.1e10, 9.8e9],
                    "board_code": ["BK1234", "BK1234"],
                }
            )
        if board_name == "ERROR":
            raise RuntimeError("mock error")
        return None

    def get_stock_concept_boards(self, stock_code):
        self.membership_calls.append(stock_code)
        if stock_code == "600519":
            return [
                {"board_code": "BK0001", "board_name": "白酒概念", "is_primary": True},
                {"board_code": "BK0002", "board_name": "消费概念", "is_primary": False},
            ]
        if stock_code == "000001":
            return []
        return None

    def get_daily_data(self, stock_code, start_date=None, end_date=None, days=130):
        if stock_code == "600519":
            return (
                pd.DataFrame(
                    {
                        "date": pd.to_datetime(["2026-01-02", "2026-01-03", "2026-01-06"]),
                        "open": [10.0, 10.1, 10.2],
                        "high": [10.2, 10.3, 10.5],
                        "low": [9.9, 10.0, 10.1],
                        "close": [10.1, 10.2, 10.4],
                        "volume": [1000, 1200, 1400],
                        "amount": [1.01e7, 1.22e7, 1.46e7],
                        "pct_chg": [0.5, 1.0, 2.0],
                    }
                ),
                "FakeFetcher",
            )
        return pd.DataFrame(), "FakeFetcher"

    def get_concept_board_rankings(self, n=20):
        return ([{"name": "储能概念", "change_pct": 2.2}], [{"name": "半导体概念", "change_pct": -1.1}])

    def get_stock_list(self):
        return pd.DataFrame(
            {
                "code": ["600519", "300750", "000001"],
                "name": ["贵州茅台", "宁德时代", "平安银行"],
            }
        )


class FakeColumn:
    def __init__(self, model_name, field_name):
        self.model_name = model_name
        self.field_name = field_name

    def desc(self):
        return ("desc", self.field_name)

    def asc(self):
        return ("asc", self.field_name)

    def __eq__(self, other):
        return ("eq", self.field_name, other)

    def is_(self, other):
        return ("is", self.field_name, other)


def build_fake_model(model_name, fields):
    attrs = {field: FakeColumn(model_name, field) for field in fields}
    return type(model_name, (), attrs)


class FakeQuery:
    def __init__(self, session, target):
        self._session = session
        self._target = target
        self._distinct = False
        self._filters = []
        self._order_by = None

    def distinct(self):
        self._distinct = True
        return self

    def order_by(self, order_expr):
        self._order_by = order_expr
        return self

    def filter(self, *conditions):
        self._filters.extend(conditions)
        return self

    def all(self):
        return self._evaluate()

    def first(self):
        rows = self._evaluate()
        return rows[0] if rows else None

    def count(self):
        return len(self._evaluate())

    def _evaluate(self):
        model_name = self._target.model_name if isinstance(self._target, FakeColumn) else self._target.__name__
        rows = list(self._session.data.get(model_name, []))
        for condition in self._filters:
            if not isinstance(condition, tuple) or len(condition) != 3:
                continue
            operator, field_name, expected = condition
            if operator not in {"eq", "is"}:
                continue
            rows = [row for row in rows if row.get(field_name) == expected]

        if isinstance(self._target, FakeColumn):
            values = [(row.get(self._target.field_name),) for row in rows]
            if self._distinct:
                unique_values = []
                seen = set()
                for item in values:
                    if item in seen:
                        continue
                    seen.add(item)
                    unique_values.append(item)
                values = unique_values
            if isinstance(self._order_by, tuple) and len(self._order_by) == 2:
                direction, _ = self._order_by
                values = sorted(values, key=lambda item: (item[0] is None, item[0]), reverse=(direction == "desc"))
            return values

        if isinstance(self._order_by, tuple) and len(self._order_by) == 2:
            direction, field_name = self._order_by
            rows = sorted(rows, key=lambda row: (row.get(field_name) is None, row.get(field_name)), reverse=(direction == "desc"))
        return rows


class FakeSession:
    def __init__(self, data):
        self.data = data

    def query(self, target):
        return FakeQuery(self, target)


class FakeDBManager:
    def __init__(self, data):
        self._data = data

    @contextmanager
    def get_session(self):
        yield FakeSession(self._data)


class QuantDataServiceTestCase(unittest.TestCase):
    def test_list_stock_pool_from_directory_uses_local_master_first(self) -> None:
        fake_db = FakeDBManager(
            {
                "StockDirectory": [
                    {"code": "600519", "is_main_board": True},
                    {"code": "300750", "is_main_board": False},
                    {"code": "000001", "is_main_board": True},
                ]
            }
        )
        service = QuantDataService(db_manager=fake_db, fetcher_manager=FakeFetcherManager())
        service._model_cache["StockDirectory"] = build_fake_model("StockDirectory", ["code", "is_main_board"])

        pool = service._list_stock_pool_from_directory()

        self.assertEqual(pool, ["000001", "600519"])

    def test_sync_index_history_without_model_deferred(self) -> None:
        service = QuantDataService(db_manager=None, fetcher_manager=FakeFetcherManager())

        summary = service.sync_index_history(index_codes=["sh000001"], days=10)

        self.assertEqual(summary["requested"], 1)
        self.assertEqual(summary["fetched"], 1)
        self.assertGreater(summary["records"], 0)
        self.assertEqual(summary["saved"], 0)
        self.assertEqual(summary["deferred"], summary["records"])
        self.assertFalse(summary["model_available"])

    def test_sync_concept_board_history_with_empty_and_error(self) -> None:
        service = QuantDataService(db_manager=None, fetcher_manager=FakeFetcherManager())

        summary = service.sync_concept_board_history(board_names=["AI算力", "未知概念", "ERROR"], days=5)

        self.assertEqual(summary["requested"], 3)
        self.assertEqual(summary["fetched"], 1)
        self.assertEqual(summary["empty"], 1)
        self.assertEqual(len(summary["errors"]), 1)
        self.assertEqual(summary["saved"], 0)
        self.assertEqual(summary["deferred"], summary["records"])

    def test_sync_concept_board_history_reports_retry_and_failed_boards(self) -> None:
        class RetryBatchService(QuantDataService):
            def __init__(self):
                super().__init__(db_manager=object(), fetcher_manager=FakeFetcherManager())
                self._model_cache["ConceptBoardDailyFeature"] = object
                self.ths_attempts = defaultdict(int)
                self.session_scope_calls = 0

            @contextmanager
            def _session_scope(self, model):
                self.session_scope_calls += 1
                yield SimpleNamespace()

            def _fetch_concept_board_history_from_ths(self, board_name, start_date=None, end_date=None):
                self.ths_attempts[board_name] += 1
                if board_name == "重试概念" and self.ths_attempts[board_name] == 1:
                    raise RuntimeError("temporary fetch error")
                if board_name == "失败概念":
                    raise RuntimeError("persistent fetch error")
                return pd.DataFrame(
                    {
                        "date": pd.to_datetime(["2026-01-02"]),
                        "pct_chg": [1.2],
                        "amount": [1.1e10],
                    }
                )

            def _upsert_records(self, session, model, records, unique_keys):
                return len(records)

        service = RetryBatchService()
        params = inspect.signature(service.sync_concept_board_history).parameters
        if "commit_batch_size" not in params or "retry_attempts" not in params:
            self.skipTest("retry/batch commit parameters are not available yet")
        summary = service.sync_concept_board_history(
            board_names=["成功概念", "重试概念", "失败概念"],
            days=5,
            commit_batch_size=2,
            retry_attempts=2,
        )

        self.assertIn("retry_count", summary)
        self.assertIn("failed_boards", summary)
        self.assertIn("committed_batches", summary)
        self.assertIn("completed_boards", summary)
        self.assertGreaterEqual(summary["retry_count"], 1)
        self.assertGreaterEqual(summary["completed_boards"], 2)
        self.assertGreaterEqual(summary["committed_batches"], 1)
        self.assertIn("失败概念", json.dumps(summary["failed_boards"], ensure_ascii=False))
        self.assertGreaterEqual(service.session_scope_calls, summary["committed_batches"])

    def test_sync_concept_board_history_commits_in_batches(self) -> None:
        class BatchCommitService(QuantDataService):
            def __init__(self):
                super().__init__(db_manager=object(), fetcher_manager=FakeFetcherManager())
                self._model_cache["ConceptBoardDailyFeature"] = object
                self.session_scope_calls = 0

            @contextmanager
            def _session_scope(self, model):
                self.session_scope_calls += 1
                yield SimpleNamespace()

            def _fetch_concept_board_history_from_ths(self, board_name, start_date=None, end_date=None):
                return pd.DataFrame(
                    {
                        "date": pd.to_datetime(["2026-01-02"]),
                        "pct_chg": [1.0],
                        "amount": [1.0e10],
                    }
                )

            def _upsert_records(self, session, model, records, unique_keys):
                return len(records)

        service = BatchCommitService()
        params = inspect.signature(service.sync_concept_board_history).parameters
        if "commit_batch_size" not in params or "retry_attempts" not in params:
            self.skipTest("retry/batch commit parameters are not available yet")
        summary = service.sync_concept_board_history(
            board_names=["板块A", "板块B", "板块C", "板块D", "板块E"],
            days=5,
            commit_batch_size=2,
            retry_attempts=0,
        )

        self.assertEqual(summary["completed_boards"], 5)
        self.assertGreaterEqual(summary["committed_batches"], 3)
        self.assertGreaterEqual(service.session_scope_calls, 3)
        self.assertEqual(summary["retry_count"], 0)
        self.assertFalse(summary["failed_boards"])

    def test_sync_stock_concept_memberships_normalizes_codes(self) -> None:
        fake_fetcher = FakeFetcherManager()
        class MembershipFallbackService(QuantDataService):
            @staticmethod
            def _fetch_stock_concept_boards_from_efinance(stock_code):
                return None

            @staticmethod
            def _fetch_all_concept_board_catalog(*args, **kwargs):
                return []

            @staticmethod
            def _build_stock_concept_memberships_from_ths(*args, **kwargs):
                return [], {"fetched_boards": 0, "empty_boards": 0, "errors": []}

        service = MembershipFallbackService(db_manager=None, fetcher_manager=fake_fetcher)

        summary = service.sync_stock_concept_memberships(
            stock_codes=["SH600519", "SZ000001"],
            trade_date="2026-02-08",
        )

        self.assertEqual(summary["requested"], 2)
        self.assertEqual(summary["fetched"], 2)
        self.assertEqual(summary["records"], 2)
        self.assertEqual(summary["saved"], 0)
        self.assertEqual(summary["deferred"], 2)
        self.assertListEqual(fake_fetcher.membership_calls, ["600519", "000001"])

    def test_refresh_quant_dataset_requires_database_manager(self) -> None:
        service = QuantDataService(db_manager=None, fetcher_manager=FakeFetcherManager())
        summary = service.refresh_quant_dataset(history_days=130)
        self.assertTrue(summary["errors"])
        self.assertIn("Database manager is required", summary["errors"][0])

    def test_refresh_quant_dataset_prefers_fetcher_stock_list_by_default(self) -> None:
        class StockListFetcher:
            name = "StockListFetcher"

            @staticmethod
            def get_stock_list():
                return pd.DataFrame({"code": ["600519", "300750", "000001", "688981"]})

        class FetcherWithStockList(FakeFetcherManager):
            def __init__(self):
                super().__init__()
                self._fetchers = [StockListFetcher()]

        class RefreshStockPoolService(QuantDataService):
            def __init__(self):
                super().__init__(db_manager=object(), fetcher_manager=FetcherWithStockList())
                self.backfill_codes = []
                self.stock_daily_fallback_used = False

            def _resolve_latest_stock_date(self):
                return date(2026, 1, 6)

            def _resolve_earliest_stock_date(self):
                return date(2026, 1, 2)

            def _list_stock_pool(self):
                self.stock_daily_fallback_used = True
                return ["600519"]

            @staticmethod
            def _list_stock_pool_from_current_a_share_list():
                return []

            def backfill_stock_daily_history(self, *, stock_codes, end_date=None, days=130):
                self.backfill_codes = list(stock_codes)
                return {"effective_days": int(days)}

            @staticmethod
            def sync_index_history(*args, **kwargs):
                return {}

            @staticmethod
            def sync_stock_concept_memberships(*args, **kwargs):
                return {}

            @staticmethod
            def expand_membership_snapshot(*args, **kwargs):
                return {}

            @staticmethod
            def _fetch_all_concept_board_names(*args, **kwargs):
                return []

            @staticmethod
            def sync_concept_board_history(*args, **kwargs):
                return {}

            @staticmethod
            def build_quant_features(*args, **kwargs):
                return {"board_feature_build": {}, "stock_feature_build": {}, "errors": []}

        service = RefreshStockPoolService()

        summary = service.refresh_quant_dataset(history_days=130)

        self.assertEqual(summary["stock_pool_size"], 2)
        self.assertSetEqual(set(service.backfill_codes), {"600519", "000001"})
        self.assertFalse(service.stock_daily_fallback_used)

    def test_backfill_stock_daily_history_prefers_baostock_for_large_batches(self) -> None:
        class PreferredBaostockFetcher:
            name = "BaostockFetcher"

            def get_daily_data(self, stock_code, start_date=None, end_date=None, days=130):
                return pd.DataFrame(
                    {
                        "code": [stock_code],
                        "date": pd.to_datetime(["2026-01-02"]),
                        "open": [10.0],
                        "high": [10.2],
                        "low": [9.9],
                        "close": [10.1],
                        "volume": [1000],
                        "amount": [1.01e7],
                        "pct_chg": [0.5],
                    }
                )

        class LargeBatchFetcherManager(FakeFetcherManager):
            def __init__(self):
                super().__init__()
                self._fetchers = [PreferredBaostockFetcher()]
                self.manager_calls = 0

            def get_daily_data(self, stock_code, start_date=None, end_date=None, days=130):
                self.manager_calls += 1
                return super().get_daily_data(stock_code, start_date=start_date, end_date=end_date, days=days)

        class SaveDailyDb:
            @staticmethod
            def save_daily_data(df=None, code=None, data_source=None):
                return int(len(df) if df is not None else 0)

        fetcher_manager = LargeBatchFetcherManager()
        service = QuantDataService(db_manager=SaveDailyDb(), fetcher_manager=fetcher_manager)

        codes = [f"600{num:03d}" for num in range(50)]
        summary = service.backfill_stock_daily_history(stock_codes=codes, end_date="2026-01-06", days=130)

        self.assertEqual(summary["requested"], 50)
        self.assertEqual(summary["fetched"], 50)
        self.assertEqual(summary["saved"], 50)
        self.assertEqual(fetcher_manager.manager_calls, 0)

    def test_resolve_default_stock_pool_prefers_current_a_share_list(self) -> None:
        service = QuantDataService(db_manager=None, fetcher_manager=FakeFetcherManager())
        fake_akshare = SimpleNamespace(
            stock_info_a_code_name=lambda: pd.DataFrame({"code": ["600519", "300750", "000001", "688981"]})
        )
        with patch.dict(sys.modules, {"akshare": fake_akshare}):
            pool = service._resolve_default_stock_pool()
        self.assertListEqual(pool, ["000001", "600519"])

    def test_prioritize_concept_boards_filters_noise(self) -> None:
        service = QuantDataService(db_manager=None, fetcher_manager=FakeFetcherManager())
        boards = [
            {"board_code": "A", "board_name": "广东板块"},
            {"board_code": "B", "board_name": "汽车行业"},
            {"board_code": "C", "board_name": "储能概念"},
            {"board_code": "D", "board_name": "充电桩"},
        ]
        picked = service._prioritize_concept_boards(boards)
        names = [row["board_name"] for row in picked]
        self.assertIn("储能概念", names)
        self.assertNotIn("广东板块", names)
        self.assertNotIn("汽车行业", names)

    def test_prioritize_concept_boards_returns_empty_when_all_candidates_are_weak(self) -> None:
        service = QuantDataService(db_manager=None, fetcher_manager=FakeFetcherManager())
        boards = [
            {"board_code": "A", "board_name": "电子"},
            {"board_code": "B", "board_name": "基础化工"},
        ]

        picked = service._prioritize_concept_boards(boards)

        self.assertEqual(picked, [])

    def test_sync_stock_concept_memberships_filters_non_concept_universe_names(self) -> None:
        class MixedBoardFetcher(FakeFetcherManager):
            def get_stock_concept_boards(self, stock_code):
                self.membership_calls.append(stock_code)
                return [
                    {"board_code": "BK1201", "board_name": "电子", "is_primary": True},
                    {"board_code": "BK0989", "board_name": "储能概念", "is_primary": False},
                ]

        class UniverseAwareService(QuantDataService):
            def _fetch_all_concept_board_catalog(self, *, snapshot_date=None, include_ranked_boards=True, ranking_size=80):
                return [
                    {"board_name": "储能概念", "board_code": "BK0989"},
                    {"board_name": "算力概念", "board_code": "BK0003"},
                ]

            @staticmethod
            def _build_stock_concept_memberships_from_ths(*args, **kwargs):
                return [], {"fetched_boards": 0, "empty_boards": 0, "errors": []}

        fake_fetcher = MixedBoardFetcher()
        service = UniverseAwareService(db_manager=None, fetcher_manager=fake_fetcher)

        summary = service.sync_stock_concept_memberships(stock_codes=["SZ000001"], trade_date="2026-03-25")

        self.assertEqual(summary["requested"], 1)
        self.assertEqual(summary["records"], 1)
        self.assertEqual(summary["deferred"], 1)

    def test_sync_stock_concept_memberships_prefers_ths_board_snapshot(self) -> None:
        class ThsMembershipService(QuantDataService):
            def _fetch_all_concept_board_catalog(self, *, snapshot_date=None, include_ranked_boards=True, ranking_size=80):
                return [
                    {"board_name": "绿色电力", "board_code": "308956"},
                    {"board_name": "储能", "board_code": "308963"},
                ]

            def _build_stock_concept_memberships_from_ths(self, *, stock_codes, snapshot_date, board_catalog):
                return (
                    [
                        {
                            "code": "600905",
                            "trade_date": snapshot_date,
                            "board_name": "绿色电力",
                            "board_code": "308956",
                            "is_primary": True,
                        },
                        {
                            "code": "600905",
                            "trade_date": snapshot_date,
                            "board_name": "储能",
                            "board_code": "308963",
                            "is_primary": False,
                        },
                    ],
                    {"fetched_boards": 2, "empty_boards": 0, "errors": []},
                )

        service = ThsMembershipService(db_manager=None, fetcher_manager=FakeFetcherManager())

        summary = service.sync_stock_concept_memberships(stock_codes=["600905"], trade_date="2026-03-25")

        self.assertEqual(summary["records"], 2)
        self.assertEqual(summary["deferred"], 2)
        self.assertEqual(summary["fetched"], 2)

    def test_build_concept_board_records_generates_board_code(self) -> None:
        service = QuantDataService(db_manager=None, fetcher_manager=FakeFetcherManager())
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-01-02", "2026-01-03"]),
                "pct_chg": [1.2, 1.3],
                "amount": [2.0e10, 2.1e10],
            }
        )
        rows = service._build_concept_board_records("储能概念", df)
        self.assertEqual(len(rows), 2)
        self.assertTrue(rows[0]["board_code"].startswith("EM_"))
        self.assertEqual(rows[0]["board_name"], "储能概念")

    def test_build_concept_board_records_supports_ths_history_shape(self) -> None:
        service = QuantDataService(db_manager=None, fetcher_manager=FakeFetcherManager())
        df = pd.DataFrame(
            {
                "日期": pd.to_datetime(["2026-01-02", "2026-01-03"]),
                "开盘价": [100.0, 101.0],
                "最高价": [101.0, 102.0],
                "最低价": [99.0, 100.5],
                "收盘价": [100.0, 103.0],
                "成交量": [10000, 12000],
                "成交额": [2.0e9, 2.5e9],
            }
        )

        rows = service._build_concept_board_records("AI PC", df)

        self.assertEqual(len(rows), 2)
        self.assertAlmostEqual(float(rows[0]["pct_chg"]), 0.0, places=6)
        self.assertAlmostEqual(float(rows[1]["pct_chg"]), 3.0, places=6)

    def test_build_board_frame_from_history_uses_market_history_metrics(self) -> None:
        service = QuantDataService(db_manager=None, fetcher_manager=FakeFetcherManager())
        history = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(
                    [
                        "2026-01-02",
                        "2026-01-02",
                        "2026-01-03",
                        "2026-01-03",
                        "2026-01-06",
                        "2026-01-06",
                    ]
                ),
                "board_code": ["BK_A", "BK_B", "BK_A", "BK_B", "BK_A", "BK_B"],
                "board_name": ["新能源概念", "储能概念", "新能源概念", "储能概念", "新能源概念", "储能概念"],
                "pct_chg": [1.0, 2.0, -0.5, 1.5, 3.0, -1.0],
                "amount": [100.0, 300.0, 120.0, 260.0, 400.0, 90.0],
            }
        )
        supplement = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2026-01-06"]),
                "board_code": ["BK_A"],
                "board_name": ["新能源概念"],
                "member_count": [5],
                "strong_stock_count": [2],
                "limit_up_count": [1],
                "limit_down_count": [0],
                "top5_avg_pct": [2.2],
                "big_drop_ratio": [0.0],
                "member_fall20_ratio": [0.0],
                "breadth_ratio": [0.4],
                "prev_limit_up_count": [0],
                "leader_stock_code": ["600001"],
                "leader_stock_name": ["600001"],
                "leader_2d_return": [8.0],
                "leader_limit_up_3d": [1],
                "leader_payload": [{"ret20": 12.0}],
            }
        )

        frame = service._build_board_frame_from_history(history, supplement)

        self.assertEqual(len(frame), 6)
        # 2026-01-02 on amount: BK_B(300) should rank better than BK_A(100)
        day = frame[frame["trade_date"] == pd.Timestamp("2026-01-02")]
        rank_a = float(day[day["board_code"] == "BK_A"]["turnover_rank_pct"].iloc[0])
        rank_b = float(day[day["board_code"] == "BK_B"]["turnover_rank_pct"].iloc[0])
        self.assertLess(rank_b, rank_a)
        # change_3d_pct for BK_A should come from board history: 1.0 + (-0.5) + 3.0 = 3.5
        bk_a_last = frame[(frame["board_code"] == "BK_A") & (frame["trade_date"] == pd.Timestamp("2026-01-06"))]
        self.assertAlmostEqual(float(bk_a_last["change_3d_pct"].iloc[0]), 3.5, places=6)

    def test_build_board_frame_from_history_works_with_empty_stock_supplement(self) -> None:
        service = QuantDataService(db_manager=None, fetcher_manager=FakeFetcherManager())
        history = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2026-01-02", "2026-01-02", "2026-01-03", "2026-01-03"]),
                "board_code": ["BK_A", "BK_B", "BK_A", "BK_B"],
                "board_name": ["AI概念", "机器人概念", "AI概念", "机器人概念"],
                "pct_chg": [1.2, -0.6, 0.8, 1.0],
                "amount": [200.0, 180.0, 210.0, 190.0],
            }
        )
        frame = service._build_board_frame_from_history(history, pd.DataFrame())
        self.assertEqual(len(frame), 4)
        self.assertSetEqual(set(frame["board_code"].tolist()), {"BK_A", "BK_B"})
        self.assertTrue((frame["strong_stock_count"] == 0).all())
        self.assertTrue((frame["member_count"] == 0).all())

    def test_build_board_frame_from_history_falls_back_to_same_day_board_name(self) -> None:
        service = QuantDataService(db_manager=None, fetcher_manager=FakeFetcherManager())
        history = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2026-01-03"]),
                "board_code": ["EM_5G"],
                "board_name": ["5G概念"],
                "pct_chg": [2.0],
                "amount": [300.0],
            }
        )
        supplement = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2026-01-03"]),
                "board_code": ["BK0714"],
                "board_name": ["5G概念"],
                "member_count": [105],
                "strong_stock_count": [15],
                "limit_up_count": [6],
                "limit_down_count": [0],
                "top5_avg_pct": [6.5],
                "big_drop_ratio": [0.02],
                "member_fall20_ratio": [0.10],
                "breadth_ratio": [15 / 105],
                "prev_limit_up_count": [3],
                "leader_stock_code": ["000001"],
                "leader_stock_name": ["000001"],
                "leader_2d_return": [12.0],
                "leader_limit_up_3d": [2],
                "leader_payload": [{"ret20": 18.0, "breakout_count_3d": 1}],
            }
        )

        frame = service._build_board_frame_from_history(history, supplement)

        self.assertEqual(int(frame.iloc[0]["member_count"]), 105)
        self.assertEqual(int(frame.iloc[0]["strong_stock_count"]), 15)
        self.assertEqual(int(frame.iloc[0]["limit_up_count"]), 6)
        self.assertEqual(str(frame.iloc[0]["leader_stock_code"]), "000001")

    def test_build_board_frame_from_history_falls_back_to_alias_board_name(self) -> None:
        service = QuantDataService(db_manager=None, fetcher_manager=FakeFetcherManager())
        history = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2026-01-03"]),
                "board_code": ["EM_STORAGE"],
                "board_name": ["储能"],
                "pct_chg": [1.6],
                "amount": [280.0],
            }
        )
        supplement = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2026-01-03"]),
                "board_code": ["BK0989"],
                "board_name": ["储能概念"],
                "member_count": [133],
                "strong_stock_count": [11],
                "limit_up_count": [5],
                "limit_down_count": [0],
                "top5_avg_pct": [5.2],
                "big_drop_ratio": [0.03],
                "member_fall20_ratio": [0.08],
                "breadth_ratio": [11 / 133],
                "prev_limit_up_count": [2],
                "leader_stock_code": ["002192"],
                "leader_stock_name": ["002192"],
                "leader_2d_return": [8.0],
                "leader_limit_up_3d": [1],
                "leader_payload": [{"ret20": 22.0, "breakout_count_3d": 1}],
            }
        )

        frame = service._build_board_frame_from_history(history, supplement)

        self.assertEqual(int(frame.iloc[0]["member_count"]), 133)
        self.assertEqual(int(frame.iloc[0]["strong_stock_count"]), 11)
        self.assertEqual(int(frame.iloc[0]["limit_up_count"]), 5)
        self.assertEqual(str(frame.iloc[0]["leader_stock_code"]), "002192")

    def test_build_board_feature_records_tolerates_nan_numeric_fields(self) -> None:
        service = QuantDataService(db_manager=None, fetcher_manager=FakeFetcherManager())
        board_frame = pd.DataFrame(
            {
                "board_code": ["BK_A"],
                "board_name": ["AI概念"],
                "trade_date": pd.to_datetime(["2026-01-03"]),
                "pct_chg": [float("nan")],
                "amount": [float("nan")],
                "turnover_rank_pct": [float("nan")],
                "limit_up_count": [float("nan")],
                "strong_stock_count": [float("nan")],
                "breadth_ratio": [float("nan")],
                "consistency_score": [float("nan")],
                "theme_score": [float("nan")],
                "leader_stock_code": [None],
                "leader_stock_name": [None],
                "leader_2d_return": [float("nan")],
                "leader_limit_up_3d": [float("nan")],
                "stage": ["IGNORE"],
                "raw_payload_json": ["{}"],
            }
        )

        rows = service._build_board_feature_records(board_frame)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["leader_limit_up_3d"], 0)
        self.assertEqual(rows[0]["theme_score"], 0)
        self.assertEqual(rows[0]["limit_up_count"], 0)

    def test_to_board_history_frame_skips_unclassified_placeholder(self) -> None:
        service = QuantDataService(db_manager=None, fetcher_manager=FakeFetcherManager())
        rows = [
            SimpleNamespace(board_code="BK_A", board_name="AI概念", trade_date="2026-01-03", pct_chg=1.0, amount=100.0),
            SimpleNamespace(board_code="EM_X", board_name="未归类概念", trade_date="2026-01-03", pct_chg=2.0, amount=50.0),
        ]

        frame = service._to_board_history_frame(rows)

        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["board_name"], "AI概念")

    def test_refresh_quant_dataset_forwards_feature_build_options(self) -> None:
        class RefreshFeatureOptionsService(QuantDataService):
            def __init__(self):
                super().__init__(db_manager=object(), fetcher_manager=FakeFetcherManager())
                self.build_kwargs = {}

            @staticmethod
            def _resolve_default_stock_pool():
                return ["000001", "600519"]

            @staticmethod
            def _resolve_latest_stock_date():
                return date(2026, 1, 6)

            @staticmethod
            def _resolve_earliest_stock_date():
                return date(2026, 1, 2)

            @staticmethod
            def backfill_stock_daily_history(*args, **kwargs):
                return {"effective_days": 130}

            @staticmethod
            def sync_index_history(*args, **kwargs):
                return {}

            @staticmethod
            def sync_stock_concept_memberships(*args, **kwargs):
                return {}

            @staticmethod
            def expand_membership_snapshot(*args, **kwargs):
                return {}

            @staticmethod
            def _fetch_all_concept_board_names(*args, **kwargs):
                return []

            @staticmethod
            def sync_concept_board_history(*args, **kwargs):
                return {}

            def build_quant_features(self, *args, **kwargs):
                self.build_kwargs = dict(kwargs)
                return {"board_feature_build": {}, "stock_feature_build": {}, "errors": []}

        service = RefreshFeatureOptionsService()

        summary = service.refresh_quant_dataset(
            history_days=130,
            feature_batch_size=64,
            latest_feature_only=True,
        )

        self.assertEqual(summary["stock_pool_size"], 2)
        self.assertEqual(service.build_kwargs["stock_batch_size"], 64)
        self.assertTrue(service.build_kwargs["latest_only"])

    def test_refresh_quant_dataset_uses_lightweight_windows_for_latest_only(self) -> None:
        class LightweightLatestOnlyService(QuantDataService):
            def __init__(self):
                super().__init__(db_manager=object(), fetcher_manager=FakeFetcherManager())
                self.backfill_days = None
                self.membership_expand_called = False
                self.board_sync_kwargs = {}

            @staticmethod
            def _resolve_default_stock_pool():
                return ["000001", "600519"]

            @staticmethod
            def _resolve_latest_stock_date():
                return date(2026, 3, 24)

            @staticmethod
            def sync_stock_directory():
                return {}

            def backfill_stock_daily_history(self, *args, **kwargs):
                self.backfill_days = kwargs.get("days")
                return {"effective_days": kwargs.get("days", 0)}

            @staticmethod
            def sync_index_history(*args, **kwargs):
                return {}

            @staticmethod
            def sync_stock_concept_memberships(*args, **kwargs):
                return {}

            def expand_membership_snapshot(self, *args, **kwargs):
                self.membership_expand_called = True
                return {}

            @staticmethod
            def _fetch_all_concept_board_names(*args, **kwargs):
                return ["储能概念"]

            def sync_concept_board_history(self, *args, **kwargs):
                self.board_sync_kwargs = dict(kwargs)
                return {}

            @staticmethod
            def build_quant_features(*args, **kwargs):
                return {"board_feature_build": {}, "stock_feature_build": {}, "errors": []}

        service = LightweightLatestOnlyService()

        service.refresh_quant_dataset(
            as_of_date="2026-03-25",
            history_days=130,
            latest_feature_only=True,
        )

        self.assertEqual(service.backfill_days, 10)
        self.assertFalse(service.membership_expand_called)
        self.assertEqual(service.board_sync_kwargs["start_date"], "2026-03-24")
        self.assertEqual(service.board_sync_kwargs["days"], 10)

    def test_refresh_quant_dataset_skips_stock_backfill_when_latest_day_exists(self) -> None:
        class SkipLatestBackfillService(QuantDataService):
            def __init__(self):
                super().__init__(db_manager=object(), fetcher_manager=FakeFetcherManager())
                self.backfill_called = False

            @staticmethod
            def sync_stock_directory():
                return {}

            @staticmethod
            def _resolve_latest_stock_date():
                return date(2026, 3, 25)

            @staticmethod
            def _list_stock_pool():
                return ["000001", "600519"]

            def backfill_stock_daily_history(self, *args, **kwargs):
                self.backfill_called = True
                return {}

            @staticmethod
            def sync_index_history(*args, **kwargs):
                return {}

            @staticmethod
            def sync_stock_concept_memberships(*args, **kwargs):
                return {}

            @staticmethod
            def expand_membership_snapshot(*args, **kwargs):
                return {}

            @staticmethod
            def _fetch_all_concept_board_names(*args, **kwargs):
                return []

            @staticmethod
            def sync_concept_board_history(*args, **kwargs):
                return {}

            @staticmethod
            def build_quant_features(*args, **kwargs):
                return {"board_feature_build": {}, "stock_feature_build": {}, "errors": []}

        service = SkipLatestBackfillService()

        summary = service.refresh_quant_dataset(
            as_of_date="2026-03-25",
            history_days=130,
            latest_feature_only=True,
        )

        self.assertFalse(service.backfill_called)
        self.assertTrue(summary["stock_history_sync"]["skipped"])
        self.assertEqual(summary["stock_history_sync"]["skip_reason"], "latest_stock_daily_already_present")

    def test_refresh_quant_dataset_skips_external_refresh_when_latest_day_is_ready(self) -> None:
        class SkipLatestExternalRefreshService(QuantDataService):
            def __init__(self):
                super().__init__(db_manager=object(), fetcher_manager=FakeFetcherManager())
                self.stock_directory_called = False
                self.index_sync_called = False
                self.membership_sync_called = False
                self.membership_expand_called = False
                self.board_sync_called = False
                self.backfill_called = False

            @staticmethod
            def _resolve_latest_stock_date():
                return date(2026, 3, 25)

            @staticmethod
            def _list_stock_pool():
                return ["000001", "600519"]

            def _query_model_edge_date(self, *, model_name, date_field, latest=True):
                return date(2026, 3, 25)

            def sync_stock_directory(self):
                self.stock_directory_called = True
                return {}

            def backfill_stock_daily_history(self, *args, **kwargs):
                self.backfill_called = True
                return {}

            def sync_index_history(self, *args, **kwargs):
                self.index_sync_called = True
                return {}

            def sync_stock_concept_memberships(self, *args, **kwargs):
                self.membership_sync_called = True
                return {}

            def expand_membership_snapshot(self, *args, **kwargs):
                self.membership_expand_called = True
                return {}

            def sync_concept_board_history(self, *args, **kwargs):
                self.board_sync_called = True
                return {}

            @staticmethod
            def build_quant_features(*args, **kwargs):
                return {"board_feature_build": {}, "stock_feature_build": {}, "errors": []}

        service = SkipLatestExternalRefreshService()

        summary = service.refresh_quant_dataset(
            as_of_date="2026-03-25",
            history_days=130,
            latest_feature_only=True,
        )

        self.assertFalse(service.stock_directory_called)
        self.assertFalse(service.backfill_called)
        self.assertFalse(service.index_sync_called)
        self.assertFalse(service.membership_sync_called)
        self.assertFalse(service.membership_expand_called)
        self.assertFalse(service.board_sync_called)
        self.assertTrue(summary["stock_directory_sync"]["skipped"])
        self.assertTrue(summary["index_sync"]["skipped"])
        self.assertTrue(summary["membership_sync"]["skipped"])
        self.assertTrue(summary["membership_expand"]["skipped"])
        self.assertTrue(summary["board_history_sync"]["skipped"])

    def test_resolve_feature_build_start_date_uses_bounded_lookback(self) -> None:
        service = QuantDataService(db_manager=None, fetcher_manager=FakeFetcherManager())

        self.assertEqual(
            service._resolve_feature_build_start_date(as_of_date=date(2026, 3, 25), history_days=10),
            date(2025, 12, 25),
        )
        self.assertEqual(
            service._resolve_feature_build_start_date(as_of_date=date(2026, 3, 25), history_days=130),
            date(2025, 11, 15),
        )

    def test_build_quant_features_batches_stock_processing(self) -> None:
        class BatchBuildService(QuantDataService):
            def __init__(self):
                super().__init__(db_manager=object(), fetcher_manager=FakeFetcherManager())
                dummy_model = type("DummyModel", (), {})
                self._models = {
                    "StockDaily": dummy_model,
                    "StockConceptMembershipDaily": dummy_model,
                    "ConceptBoardDailyFeature": dummy_model,
                    "StockDailyFeature": dummy_model,
                }
                self.loaded_batches = []

            def _get_model(self, model_name):
                return self._models.get(model_name)

            def _load_board_rows_for_feature_build(self, *, start_date, end_date):
                return [
                    SimpleNamespace(board_code="BK_A", board_name="AI概念", trade_date=date(2026, 1, 2), pct_chg=1.0, amount=100.0),
                    SimpleNamespace(board_code="BK_A", board_name="AI概念", trade_date=date(2026, 1, 3), pct_chg=1.5, amount=110.0),
                    SimpleNamespace(board_code="BK_A", board_name="AI概念", trade_date=date(2026, 1, 6), pct_chg=0.8, amount=120.0),
                    SimpleNamespace(board_code="BK_B", board_name="机器人概念", trade_date=date(2026, 1, 2), pct_chg=0.5, amount=90.0),
                    SimpleNamespace(board_code="BK_B", board_name="机器人概念", trade_date=date(2026, 1, 3), pct_chg=-0.3, amount=95.0),
                    SimpleNamespace(board_code="BK_B", board_name="机器人概念", trade_date=date(2026, 1, 6), pct_chg=1.2, amount=130.0),
                ]

            def _load_membership_rows_for_feature_build(self, *, trade_date):
                return [
                    SimpleNamespace(code="000001", board_name="AI概念", board_code="BK_A", is_primary=True),
                    SimpleNamespace(code="000002", board_name="AI概念", board_code="BK_A", is_primary=True),
                    SimpleNamespace(code="600001", board_name="机器人概念", board_code="BK_B", is_primary=True),
                ]

            @staticmethod
            def _list_stock_codes_for_feature_build(*, start_date, end_date):
                return ["000001", "000002", "600001"]

            def _load_stock_rows_for_feature_build(self, *, start_date, end_date, stock_codes):
                self.loaded_batches.append(list(stock_codes))
                rows = []
                price_seed = {"000001": 10.0, "000002": 20.0, "600001": 30.0}
                for code in stock_codes:
                    base = price_seed[code]
                    for idx, trade_date in enumerate([date(2026, 1, 2), date(2026, 1, 3), date(2026, 1, 6)]):
                        close = base + idx * 0.3
                        rows.append(
                            SimpleNamespace(
                                code=code,
                                date=trade_date,
                                open=close - 0.1,
                                high=close + 0.2,
                                low=close - 0.2,
                                close=close,
                                volume=1000 + idx * 50,
                                amount=(close * (1000 + idx * 50)),
                                pct_chg=1.0 + idx * 0.1,
                            )
                        )
                return rows

            @contextmanager
            def _session_scope(self, model):
                yield SimpleNamespace()

            @staticmethod
            def _upsert_records(session, model, records, unique_keys):
                return len(records)

        service = BatchBuildService()

        summary = service.build_quant_features(
            as_of_date=date(2026, 1, 6),
            history_days=10,
            stock_batch_size=2,
            latest_only=True,
        )

        self.assertEqual(service.loaded_batches[:2], [["000001", "000002"], ["600001"]])
        self.assertEqual(len(service.loaded_batches), 2)
        self.assertEqual(summary["board_feature_build"]["batches"], 2)
        self.assertEqual(summary["stock_feature_build"]["batches"], 2)
        self.assertEqual(summary["board_feature_build"]["dates"], 1)
        self.assertGreater(summary["board_feature_build"]["saved"], 0)
        self.assertGreater(summary["stock_feature_build"]["saved"], 0)

    def test_build_quant_features_builds_window_in_single_pass(self) -> None:
        class WindowBuildService(QuantDataService):
            def __init__(self):
                super().__init__(db_manager=object(), fetcher_manager=FakeFetcherManager())
                dummy_model = type("DummyModel", (), {})
                self._models = {
                    "StockDaily": dummy_model,
                    "StockConceptMembershipDaily": dummy_model,
                    "ConceptBoardDailyFeature": dummy_model,
                    "StockDailyFeature": dummy_model,
                }
                self.loaded_batches = []
                self.window_membership_requested = False

            def _get_model(self, model_name):
                return self._models.get(model_name)

            @staticmethod
            def _list_feature_trade_dates(*, start_date, end_date):
                raise AssertionError("single-pass build should not query trade dates for recursive daily rebuild")

            def _load_board_rows_for_feature_build(self, *, start_date, end_date):
                return [
                    SimpleNamespace(board_code="BK_A", board_name="AI概念", trade_date=date(2026, 1, 2), pct_chg=1.0, amount=100.0),
                    SimpleNamespace(board_code="BK_A", board_name="AI概念", trade_date=date(2026, 1, 3), pct_chg=1.5, amount=110.0),
                    SimpleNamespace(board_code="BK_A", board_name="AI概念", trade_date=date(2026, 1, 6), pct_chg=0.8, amount=120.0),
                ]

            def _load_membership_rows_for_feature_build(self, *, trade_date):
                return [SimpleNamespace(code="000001", board_name="AI概念", board_code="BK_A", is_primary=True)]

            def _load_membership_rows_for_feature_window(self, *, start_date, end_date):
                self.window_membership_requested = True
                return [
                    SimpleNamespace(
                        code="000001",
                        trade_date=date(2026, 1, 3),
                        board_name="AI概念",
                        board_code="BK_A",
                        is_primary=True,
                    )
                ]

            @staticmethod
            def _list_stock_codes_for_feature_build(*, start_date, end_date):
                return ["000001"]

            def _load_stock_rows_for_feature_build(self, *, start_date, end_date, stock_codes):
                self.loaded_batches.append(list(stock_codes))
                rows = []
                for idx, trade_date in enumerate([date(2026, 1, 2), date(2026, 1, 3), date(2026, 1, 6)]):
                    close = 10.0 + idx * 0.3
                    rows.append(
                        SimpleNamespace(
                            code="000001",
                            date=trade_date,
                            open=close - 0.1,
                            high=close + 0.2,
                            low=close - 0.2,
                            close=close,
                            volume=1000 + idx * 50,
                            amount=(close * (1000 + idx * 50)),
                            pct_chg=1.0 + idx * 0.1,
                        )
                    )
                return rows

            @contextmanager
            def _session_scope(self, model):
                yield SimpleNamespace()

            @staticmethod
            def _upsert_records(session, model, records, unique_keys):
                return len(records)

        service = WindowBuildService()

        summary = service.build_quant_features(
            as_of_date=date(2026, 1, 6),
            history_days=3,
            stock_batch_size=50,
            latest_only=False,
        )

        self.assertTrue(service.window_membership_requested)
        self.assertEqual(service.loaded_batches, [["000001"]])
        self.assertEqual(summary["board_feature_build"]["records"], 2)
        self.assertEqual(summary["board_feature_build"]["saved"], 2)
        self.assertEqual(summary["board_feature_build"]["dates"], 2)
        self.assertEqual(summary["stock_feature_build"]["records"], 2)
        self.assertEqual(summary["stock_feature_build"]["saved"], 2)
        self.assertEqual(summary["stock_feature_build"]["dates"], 2)

    def test_build_stock_feature_records_marks_unclassified_board_as_ineligible(self) -> None:
        service = QuantDataService(db_manager=None, fetcher_manager=FakeFetcherManager())
        enriched = pd.DataFrame(
            {
                "code": ["000001"],
                "trade_date": pd.to_datetime(["2026-01-06"]),
                "board_code": ["EM_UNKNOWN"],
                "board_name": ["未归类概念"],
                "close": [10.0],
                "open": [9.9],
                "high": [10.2],
                "low": [9.8],
                "ma5": [9.8],
                "ma10": [9.7],
                "ma20": [9.5],
                "ma60": [9.0],
                "ret20": [12.0],
                "ret60": [15.0],
                "median_amount_20": [3.2e8],
                "median_turnover_20": [1.5],
                "listed_days": [300],
                "above_ma60": [True],
                "close_above_ma20_ratio": [0.9],
                "platform_width_pct": [8.0],
                "breakout_pct": [2.0],
                "amount_ratio_5": [1.4],
                "close_position_ratio": [0.8],
                "upper_shadow_pct": [1.0],
                "pullback_pct_5d": [-3.0],
                "pullback_amount_ratio": [0.8],
                "low_vs_ma20_pct": [1.01],
                "low_vs_ma60_pct": [1.05],
                "lower_shadow_body_ratio": [0.6],
                "close_ge_open": [True],
                "rebound_break_prev_high": [True],
                "ret5": [5.0],
                "limit_up_count_5d": [0],
                "prev_close_below_ma5": [False],
                "close_above_ma5": [True],
                "close_above_prev_high": [True],
                "weak_to_strong_amount_ratio": [1.0],
                "close_vs_ma5_pct": [1.5],
                "platform_high_prev": [10.0],
                "platform_low_prev": [9.4],
                "prev_high": [9.9],
                "prev_low": [9.7],
                "is_strong": [True],
            }
        )

        rows = service._build_stock_feature_records(enriched, pd.DataFrame())

        self.assertEqual(len(rows), 1)
        self.assertFalse(rows[0]["eligible_universe"])

    def test_get_quant_sync_status_summary_aggregates_dates_and_counts(self) -> None:
        stock_daily_model = build_fake_model("StockDaily", ["code", "date"])
        stock_feature_model = build_fake_model("StockDailyFeature", ["code", "trade_date"])
        concept_board_model = build_fake_model("ConceptBoardDailyFeature", ["board_code", "board_name", "trade_date"])
        membership_model = build_fake_model("StockConceptMembershipDaily", ["code", "trade_date"])
        index_feature_model = build_fake_model("IndexDailyFeature", ["trade_date"])
        model_map = {
            "StockDaily": stock_daily_model,
            "StockDailyFeature": stock_feature_model,
            "ConceptBoardDailyFeature": concept_board_model,
            "StockConceptMembershipDaily": membership_model,
            "IndexDailyFeature": index_feature_model,
        }

        fake_db_data = {
            "StockDaily": [
                {"code": "000001", "date": date(2026, 1, 2)},
                {"code": "600519", "date": date(2026, 1, 3)},
                {"code": "000001", "date": date(2026, 1, 3)},
            ],
            "StockDailyFeature": [
                {"code": "000001", "trade_date": date(2026, 1, 2)},
                {"code": "000001", "trade_date": date(2026, 1, 3)},
                {"code": "600519", "trade_date": date(2026, 1, 3)},
            ],
            "ConceptBoardDailyFeature": [
                {"board_code": "BK_A", "board_name": "AI概念", "trade_date": date(2026, 1, 2)},
                {"board_code": "BK_A", "board_name": "AI概念", "trade_date": date(2026, 1, 3)},
                {"board_code": "BK_B", "board_name": "机器人概念", "trade_date": date(2026, 1, 3)},
            ],
            "StockConceptMembershipDaily": [
                {"code": "000001", "trade_date": date(2026, 1, 3)},
                {"code": "600519", "trade_date": date(2026, 1, 3)},
            ],
            "IndexDailyFeature": [
                {"trade_date": date(2026, 1, 2)},
                {"trade_date": date(2026, 1, 3)},
                {"trade_date": date(2026, 1, 3)},
            ],
        }

        class StatusSummaryService(QuantDataService):
            def __init__(self):
                super().__init__(db_manager=FakeDBManager(fake_db_data), fetcher_manager=FakeFetcherManager())
                self._models = model_map

            def _get_model(self, model_name):
                return self._models.get(model_name)

            @staticmethod
            def _resolve_default_stock_pool():
                return ["000001", "600519", "300750"]

        service = StatusSummaryService()

        summary = service.get_quant_sync_status_summary()

        self.assertEqual(summary["as_of_date"], "2026-01-03")
        self.assertEqual(summary["latest_stock_daily_date"], "2026-01-03")
        self.assertEqual(summary["earliest_stock_daily_date"], "2026-01-02")
        self.assertEqual(summary["stock_daily_distinct_codes"], 2)
        self.assertEqual(summary["main_board_stock_pool_size"], 3)
        self.assertEqual(summary["membership_distinct_codes"], 2)
        self.assertEqual(summary["latest_membership_date"], "2026-01-03")
        self.assertEqual(summary["latest_membership_count"], 2)
        self.assertEqual(summary["stock_feature_distinct_codes"], 2)
        self.assertEqual(summary["latest_stock_feature_date"], "2026-01-03")
        self.assertEqual(summary["latest_stock_feature_count"], 2)
        self.assertEqual(summary["concept_board_distinct_count"], 2)
        self.assertEqual(summary["latest_concept_board_date"], "2026-01-03")
        self.assertEqual(summary["latest_concept_board_count"], 2)
        self.assertEqual(summary["index_feature_latest_date"], "2026-01-03")
        self.assertEqual(summary["index_feature_count"], 2)


if __name__ == "__main__":
    unittest.main()
