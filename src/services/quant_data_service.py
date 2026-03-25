# -*- coding: utf-8 -*-
"""Quant data snapshot synchronization service."""

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from hashlib import md5
from io import StringIO
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Type

import pandas as pd
import requests

from data_provider.base import DataFetcherManager, normalize_stock_code
from src.data.stock_mapping import STOCK_NAME_MAP

logger = logging.getLogger(__name__)


class QuantDataService:
    """Synchronize index/concept snapshots into quant feature tables."""

    CONCEPT_INCLUDE_KEYWORDS = (
        "概念",
        "算力",
        "机器人",
        "新能源",
        "光伏",
        "储能",
        "充电桩",
        "半导体",
        "芯片",
        "智能",
        "低空",
        "军工",
        "新型工业化",
        "AI",
        "AIGC",
        "信创",
        "数字",
    )
    CONCEPT_EXCLUDE_KEYWORDS = (
        "行业",
        "地域",
        "指数",
        "沪深",
        "上证",
        "深证",
        "中证",
        "昨日",
        "今日",
        "连板",
        "振幅",
        "换手",
        "融资",
        "融券",
        "龙虎榜",
        "板块涨幅",
    )
    REGION_BOARD_SUFFIXES = (
        "板块",
        "地区",
    )
    BOARD_NAME_ALIAS_MAP = {
        "新能源": ("新能源汽车",),
        "新能源车": ("新能源汽车",),
        "电网概念": ("智能电网",),
        "电网": ("智能电网",),
        "锂矿概念": ("盐湖提锂", "锂电池概念"),
        "CPO概念": ("共封装光学(CPO)",),
        "液冷概念": ("液冷服务器",),
        "券商概念": ("参股券商",),
        "并购重组概念": ("股权转让(并购重组)",),
        "猪肉概念": ("猪肉",),
    }

    def __init__(self, db_manager: Any = None, fetcher_manager: Optional[DataFetcherManager] = None):
        self.db = db_manager
        self.fetcher_manager = fetcher_manager or DataFetcherManager()
        self._model_cache: Dict[str, Optional[Type[Any]]] = {}
        self._ths_board_catalog_cache: Optional[List[Dict[str, str]]] = None

    def refresh_quant_dataset(
        self,
        *,
        stock_codes: Optional[Sequence[str]] = None,
        as_of_date: Optional[str] = None,
        history_days: int = 130,
        index_codes: Sequence[str] = ("sh000001", "sz399001", "sz399006"),
        include_ranked_boards: bool = True,
        ranking_size: int = 80,
        concept_commit_batch_size: int = 20,
        concept_retry_attempts: int = 2,
        feature_batch_size: int = 200,
        latest_feature_only: bool = False,
    ) -> Dict[str, Any]:
        """One-click refresh for quant strategy data and features."""
        summary: Dict[str, Any] = {
            "as_of_date": None,
            "stock_pool_size": 0,
            "stock_directory_sync": {},
            "stock_history_sync": {},
            "index_sync": {},
            "membership_sync": {},
            "membership_expand": {},
            "board_history_sync": {},
            "board_feature_build": {},
            "stock_feature_build": {},
            "errors": [],
        }
        if self.db is None:
            summary["errors"].append("Database manager is required for refresh_quant_dataset.")
            return summary

        snapshot_date = self._to_date(as_of_date) if as_of_date else self._resolve_latest_stock_date()
        if snapshot_date is None:
            snapshot_date = date.today()
        summary["as_of_date"] = snapshot_date.isoformat()

        is_latest_only = bool(latest_feature_only)
        latest_index_feature_date = self._query_model_edge_date(
            model_name="IndexDailyFeature",
            date_field="trade_date",
            latest=True,
        )
        latest_membership_date = self._query_model_edge_date(
            model_name="StockConceptMembershipDaily",
            date_field="trade_date",
            latest=True,
        )
        latest_board_feature_date = self._query_model_edge_date(
            model_name="ConceptBoardDailyFeature",
            date_field="trade_date",
            latest=True,
        )

        if is_latest_only:
            directory_summary = {
                "requested": 0,
                "records": 0,
                "saved": 0,
                "updated": 0,
                "deferred": 0,
                "model_available": self._get_model("StockDirectory") is not None,
                "data_source": None,
                "errors": [],
                "skipped": True,
                "skip_reason": "latest_feature_only",
            }
        else:
            directory_summary = self.sync_stock_directory()
        summary["stock_directory_sync"] = directory_summary

        latest_stock_daily_date = self._resolve_latest_stock_date()

        pool = [
            normalize_stock_code(code)
            for code in (stock_codes or [])
            if str(code).strip() and self._is_main_board_stock(str(code))
        ]
        if not pool:
            if latest_feature_only and self.db is not None and hasattr(self.db, "get_session"):
                pool = self._list_stock_pool()
            else:
                pool = []
        if not pool:
            pool = self._resolve_default_stock_pool()
        if not pool:
            summary["errors"].append("No stock codes found from market stock list or stock_daily fallback.")
            return summary
        summary["stock_pool_size"] = len(pool)

        stock_history_days = 10 if is_latest_only else max(int(history_days), 130)
        board_history_days = 10 if is_latest_only else max(int(history_days), 90)
        membership_expand_start_date = snapshot_date if is_latest_only else self._resolve_earliest_stock_date()
        membership_expand_end_date = snapshot_date if is_latest_only else snapshot_date

        if is_latest_only and latest_stock_daily_date is not None and latest_stock_daily_date >= snapshot_date:
            history_summary = {
                "requested": len(pool),
                "fetched": 0,
                "saved": 0,
                "rows": 0,
                "empty": 0,
                "errors": [],
                "effective_days": 0,
                "skipped": True,
                "skip_reason": "latest_stock_daily_already_present",
            }
        else:
            history_summary = self.backfill_stock_daily_history(
                stock_codes=pool,
                end_date=snapshot_date.isoformat(),
                days=stock_history_days,
            )
        summary["stock_history_sync"] = history_summary

        latest_after_backfill = self._resolve_latest_stock_date()
        if latest_after_backfill and latest_after_backfill <= snapshot_date:
            snapshot_date = latest_after_backfill
            summary["as_of_date"] = snapshot_date.isoformat()

        if is_latest_only and latest_index_feature_date is not None and latest_index_feature_date >= snapshot_date:
            index_summary = {
                "requested": len(index_codes),
                "fetched": 0,
                "saved": 0,
                "records": 0,
                "empty": 0,
                "errors": [],
                "model_available": self._get_model("IndexDailyFeature") is not None and self.db is not None,
                "skipped": True,
                "skip_reason": "latest_index_feature_already_present",
            }
        else:
            index_summary = self.sync_index_history(
                index_codes=index_codes,
                end_date=snapshot_date.isoformat(),
                days=max(260, history_summary.get("effective_days", 0), int(history_days)),
            )
        summary["index_sync"] = index_summary

        if is_latest_only and latest_membership_date is not None and latest_membership_date >= snapshot_date:
            membership_summary = {
                "requested": len(pool),
                "fetched": 0,
                "saved": 0,
                "records": 0,
                "empty": 0,
                "errors": [],
                "model_available": self._get_model("StockConceptMembershipDaily") is not None and self.db is not None,
                "skipped": True,
                "skip_reason": "latest_membership_already_present",
            }
        else:
            membership_summary = self.sync_stock_concept_memberships(
                stock_codes=pool,
                trade_date=snapshot_date.isoformat(),
            )
        summary["membership_sync"] = membership_summary

        if is_latest_only:
            expand_summary = {
                "requested_dates": 0,
                "records": 0,
                "saved": 0,
                "errors": [],
                "skipped": True,
                "skip_reason": "latest_feature_only",
            }
        else:
            expand_summary = self.expand_membership_snapshot(
                snapshot_date=snapshot_date,
                start_date=membership_expand_start_date,
                end_date=membership_expand_end_date,
            )
        summary["membership_expand"] = expand_summary

        if is_latest_only and latest_board_feature_date is not None and latest_board_feature_date >= snapshot_date:
            board_history_summary = {
                "requested": 0,
                "fetched": 0,
                "saved": 0,
                "records": 0,
                "empty": 0,
                "errors": [],
                "retry_count": 0,
                "failed_boards": [],
                "committed_batches": 0,
                "completed_boards": 0,
                "model_available": self._get_model("ConceptBoardDailyFeature") is not None and self.db is not None,
                "skipped": True,
                "skip_reason": "latest_board_feature_already_present",
            }
        else:
            board_names = self._fetch_all_concept_board_names(
                snapshot_date=snapshot_date,
                include_ranked_boards=include_ranked_boards,
                ranking_size=ranking_size,
            )
            board_history_summary = self.sync_concept_board_history(
                board_names=board_names,
                start_date=snapshot_date.isoformat() if is_latest_only else None,
                end_date=snapshot_date.isoformat(),
                days=board_history_days,
                commit_batch_size=concept_commit_batch_size,
                retry_attempts=concept_retry_attempts,
            )
        summary["board_history_sync"] = board_history_summary

        build_summary = self.build_quant_features(
            as_of_date=snapshot_date,
            history_days=max(int(history_days), 130),
            stock_batch_size=feature_batch_size,
            latest_only=latest_feature_only,
        )
        summary["board_feature_build"] = build_summary.get("board_feature_build", {})
        summary["stock_feature_build"] = build_summary.get("stock_feature_build", {})
        summary["errors"].extend(build_summary.get("errors", []))
        return summary

    def sync_stock_directory(self) -> Dict[str, Any]:
        """Synchronize a local stock master directory for stable name resolution."""
        stock_directory_model = self._get_model("StockDirectory")
        summary: Dict[str, Any] = {
            "requested": 0,
            "records": 0,
            "saved": 0,
            "updated": 0,
            "deferred": 0,
            "model_available": stock_directory_model is not None,
            "data_source": None,
            "errors": [],
        }
        if self.db is None or stock_directory_model is None:
            return summary

        stock_frame, data_source = self._fetch_stock_directory_frame()
        if stock_frame is None or stock_frame.empty:
            summary["errors"].append("No stock directory payload returned from configured sources.")
            return summary

        normalized = self._normalize_stock_directory_frame(stock_frame, data_source=data_source)
        summary["requested"] = int(len(stock_frame))
        summary["records"] = int(len(normalized))
        summary["data_source"] = data_source
        if normalized.empty:
            summary["errors"].append("Stock directory payload did not contain usable code/name rows.")
            return summary

        has_session_support = hasattr(self.db, "session_scope") or hasattr(self.db, "get_session")
        if not has_session_support:
            for record in normalized.to_dict("records"):
                STOCK_NAME_MAP[record["code"]] = record["name"]
            summary["deferred"] = int(len(normalized))
            return summary

        records = normalized.to_dict("records")
        with self._session_scope(stock_directory_model) as session:
            if session is None:
                for record in records:
                    STOCK_NAME_MAP[record["code"]] = record["name"]
                summary["deferred"] = int(len(records))
                return summary
            codes = [record["code"] for record in records]
            existing_rows = (
                session.query(stock_directory_model)
                .filter(stock_directory_model.code.in_(codes))
                .all()
            )
            existing_map = {str(row.code): row for row in existing_rows}

            for record in records:
                code = record["code"]
                existing = existing_map.get(code)
                if existing is None:
                    session.add(stock_directory_model(**record))
                    summary["saved"] += 1
                else:
                    changed = False
                    for field_name, field_value in record.items():
                        if getattr(existing, field_name, None) != field_value:
                            setattr(existing, field_name, field_value)
                            changed = True
                    if changed:
                        summary["updated"] += 1
                STOCK_NAME_MAP[code] = record["name"]
        return summary

    def get_quant_sync_status_summary(self, *, as_of_date: Optional[str] = None) -> Dict[str, Any]:
        """Build page-ready quant synchronization status summary."""
        latest_stock_daily_date = self._query_model_edge_date(model_name="StockDaily", date_field="date", latest=True)
        earliest_stock_daily_date = self._query_model_edge_date(model_name="StockDaily", date_field="date", latest=False)
        latest_stock_feature_date = self._query_model_edge_date(
            model_name="StockDailyFeature",
            date_field="trade_date",
            latest=True,
        )
        latest_concept_board_date = self._query_model_edge_date(
            model_name="ConceptBoardDailyFeature",
            date_field="trade_date",
            latest=True,
        )
        latest_membership_date = self._query_model_edge_date(
            model_name="StockConceptMembershipDaily",
            date_field="trade_date",
            latest=True,
        )
        index_feature_latest_date = self._query_model_edge_date(
            model_name="IndexDailyFeature",
            date_field="trade_date",
            latest=True,
        )

        resolved_as_of_date = latest_stock_daily_date
        if as_of_date:
            try:
                resolved_as_of_date = self._to_date(as_of_date)
            except Exception as exc:
                logger.warning(f"[QuantData] invalid as_of_date for status summary: {as_of_date}, error={exc}")
        summary: Dict[str, Any] = {
            "as_of_date": resolved_as_of_date.isoformat() if resolved_as_of_date else None,
            "latest_stock_daily_date": latest_stock_daily_date.isoformat() if latest_stock_daily_date else None,
            "earliest_stock_daily_date": earliest_stock_daily_date.isoformat() if earliest_stock_daily_date else None,
            "stock_daily_distinct_codes": self._query_model_distinct_count(
                model_name="StockDaily",
                field_candidates=["code"],
            ),
            "main_board_stock_pool_size": len(self._resolve_default_stock_pool()),
            "membership_distinct_codes": self._query_model_distinct_count(
                model_name="StockConceptMembershipDaily",
                field_candidates=["code"],
            ),
            "latest_membership_date": latest_membership_date.isoformat() if latest_membership_date else None,
            "latest_membership_count": self._query_model_date_count(
                model_name="StockConceptMembershipDaily",
                date_field="trade_date",
                target_date=latest_membership_date,
            ),
            "stock_feature_distinct_codes": self._query_model_distinct_count(
                model_name="StockDailyFeature",
                field_candidates=["code"],
            ),
            "latest_stock_feature_date": latest_stock_feature_date.isoformat() if latest_stock_feature_date else None,
            "latest_stock_feature_count": self._query_model_date_count(
                model_name="StockDailyFeature",
                date_field="trade_date",
                target_date=latest_stock_feature_date,
            ),
            "concept_board_distinct_count": self._query_model_distinct_count(
                model_name="ConceptBoardDailyFeature",
                field_candidates=["board_code", "board_name"],
            ),
            "latest_concept_board_date": latest_concept_board_date.isoformat() if latest_concept_board_date else None,
            "latest_concept_board_count": self._query_model_date_count(
                model_name="ConceptBoardDailyFeature",
                date_field="trade_date",
                target_date=latest_concept_board_date,
            ),
            "index_feature_latest_date": index_feature_latest_date.isoformat() if index_feature_latest_date else None,
            "index_feature_count": self._query_model_date_count(
                model_name="IndexDailyFeature",
                date_field="trade_date",
                target_date=index_feature_latest_date,
            ),
        }
        return summary

    def backfill_stock_daily_history(
        self,
        *,
        stock_codes: Sequence[str],
        end_date: Optional[str] = None,
        days: int = 130,
    ) -> Dict[str, Any]:
        """Backfill stock_daily with additional history for quant feature windows."""
        summary: Dict[str, Any] = {
            "requested": len(stock_codes),
            "fetched": 0,
            "saved": 0,
            "rows": 0,
            "empty": 0,
            "errors": [],
            "effective_days": int(days),
        }
        if self.db is None:
            summary["errors"].append("Database manager is required.")
            return summary
        if not hasattr(self.db, "save_daily_data"):
            summary["errors"].append("Database manager does not support save_daily_data.")
            return summary

        prefer_bulk_fetcher = len(stock_codes) >= 50
        for raw_code in stock_codes:
            code = normalize_stock_code(raw_code)
            try:
                df, source_name = self._fetch_stock_daily_backfill_data(
                    stock_code=code,
                    end_date=end_date,
                    days=max(int(days), 1),
                    prefer_bulk_fetcher=prefer_bulk_fetcher,
                )
                if df is None or df.empty:
                    summary["empty"] += 1
                    continue
                summary["fetched"] += 1
                summary["rows"] += int(len(df))
                saved = int(self.db.save_daily_data(df=df, code=code, data_source=source_name))
                summary["saved"] += saved
            except Exception as exc:
                logger.warning(f"[QuantData] backfill_stock_daily_history failed: {code}, error={exc}")
                summary["errors"].append({"symbol": code, "error": str(exc)})
        return summary

    def _fetch_stock_daily_backfill_data(
        self,
        *,
        stock_code: str,
        end_date: Optional[str],
        days: int,
        prefer_bulk_fetcher: bool,
    ) -> Tuple[pd.DataFrame, str]:
        if prefer_bulk_fetcher:
            preferred = self._fetch_stock_daily_with_preferred_fetcher(stock_code=stock_code, end_date=end_date, days=days)
            if preferred is not None:
                return preferred
        return self.fetcher_manager.get_daily_data(stock_code, end_date=end_date, days=days)

    def _fetch_stock_daily_with_preferred_fetcher(
        self,
        *,
        stock_code: str,
        end_date: Optional[str],
        days: int,
    ) -> Optional[Tuple[pd.DataFrame, str]]:
        """Prefer stable bulk-history fetchers during large stock_daily backfills."""
        for fetcher in self._iter_preferred_bulk_fetchers():
            try:
                df = fetcher.get_daily_data(stock_code=stock_code, end_date=end_date, days=days)
                if df is not None and not df.empty:
                    return df, str(getattr(fetcher, "name", fetcher.__class__.__name__))
            except Exception as exc:
                fetcher_name = getattr(fetcher, "name", fetcher.__class__.__name__)
                logger.warning(f"[QuantData] preferred bulk fetcher failed: {fetcher_name}, {stock_code}, error={exc}")
        return None

    def _iter_preferred_bulk_fetchers(self) -> Iterable[Any]:
        manager = self.fetcher_manager
        if manager is None:
            return []
        preferred_order = ("BaostockFetcher",)
        fetchers = list(getattr(manager, "_fetchers", []) or [])
        ordered: List[Any] = []
        for fetcher_name in preferred_order:
            ordered.extend(fetcher for fetcher in fetchers if getattr(fetcher, "name", "") == fetcher_name)
        return ordered

    def expand_membership_snapshot(
        self,
        *,
        snapshot_date: date,
        start_date: Optional[date],
        end_date: Optional[date],
    ) -> Dict[str, Any]:
        """Expand one-day stock concept membership snapshot to all stock trading dates."""
        model = self._get_model("StockConceptMembershipDaily")
        stock_daily_model = self._get_model("StockDaily")
        if model is None or stock_daily_model is None or self.db is None:
            return {"requested_dates": 0, "records": 0, "saved": 0, "errors": ["Required models unavailable."]}

        with self.db.get_session() as session:
            snapshot_rows = (
                session.query(model)
                .filter(model.trade_date == snapshot_date)
                .order_by(model.code.asc(), model.is_primary.desc())
                .all()
            )
            if not snapshot_rows:
                return {"requested_dates": 0, "records": 0, "saved": 0, "errors": []}
            date_query = session.query(stock_daily_model.date).distinct()
            if start_date:
                date_query = date_query.filter(stock_daily_model.date >= start_date)
            if end_date:
                date_query = date_query.filter(stock_daily_model.date <= end_date)
            trade_dates = [row[0] for row in date_query.order_by(stock_daily_model.date.asc()).all()]

        records: List[Dict[str, Any]] = []
        for trade_date in trade_dates:
            for row in snapshot_rows:
                records.append(
                    {
                        "code": row.code,
                        "trade_date": trade_date,
                        "board_code": row.board_code,
                        "board_name": row.board_name,
                        "is_primary": bool(row.is_primary),
                    }
                )
        saved = 0
        with self._session_scope(model) as session:
            if session is not None:
                saved = self._upsert_records(
                    session=session,
                    model=model,
                    records=records,
                    unique_keys=["code", "trade_date", "board_code"],
                )
        return {"requested_dates": len(trade_dates), "records": len(records), "saved": saved, "errors": []}

    def build_quant_features(
        self,
        *,
        as_of_date: date,
        history_days: int = 130,
        stock_batch_size: int = 200,
        latest_only: bool = False,
    ) -> Dict[str, Any]:
        """Build quant features in stock batches to avoid large in-memory snapshots."""
        result: Dict[str, Any] = {
            "board_feature_build": {"records": 0, "saved": 0, "dates": 0, "batches": 0},
            "stock_feature_build": {"records": 0, "saved": 0, "dates": 0, "batches": 0},
            "errors": [],
        }
        if self.db is None:
            result["errors"].append("Database manager is required.")
            return result

        stock_daily_model = self._get_model("StockDaily")
        membership_model = self._get_model("StockConceptMembershipDaily")
        board_model = self._get_model("ConceptBoardDailyFeature")
        stock_feature_model = self._get_model("StockDailyFeature")
        if any(model is None for model in [stock_daily_model, membership_model, board_model, stock_feature_model]):
            result["errors"].append("Required quant models are unavailable.")
            return result

        window_start_date = as_of_date - timedelta(days=max(int(history_days), 1))
        output_start_date = as_of_date if latest_only else window_start_date
        load_start_date = self._resolve_feature_build_start_date(as_of_date=output_start_date, history_days=history_days)
        board_rows = self._load_board_rows_for_feature_build(start_date=load_start_date, end_date=as_of_date)
        if not board_rows:
            result["errors"].append("No concept board history found for feature generation.")
            return result
        if latest_only:
            snapshot_rows = self._load_membership_rows_for_feature_build(trade_date=as_of_date)
            board_map = self._build_board_map(snapshot_rows)
            board_map_by_date: Optional[Dict[date, Dict[str, Tuple[str, str]]]] = None
        else:
            window_rows = self._load_membership_rows_for_feature_window(start_date=window_start_date, end_date=as_of_date)
            board_map_by_date = self._build_board_map_by_trade_date(window_rows)
            latest_rows = self._load_membership_rows_for_feature_build(trade_date=as_of_date)
            board_map = self._build_board_map(latest_rows)
        board_history_df = self._to_board_history_frame(board_rows)
        feature_stock_codes = self._list_stock_codes_for_feature_build(start_date=load_start_date, end_date=as_of_date)
        if not feature_stock_codes:
            result["errors"].append("No stock history found for quant feature generation.")
            return result

        latest_trade_ts = pd.Timestamp(as_of_date)
        output_start_ts = pd.Timestamp(output_start_date)
        batch_size = max(int(stock_batch_size), 1)
        board_aggregate: Dict[Tuple[pd.Timestamp, str, str], Dict[str, Any]] = {}
        leader_aggregate: Dict[Tuple[pd.Timestamp, str], Dict[str, Any]] = {}
        enriched_batches: List[pd.DataFrame] = []
        supplement_batches = 0
        for batch_codes in self._chunked(feature_stock_codes, batch_size):
            try:
                stock_rows = self._load_stock_rows_for_feature_build(
                    start_date=load_start_date,
                    end_date=as_of_date,
                    stock_codes=batch_codes,
                )
                stock_df = (
                    self._to_stock_frame(
                        stock_rows,
                        board_map=board_map,
                        board_map_by_date=board_map_by_date,
                    )
                    if stock_rows
                    else pd.DataFrame()
                )
                enriched = self._enrich_stock_frame(stock_df) if not stock_df.empty else pd.DataFrame()
                if not enriched.empty:
                    enriched = enriched[
                        (enriched["trade_date"] >= output_start_ts) & (enriched["trade_date"] <= latest_trade_ts)
                    ].copy()
                self._accumulate_board_supplement(enriched, aggregate=board_aggregate, leaders=leader_aggregate)
                if not enriched.empty:
                    enriched_batches.append(enriched)
                supplement_batches += 1
            except Exception as exc:
                logger.warning(
                    "[QuantData] build_quant_features supplement batch failed: "
                    f"codes={len(batch_codes)}, error={exc}"
                )
                result["errors"].append({"phase": "board_supplement", "codes": len(batch_codes), "error": str(exc)})

        board_supplement = self._finalize_board_supplement_aggregate(board_aggregate, leader_aggregate)
        board_frame = self._build_board_frame_from_history(board_history_df, board_supplement)
        if not board_frame.empty:
            board_frame = board_frame[
                (board_frame["trade_date"] >= output_start_ts) & (board_frame["trade_date"] <= latest_trade_ts)
            ].copy()
        board_records = self._build_board_feature_records(board_frame)

        with self._session_scope(board_model) as session:
            saved_board = 0
            if session is not None:
                saved_board = self._upsert_records(
                    session=session,
                    model=board_model,
                    records=board_records,
                    unique_keys=["board_code", "trade_date"],
                )

        stock_record_count = 0
        saved_stock = 0
        stock_dates: set = set()
        stock_batches = 0
        for enriched in enriched_batches:
            try:
                if enriched.empty:
                    stock_batches += 1
                    continue
                stock_records = self._build_stock_feature_records(enriched, board_frame)
                stock_record_count += len(stock_records)
                stock_dates.update(pd.Timestamp(value).date() for value in enriched["trade_date"].tolist())
                with self._session_scope(stock_feature_model) as session:
                    if session is not None:
                        saved_stock += self._upsert_records(
                            session=session,
                            model=stock_feature_model,
                            records=stock_records,
                            unique_keys=["code", "trade_date"],
                        )
                stock_batches += 1
            except Exception as exc:
                logger.warning("[QuantData] build_quant_features stock batch failed: error=%s", exc)
                result["errors"].append({"phase": "stock_feature", "error": str(exc)})

        result["board_feature_build"] = {
            "records": len(board_records),
            "saved": saved_board,
            "dates": int(board_frame["trade_date"].nunique()) if not board_frame.empty else 0,
            "batches": supplement_batches,
        }
        result["stock_feature_build"] = {
            "records": stock_record_count,
            "saved": saved_stock,
            "dates": len(stock_dates),
            "batches": stock_batches,
        }
        return result

    @staticmethod
    def _resolve_feature_build_start_date(*, as_of_date: date, history_days: int) -> date:
        lookback_days = max(int(history_days), 90)
        return as_of_date - timedelta(days=lookback_days)

    def _load_board_rows_for_feature_build(self, *, start_date: date, end_date: date) -> List[Any]:
        board_model = self._get_model("ConceptBoardDailyFeature")
        if self.db is None or board_model is None:
            return []
        with self.db.get_session() as session:
            return (
                session.query(board_model)
                .filter(board_model.trade_date >= start_date, board_model.trade_date <= end_date)
                .order_by(board_model.board_code.asc(), board_model.trade_date.asc())
                .all()
            )

    def _load_membership_rows_for_feature_build(self, *, trade_date: date) -> List[Any]:
        membership_model = self._get_model("StockConceptMembershipDaily")
        if self.db is None or membership_model is None:
            return []
        with self.db.get_session() as session:
            return (
                session.query(membership_model)
                .filter(membership_model.trade_date == trade_date)
                .order_by(membership_model.code.asc(), membership_model.is_primary.desc())
                .all()
            )

    def _load_membership_rows_for_feature_window(self, *, start_date: date, end_date: date) -> List[Any]:
        membership_model = self._get_model("StockConceptMembershipDaily")
        if self.db is None or membership_model is None:
            return []
        with self.db.get_session() as session:
            return (
                session.query(membership_model)
                .filter(membership_model.trade_date >= start_date, membership_model.trade_date <= end_date)
                .order_by(membership_model.trade_date.asc(), membership_model.code.asc(), membership_model.is_primary.desc())
                .all()
            )

    def _list_stock_codes_for_feature_build(self, *, start_date: date, end_date: date) -> List[str]:
        stock_daily_model = self._get_model("StockDaily")
        if self.db is None or stock_daily_model is None:
            return []
        with self.db.get_session() as session:
            rows = (
                session.query(stock_daily_model.code)
                .filter(stock_daily_model.date >= start_date, stock_daily_model.date <= end_date)
                .distinct()
                .order_by(stock_daily_model.code.asc())
                .all()
            )
        return [normalize_stock_code(row[0]) for row in rows if row and self._as_str(row[0])]

    def _list_feature_trade_dates(self, *, start_date: date, end_date: date) -> List[date]:
        stock_daily_model = self._get_model("StockDaily")
        if self.db is None or stock_daily_model is None:
            return []
        with self.db.get_session() as session:
            rows = (
                session.query(stock_daily_model.date)
                .filter(stock_daily_model.date >= start_date, stock_daily_model.date <= end_date)
                .distinct()
                .order_by(stock_daily_model.date.asc())
                .all()
            )
        return [self._to_date(row[0]) for row in rows if row and row[0] is not None]

    def _load_stock_rows_for_feature_build(
        self,
        *,
        start_date: date,
        end_date: date,
        stock_codes: Sequence[str],
    ) -> List[Any]:
        stock_daily_model = self._get_model("StockDaily")
        if self.db is None or stock_daily_model is None or not stock_codes:
            return []
        normalized_codes = [normalize_stock_code(code) for code in stock_codes if self._as_str(code)]
        if not normalized_codes:
            return []
        with self.db.get_session() as session:
            return (
                session.query(stock_daily_model)
                .filter(
                    stock_daily_model.date >= start_date,
                    stock_daily_model.date <= end_date,
                    stock_daily_model.code.in_(normalized_codes),
                )
                .order_by(stock_daily_model.code.asc(), stock_daily_model.date.asc())
                .all()
            )

    @staticmethod
    def _chunked(items: Sequence[str], chunk_size: int) -> Iterable[List[str]]:
        size = max(int(chunk_size), 1)
        for index in range(0, len(items), size):
            yield list(items[index : index + size])

    def _accumulate_board_supplement(
        self,
        enriched: pd.DataFrame,
        *,
        aggregate: Dict[Tuple[pd.Timestamp, str, str], Dict[str, Any]],
        leaders: Dict[Tuple[pd.Timestamp, str], Dict[str, Any]],
    ) -> None:
        """Accumulate board-level breadth and leader metrics without keeping full stock history in memory."""
        if enriched.empty:
            return
        for row in enriched.itertuples(index=False):
            trade_ts = pd.Timestamp(row.trade_date)
            board_code = str(row.board_code)
            board_name = str(row.board_name)
            aggregate_key = (trade_ts, board_code, board_name)
            stats = aggregate.setdefault(
                aggregate_key,
                {
                    "member_count": 0,
                    "strong_stock_count": 0,
                    "limit_up_count": 0,
                    "limit_down_count": 0,
                    "big_drop_count": 0,
                    "member_fall20_count": 0,
                    "top_pct_values": [],
                },
            )
            pct_chg = self._safe_float(getattr(row, "pct_chg", 0.0), 0.0)
            stats["member_count"] += 1
            stats["strong_stock_count"] += int(bool(getattr(row, "is_strong", False)))
            stats["limit_up_count"] += int(pct_chg >= 9.8)
            stats["limit_down_count"] += int(pct_chg <= -9.8)
            stats["big_drop_count"] += int(pct_chg <= -5.0)
            stats["member_fall20_count"] += int(pct_chg <= -2.0)
            stats["top_pct_values"].append(pct_chg)
            stats["top_pct_values"] = sorted(stats["top_pct_values"], reverse=True)[:5]

            leader_key = (trade_ts, board_code)
            leader_rank = (
                self._safe_float(getattr(row, "ret20", 0.0), 0.0),
                self._safe_float(getattr(row, "amount_5d", 0.0), 0.0),
                self._safe_int(getattr(row, "breakout_count_3d", 0), 0),
                self._safe_float(getattr(row, "return_2d", 0.0), 0.0),
                str(getattr(row, "code", "")),
            )
            current_leader = leaders.get(leader_key)
            if current_leader is not None and current_leader["rank"] >= leader_rank:
                continue
            leaders[leader_key] = {
                "rank": leader_rank,
                "board_name": board_name,
                "leader_stock_code": str(getattr(row, "code", "")),
                "leader_stock_name": str(getattr(row, "code", "")),
                "leader_2d_return": self._safe_float(getattr(row, "return_2d", 0.0), 0.0),
                "leader_limit_up_3d": self._safe_int(getattr(row, "leader_limit_up_3d", 0), 0),
                "leader_payload": {
                    "ret20": self._safe_float(getattr(row, "ret20", 0.0), 0.0),
                    "amount_5d": self._safe_float(getattr(row, "amount_5d", 0.0), 0.0),
                    "breakout_count_3d": self._safe_int(getattr(row, "breakout_count_3d", 0), 0),
                    "consecutive_new_high_3d": self._safe_int(getattr(row, "consecutive_new_high_3d", 0), 0),
                    "close_vs_ma5_pct": self._safe_float(getattr(row, "close_vs_ma5_pct", 0.0), 0.0),
                    "close_above_ma10": bool(getattr(row, "close_above_ma10", False)),
                    "low_above_ma20": bool(getattr(row, "low_above_ma20", False)),
                    "pullback_volume_ratio": self._safe_float(getattr(row, "pullback_volume_ratio", 1.0), 1.0),
                    "single_day_drop_pct": self._safe_float(getattr(row, "single_day_drop_pct", 0.0), 0.0),
                    "broke_ma10_with_volume": bool(getattr(row, "broke_ma10_with_volume", False)),
                    "broke_ma20": bool(getattr(row, "broke_ma20", False)),
                    "is_limit_down": bool(getattr(row, "is_limit_down", False)),
                    "close_to_5d_high_drawdown_pct": self._safe_float(
                        getattr(row, "close_to_5d_high_drawdown_pct", 0.0),
                        0.0,
                    ),
                    "return_2d": self._safe_float(getattr(row, "return_2d", 0.0), 0.0),
                    "limit_up_count_3d": self._safe_int(getattr(row, "leader_limit_up_3d", 0), 0),
                },
            }

    def _finalize_board_supplement_aggregate(
        self,
        aggregate: Dict[Tuple[pd.Timestamp, str, str], Dict[str, Any]],
        leaders: Dict[Tuple[pd.Timestamp, str], Dict[str, Any]],
    ) -> pd.DataFrame:
        """Finalize board supplement rows from the incremental aggregate cache."""
        if not aggregate:
            return pd.DataFrame()

        rows: List[Dict[str, Any]] = []
        for (trade_ts, board_code, board_name), stats in aggregate.items():
            member_count = max(int(stats.get("member_count", 0)), 0)
            strong_stock_count = max(int(stats.get("strong_stock_count", 0)), 0)
            limit_up_count = max(int(stats.get("limit_up_count", 0)), 0)
            limit_down_count = max(int(stats.get("limit_down_count", 0)), 0)
            big_drop_count = max(int(stats.get("big_drop_count", 0)), 0)
            member_fall20_count = max(int(stats.get("member_fall20_count", 0)), 0)
            top_pct_values = list(stats.get("top_pct_values", []) or [])
            leader = leaders.get((trade_ts, board_code), {})
            rows.append(
                {
                    "trade_date": trade_ts,
                    "board_code": board_code,
                    "board_name": board_name,
                    "member_count": member_count,
                    "strong_stock_count": strong_stock_count,
                    "limit_up_count": limit_up_count,
                    "limit_down_count": limit_down_count,
                    "top5_avg_pct": float(sum(top_pct_values) / len(top_pct_values)) if top_pct_values else 0.0,
                    "big_drop_ratio": (big_drop_count / member_count) if member_count else 0.0,
                    "member_fall20_ratio": (member_fall20_count / member_count) if member_count else 0.0,
                    "breadth_ratio": (strong_stock_count / member_count) if member_count else 0.0,
                    "leader_stock_code": self._as_str(leader.get("leader_stock_code")),
                    "leader_stock_name": self._as_str(leader.get("leader_stock_name")),
                    "leader_2d_return": self._safe_float(leader.get("leader_2d_return", 0.0), 0.0),
                    "leader_limit_up_3d": self._safe_int(leader.get("leader_limit_up_3d", 0), 0),
                    "leader_payload": leader.get("leader_payload", {}),
                }
            )
        frame = pd.DataFrame(rows)
        if frame.empty:
            return frame
        frame = frame.sort_values(["board_code", "trade_date"]).reset_index(drop=True)
        frame["prev_limit_up_count"] = frame.groupby("board_code")["limit_up_count"].shift(1).fillna(0).astype(int)
        return frame

    def sync_index_history(
        self,
        index_codes: Sequence[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        days: int = 260,
    ) -> Dict[str, Any]:
        """Sync index daily snapshots."""
        model = self._get_model("IndexDailyFeature")
        summary = self._empty_summary(index_codes)

        with self._session_scope(model) as session:
            for index_code in index_codes:
                try:
                    raw_df = self.fetcher_manager.get_index_history(
                        index_code=index_code,
                        start_date=start_date,
                        end_date=end_date,
                        days=days,
                    )
                    if raw_df is None or raw_df.empty:
                        summary["empty"] += 1
                        continue

                    summary["fetched"] += 1
                    records = self._build_index_records(index_code=index_code, df=raw_df)
                    summary["records"] += len(records)
                    if session is None:
                        summary["deferred"] += len(records)
                        continue

                    saved = self._upsert_records(
                        session=session,
                        model=model,
                        records=records,
                        unique_keys=["index_code", "trade_date"],
                    )
                    summary["saved"] += saved
                except Exception as exc:
                    logger.warning(f"[QuantData] sync_index_history failed: {index_code}, error={exc}")
                    summary["errors"].append({"symbol": index_code, "error": str(exc)})
        summary["model_available"] = model is not None and self.db is not None
        return summary

    def sync_concept_board_history(
        self,
        board_names: Sequence[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        days: int = 60,
        commit_batch_size: int = 20,
        retry_attempts: int = 2,
    ) -> Dict[str, Any]:
        """Sync concept board daily snapshots."""
        model = self._get_model("ConceptBoardDailyFeature")
        summary = self._empty_summary(board_names)
        summary.update(
            {
                "retry_count": 0,
                "failed_boards": [],
                "committed_batches": 0,
                "completed_boards": 0,
            }
        )

        board_queue = [self._as_str(name) for name in board_names]
        board_queue = [name for name in board_queue if name]
        batch_size = max(int(commit_batch_size), 1)
        max_retries = max(int(retry_attempts), 0)
        max_attempts = max_retries + 1

        def fetch_board_history_with_retry(board_name: str) -> Optional[pd.DataFrame]:
            for attempt in range(1, max_attempts + 1):
                if attempt > 1:
                    summary["retry_count"] += 1
                try:
                    raw_df = self._fetch_concept_board_history_from_ths(
                        board_name=board_name,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    if raw_df is None or raw_df.empty:
                        raw_df = self.fetcher_manager.get_concept_board_history(
                            board_name=board_name,
                            start_date=start_date,
                            end_date=end_date,
                            days=days,
                        )
                    if raw_df is not None and not raw_df.empty:
                        return raw_df
                except Exception as exc:
                    if attempt >= max_attempts:
                        raise
                    logger.warning(
                        f"[QuantData] sync_concept_board_history retry: {board_name}, "
                        f"attempt={attempt}/{max_attempts}, error={exc}"
                    )
            return None

        def process_board(board_name: str, session: Any) -> None:
            try:
                raw_df = fetch_board_history_with_retry(board_name)
                if raw_df is None or raw_df.empty:
                    summary["empty"] += 1
                    summary["failed_boards"].append(board_name)
                    return

                summary["fetched"] += 1
                records = self._build_concept_board_records(board_name=board_name, df=raw_df)
                if not records:
                    summary["empty"] += 1
                    summary["failed_boards"].append(board_name)
                    return

                summary["records"] += len(records)
                if session is None:
                    summary["deferred"] += len(records)
                    summary["completed_boards"] += 1
                    return

                # Use a nested transaction when available, and gracefully fall back
                # for lightweight testing sessions that do not implement begin_nested.
                if hasattr(session, "begin_nested"):
                    with session.begin_nested():
                        saved = self._upsert_records(
                            session=session,
                            model=model,
                            records=records,
                            unique_keys=["board_code", "board_name", "trade_date"],
                        )
                else:
                    saved = self._upsert_records(
                        session=session,
                        model=model,
                        records=records,
                        unique_keys=["board_code", "board_name", "trade_date"],
                    )
                summary["saved"] += saved
                summary["completed_boards"] += 1
            except Exception as exc:
                logger.warning(f"[QuantData] sync_concept_board_history failed: {board_name}, error={exc}")
                summary["errors"].append({"symbol": board_name, "error": str(exc)})
                summary["failed_boards"].append(board_name)

        if model is None or self.db is None:
            for board_name in board_queue:
                process_board(board_name, session=None)
            summary["committed_batches"] = 0
        else:
            for offset in range(0, len(board_queue), batch_size):
                batch = board_queue[offset : offset + batch_size]
                try:
                    with self._session_scope(model) as session:
                        for board_name in batch:
                            process_board(board_name, session=session)
                    summary["committed_batches"] += 1
                except Exception as exc:
                    logger.warning(
                        f"[QuantData] sync_concept_board_history batch commit failed: "
                        f"offset={offset}, size={len(batch)}, error={exc}"
                    )
                    summary["errors"].append(
                        {
                            "symbol": "__batch__",
                            "error": str(exc),
                            "offset": offset,
                            "batch_size": len(batch),
                        }
                    )
                    for board_name in batch:
                        if board_name not in summary["failed_boards"]:
                            summary["failed_boards"].append(board_name)
        summary["model_available"] = model is not None and self.db is not None
        return summary

    def sync_stock_concept_memberships(
        self,
        stock_codes: Sequence[str],
        trade_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Sync stock -> concept board memberships snapshot."""
        model = self._get_model("StockConceptMembershipDaily")
        summary = self._empty_summary(stock_codes)
        snapshot_date = self._to_date(trade_date) if trade_date else date.today()
        catalog = self._fetch_all_concept_board_catalog(
            snapshot_date=snapshot_date,
            include_ranked_boards=False,
            ranking_size=80,
        )
        concept_universe = self._build_concept_name_aliases([item["board_name"] for item in catalog])
        if len(concept_universe) < 100:
            concept_universe = set()

        ths_records, ths_meta = self._build_stock_concept_memberships_from_ths(
            stock_codes=stock_codes,
            snapshot_date=snapshot_date,
            board_catalog=catalog,
        )
        if ths_records:
            summary["fetched"] = int(ths_meta.get("fetched_boards", 0))
            summary["empty"] = int(ths_meta.get("empty_boards", 0))
            summary["records"] = len(ths_records)
            summary["errors"].extend(ths_meta.get("errors", []))
            with self._session_scope(model) as session:
                if session is None:
                    summary["deferred"] = len(ths_records)
                else:
                    trade_codes = sorted({record["code"] for record in ths_records})
                    if hasattr(session, "query") and trade_codes:
                        session.query(model).filter(model.code.in_(trade_codes), model.trade_date == snapshot_date).delete(
                            synchronize_session=False
                        )
                    saved = self._upsert_records(
                        session=session,
                        model=model,
                        records=ths_records,
                        unique_keys=["code", "trade_date", "board_code", "board_name"],
                    )
                    summary["saved"] += saved
            summary["model_available"] = model is not None and self.db is not None
            return summary

        with self._session_scope(model) as session:
            for stock_code in stock_codes:
                code = normalize_stock_code(stock_code)
                try:
                    boards = self._fetch_stock_concept_boards_from_efinance(code)
                    boards = self._prioritize_concept_boards(boards)
                    if not boards:
                        boards = self.fetcher_manager.get_stock_concept_boards(code)
                        boards = self._prioritize_concept_boards(boards)
                    if boards is None:
                        summary["empty"] += 1
                        continue

                    summary["fetched"] += 1
                    records: List[Dict[str, Any]] = []
                    for item in boards:
                        board_name = self._as_str(item.get("board_name") or item.get("name"))
                        if concept_universe and not self._board_name_in_concept_universe(board_name, concept_universe):
                            continue
                        records.append(
                            {
                                "code": code,
                                "trade_date": snapshot_date,
                                "board_name": board_name,
                                "board_code": self._normalize_board_code(
                                    item.get("board_code"),
                                    board_name,
                                ),
                                "is_primary": bool(item.get("is_primary", False)),
                            }
                        )

                    records = [row for row in records if row.get("board_name") and row.get("board_code")]
                    summary["records"] += len(records)
                    if session is None:
                        summary["deferred"] += len(records)
                        continue

                    if hasattr(session, "query"):
                        session.query(model).filter(model.code == code, model.trade_date == snapshot_date).delete()
                    saved = self._upsert_records(
                        session=session,
                        model=model,
                        records=records,
                        unique_keys=["code", "trade_date", "board_code", "board_name"],
                    )
                    summary["saved"] += saved
                except Exception as exc:
                    logger.warning(f"[QuantData] sync_stock_concept_memberships failed: {code}, error={exc}")
                    summary["errors"].append({"symbol": code, "error": str(exc)})
        summary["model_available"] = model is not None and self.db is not None
        return summary

    def _build_stock_concept_memberships_from_ths(
        self,
        *,
        stock_codes: Sequence[str],
        snapshot_date: date,
        board_catalog: Sequence[Dict[str, str]],
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Build stock -> concept memberships from THS concept detail pages."""
        normalized_codes = {
            normalize_stock_code(code)
            for code in stock_codes
            if self._as_str(code) and self._is_main_board_stock(normalize_stock_code(code))
        }
        metadata: Dict[str, Any] = {
            "fetched_boards": 0,
            "empty_boards": 0,
            "errors": [],
        }
        if not normalized_codes or not board_catalog:
            return [], metadata

        raw_records: List[Dict[str, Any]] = []
        preferred_board_by_code: Dict[str, Tuple[Tuple[float, int, int, int, str], str, str]] = {}
        seen: set = set()
        for board in board_catalog:
            board_name = self._as_str(board.get("board_name"))
            board_code = self._normalize_board_code(board.get("board_code"), board_name)
            if not board_name or not board_code:
                continue
            try:
                snapshot = self._fetch_concept_board_snapshot_from_ths(board_name=board_name, board_code=board_code)
            except Exception as exc:
                logger.warning(f"[QuantData] THS concept snapshot failed: {board_name}, error={exc}")
                metadata["errors"].append({"symbol": board_name, "error": str(exc)})
                continue
            constituents = snapshot.get("constituents", [])
            if not constituents:
                metadata["empty_boards"] += 1
                continue
            metadata["fetched_boards"] += 1
            priority = self._ths_board_priority(snapshot)
            for item in constituents:
                code = normalize_stock_code(str(item.get("code") or ""))
                if code not in normalized_codes or not self._is_main_board_stock(code):
                    continue
                key = (code, snapshot_date, board_code, board_name)
                if key in seen:
                    continue
                seen.add(key)
                raw_records.append(
                    {
                        "code": code,
                        "trade_date": snapshot_date,
                        "board_name": board_name,
                        "board_code": board_code,
                        "is_primary": False,
                    }
                )
                current = preferred_board_by_code.get(code)
                if current is None or priority > current[0]:
                    preferred_board_by_code[code] = (priority, board_code, board_name)

        if not raw_records:
            return [], metadata

        for record in raw_records:
            preferred = preferred_board_by_code.get(record["code"])
            record["is_primary"] = bool(
                preferred
                and preferred[1] == record["board_code"]
                and preferred[2] == record["board_name"]
            )
        return raw_records, metadata

    @staticmethod
    def _empty_summary(symbols: Iterable[str]) -> Dict[str, Any]:
        requested = list(symbols)
        return {
            "requested": len(requested),
            "fetched": 0,
            "empty": 0,
            "records": 0,
            "saved": 0,
            "deferred": 0,
            "errors": [],
            "model_available": False,
        }

    def _get_model(self, model_name: str) -> Optional[Type[Any]]:
        if model_name in self._model_cache:
            return self._model_cache[model_name]
        try:
            from src import storage

            model = getattr(storage, model_name, None)
            self._model_cache[model_name] = model
            if model is None:
                logger.info(f"[QuantData] model unavailable, deferred writes: {model_name}")
            return model
        except Exception as exc:
            logger.warning(f"[QuantData] failed to load model: {model_name}, error={exc}")
            self._model_cache[model_name] = None
            return None

    @contextmanager
    def _session_scope(self, model: Optional[Type[Any]]):
        if model is None or self.db is None:
            yield None
            return

        if hasattr(self.db, "session_scope"):
            with self.db.session_scope() as session:
                yield session
            return

        session = self.db.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def _column_names(model: Type[Any]) -> set:
        table = getattr(model, "__table__", None)
        if table is None:
            return set()
        return {col.name for col in table.columns}

    def _resolve_latest_stock_date(self) -> Optional[date]:
        stock_daily_model = self._get_model("StockDaily")
        if self.db is None or stock_daily_model is None:
            return None
        with self.db.get_session() as session:
            row = session.query(stock_daily_model.date).order_by(stock_daily_model.date.desc()).first()
            return row[0] if row else None

    def _resolve_earliest_stock_date(self) -> Optional[date]:
        stock_daily_model = self._get_model("StockDaily")
        if self.db is None or stock_daily_model is None:
            return None
        with self.db.get_session() as session:
            row = session.query(stock_daily_model.date).order_by(stock_daily_model.date.asc()).first()
            return row[0] if row else None

    def _resolve_default_stock_pool(self) -> List[str]:
        """Resolve default main-board stock pool with fetcher-first fallback."""
        pool = self._list_stock_pool_from_directory()
        if pool:
            return pool
        pool = self._list_stock_pool_from_current_a_share_list()
        if pool:
            return pool
        pool = self._list_stock_pool_from_fetchers()
        if pool:
            return pool
        return self._list_stock_pool()

    def _list_stock_pool_from_current_a_share_list(self) -> List[str]:
        """Fetch the current A-share list and keep main-board codes only."""
        try:
            import akshare as ak

            stock_list = ak.stock_info_a_code_name()
            codes = self._extract_main_board_codes(stock_list)
            if codes:
                return codes
        except Exception as exc:
            logger.warning(f"[QuantData] fetch current A-share stock list failed: {exc}")
        return []

    def _list_stock_pool_from_fetchers(self) -> List[str]:
        """Fetch full-market stock list from fetchers and keep main-board only."""
        manager = self.fetcher_manager
        if manager is None:
            return []

        if hasattr(manager, "get_stock_list"):
            try:
                stock_list = manager.get_stock_list()
                codes = self._extract_main_board_codes(stock_list)
                if codes:
                    return codes
            except Exception as exc:
                logger.warning(f"[QuantData] fetch stock list from manager failed: {exc}")

        fetchers = list(getattr(manager, "_fetchers", []) or [])
        for fetcher in fetchers:
            get_stock_list = getattr(fetcher, "get_stock_list", None)
            if not callable(get_stock_list):
                continue
            try:
                stock_list = get_stock_list()
                codes = self._extract_main_board_codes(stock_list)
                if codes:
                    return codes
            except Exception as exc:
                fetcher_name = getattr(fetcher, "name", fetcher.__class__.__name__)
                logger.warning(f"[QuantData] fetch stock list from {fetcher_name} failed: {exc}")
        return []

    def _extract_main_board_codes(self, stock_list: Any) -> List[str]:
        """Normalize stock list payload and return deduplicated main-board codes."""
        if stock_list is None:
            return []

        raw_codes: List[Any] = []
        if isinstance(stock_list, pd.DataFrame):
            code_column = next(
                (col for col in ["code", "ts_code", "symbol", "股票代码", "代码"] if col in stock_list.columns),
                None,
            )
            if code_column is None:
                return []
            raw_codes = stock_list[code_column].tolist()
        elif isinstance(stock_list, (list, tuple, set)):
            raw_codes = list(stock_list)
        elif isinstance(stock_list, dict):
            raw_codes = list(stock_list.keys())
        else:
            return []

        pool: set = set()
        for raw_code in raw_codes:
            text = self._as_str(raw_code)
            if not text:
                continue
            code = normalize_stock_code(text)
            if code and self._is_main_board_stock(code):
                pool.add(code)
        return sorted(pool)

    def _list_stock_pool(self) -> List[str]:
        stock_daily_model = self._get_model("StockDaily")
        if self.db is None or stock_daily_model is None:
            return []
        with self.db.get_session() as session:
            rows = session.query(stock_daily_model.code).distinct().order_by(stock_daily_model.code.asc()).all()
        return [
            normalize_stock_code(row[0])
            for row in rows
            if row and row[0] and self._is_main_board_stock(str(row[0]))
        ]

    def _list_stock_pool_from_directory(self) -> List[str]:
        """Read default stock pool from the local stock directory cache first."""
        stock_directory_model = self._get_model("StockDirectory")
        if self.db is None or stock_directory_model is None:
            return []
        if not hasattr(self.db, "get_session"):
            return []
        with self.db.get_session() as session:
            rows = (
                session.query(stock_directory_model.code)
                .filter(stock_directory_model.is_main_board.is_(True))
                .order_by(stock_directory_model.code.asc())
                .all()
            )
        return [normalize_stock_code(row[0]) for row in rows if row and row[0]]

    def _fetch_stock_directory_frame(self) -> Tuple[pd.DataFrame, Optional[str]]:
        """Fetch stock directory data from lightweight bulk sources."""
        try:
            import akshare as ak

            stock_list = ak.stock_info_a_code_name()
            if isinstance(stock_list, pd.DataFrame) and not stock_list.empty:
                return stock_list, "Akshare"
        except Exception as exc:
            logger.warning(f"[QuantData] fetch stock directory from Akshare failed: {exc}")

        fetchers = list(getattr(self.fetcher_manager, "_fetchers", []) or [])
        preferred_order = {
            "BaostockFetcher": 0,
            "AkshareFetcher": 1,
            "PytdxFetcher": 2,
            "TushareFetcher": 3,
        }
        sorted_fetchers = sorted(
            fetchers,
            key=lambda fetcher: preferred_order.get(getattr(fetcher, "name", fetcher.__class__.__name__), 99),
        )
        for fetcher in sorted_fetchers:
            get_stock_list = getattr(fetcher, "get_stock_list", None)
            if not callable(get_stock_list):
                continue
            fetcher_name = getattr(fetcher, "name", fetcher.__class__.__name__)
            try:
                stock_list = get_stock_list()
                if isinstance(stock_list, pd.DataFrame) and not stock_list.empty:
                    return stock_list, fetcher_name
            except Exception as exc:
                logger.warning(f"[QuantData] fetch stock directory from {fetcher_name} failed: {exc}")
        return pd.DataFrame(), None

    def _normalize_stock_directory_frame(self, stock_frame: pd.DataFrame, *, data_source: Optional[str]) -> pd.DataFrame:
        """Normalize stock directory payload into a stable local master frame."""
        if stock_frame is None or stock_frame.empty:
            return pd.DataFrame(
                columns=["code", "name", "exchange", "market", "is_main_board", "list_status", "data_source"]
            )

        code_column = next(
            (column for column in ["code", "ts_code", "symbol", "股票代码", "代码"] if column in stock_frame.columns),
            None,
        )
        name_column = next(
            (column for column in ["name", "code_name", "股票简称", "股票名称", "名称"] if column in stock_frame.columns),
            None,
        )
        if code_column is None or name_column is None:
            return pd.DataFrame(
                columns=["code", "name", "exchange", "market", "is_main_board", "list_status", "data_source"]
            )

        normalized_rows: List[Dict[str, Any]] = []
        seen_codes: set = set()
        for _, row in stock_frame.iterrows():
            code = normalize_stock_code(self._as_str(row.get(code_column)))
            name = self._as_str(row.get(name_column))
            if not code or not name or code in seen_codes:
                continue
            seen_codes.add(code)
            normalized_rows.append(
                {
                    "code": code,
                    "name": name,
                    "exchange": self._infer_exchange(code),
                    "market": "A-share",
                    "is_main_board": self._is_main_board_stock(code),
                    "list_status": "listed",
                    "data_source": data_source,
                }
            )
        return pd.DataFrame(normalized_rows)

    @staticmethod
    def _infer_exchange(code: str) -> str:
        """Infer exchange label from a normalized security code."""
        normalized_code = str(code or "").strip()
        bse_prefixes = (
            "430",
            "831",
            "832",
            "833",
            "835",
            "836",
            "837",
            "838",
            "839",
            "870",
            "871",
            "872",
            "873",
            "874",
            "875",
            "876",
            "877",
            "878",
            "879",
            "880",
            "881",
            "882",
            "883",
            "884",
            "885",
            "886",
            "887",
            "888",
            "889",
            "920",
        )
        if normalized_code.startswith(("600", "601", "603", "605")):
            return "SSE"
        if normalized_code.startswith(("000", "001", "002", "003")):
            return "SZSE"
        if normalized_code.startswith(bse_prefixes):
            return "BSE"
        if normalized_code.startswith("688"):
            return "STAR"
        if normalized_code.startswith(("300", "301")):
            return "GEM"
        return "UNKNOWN"

    def _resolve_model_field(self, model_name: str, field_candidates: Sequence[str]) -> Tuple[Optional[Type[Any]], Optional[Any]]:
        model = self._get_model(model_name)
        if model is None:
            return None, None
        for field_name in field_candidates:
            if hasattr(model, field_name):
                return model, getattr(model, field_name)
        return model, None

    def _query_model_edge_date(self, *, model_name: str, date_field: str, latest: bool) -> Optional[date]:
        model, date_column = self._resolve_model_field(model_name, [date_field])
        if self.db is None or model is None or date_column is None:
            return None
        try:
            with self.db.get_session() as session:
                order_expr = date_column.desc() if latest else date_column.asc()
                row = session.query(date_column).order_by(order_expr).first()
            if not row or row[0] is None:
                return None
            return self._to_date(row[0])
        except Exception as exc:
            logger.warning(f"[QuantData] query edge date failed: model={model_name}, field={date_field}, error={exc}")
            return None

    def _query_model_distinct_count(self, *, model_name: str, field_candidates: Sequence[str]) -> Optional[int]:
        model, column = self._resolve_model_field(model_name, field_candidates)
        if self.db is None or model is None or column is None:
            return None
        try:
            with self.db.get_session() as session:
                query = session.query(column).distinct()
                try:
                    return int(query.count())
                except Exception:
                    rows = query.all()
                    return len(rows)
        except Exception as exc:
            logger.warning(
                f"[QuantData] query distinct count failed: model={model_name}, fields={list(field_candidates)}, error={exc}"
            )
            return None

    def _query_model_date_count(self, *, model_name: str, date_field: str, target_date: Optional[date]) -> Optional[int]:
        model, date_column = self._resolve_model_field(model_name, [date_field])
        if self.db is None or model is None or date_column is None or target_date is None:
            return None
        try:
            with self.db.get_session() as session:
                query = session.query(model).filter(date_column == target_date)
                return int(query.count())
        except Exception as exc:
            logger.warning(
                f"[QuantData] query date count failed: model={model_name}, field={date_field}, date={target_date}, error={exc}"
            )
            return None

    def _list_board_names_for_date(self, trade_date: date) -> List[str]:
        membership_model = self._get_model("StockConceptMembershipDaily")
        if self.db is None or membership_model is None:
            return []
        with self.db.get_session() as session:
            rows = (
                session.query(membership_model.board_name)
                .filter(membership_model.trade_date == trade_date)
                .distinct()
                .all()
            )
        return [self._as_str(row[0]) for row in rows if row and self._as_str(row[0])]

    def _fetch_ranked_board_names(self, ranking_size: int = 80) -> List[str]:
        names: List[str] = []
        try:
            import akshare as ak

            summary_df = ak.stock_board_concept_summary_ths()
            if summary_df is not None and not summary_df.empty:
                name_col = next((col for col in ["概念名称", "name", "板块名称"] if col in summary_df.columns), None)
                if name_col:
                    for value in summary_df[name_col].tolist():
                        name = self._as_str(value)
                        if name:
                            names.append(name)
        except Exception as exc:
            logger.warning(f"[QuantData] fetch THS ranked concept boards failed: {exc}")
        if names:
            return names[: max(int(ranking_size), 1)]
        try:
            ranking = self.fetcher_manager.get_concept_board_rankings(n=ranking_size)
            if not ranking:
                return names
            top_list, bottom_list = ranking
            for item in list(top_list) + list(bottom_list):
                name = self._as_str(item.get("name") if isinstance(item, dict) else None)
                if name:
                    names.append(name)
        except Exception as exc:
            logger.warning(f"[QuantData] fetch concept ranking boards failed: {exc}")
        return names

    def _fetch_all_concept_board_catalog(
        self,
        *,
        snapshot_date: Optional[date] = None,
        include_ranked_boards: bool = True,
        ranking_size: int = 80,
    ) -> List[Dict[str, str]]:
        """Fetch a canonical concept-board catalog, preferring THS concept names and codes."""
        if self._ths_board_catalog_cache:
            catalog = list(self._ths_board_catalog_cache)
        else:
            catalog = self._fetch_concept_board_catalog_from_ths()
            if catalog:
                self._ths_board_catalog_cache = list(catalog)

        merged: Dict[str, Dict[str, str]] = {
            item["board_name"]: {"board_name": item["board_name"], "board_code": item["board_code"]}
            for item in catalog
            if item.get("board_name") and item.get("board_code")
        }

        if not merged:
            for item in self._fetch_concept_board_catalog_from_em():
                name = self._as_str(item.get("board_name"))
                board_code = self._normalize_board_code(item.get("board_code"), name)
                if name and board_code:
                    merged.setdefault(name, {"board_name": name, "board_code": board_code})

        if include_ranked_boards:
            for name in self._fetch_ranked_board_names(ranking_size=ranking_size):
                board_name = self._as_str(name)
                if board_name and board_name not in merged:
                    merged[board_name] = {
                        "board_name": board_name,
                        "board_code": self._normalize_board_code(None, board_name),
                    }

        if snapshot_date is not None:
            for name in self._list_board_names_for_date(snapshot_date):
                board_name = self._as_str(name)
                if board_name and board_name not in merged:
                    merged[board_name] = {
                        "board_name": board_name,
                        "board_code": self._normalize_board_code(None, board_name),
                    }

        return sorted(merged.values(), key=lambda item: item["board_name"])

    def _fetch_all_concept_board_names(
        self,
        *,
        snapshot_date: Optional[date] = None,
        include_ranked_boards: bool = True,
        ranking_size: int = 80,
    ) -> List[str]:
        """Fetch full-market concept board names using the canonical concept catalog."""
        return [
            item["board_name"]
            for item in self._fetch_all_concept_board_catalog(
                snapshot_date=snapshot_date,
                include_ranked_boards=include_ranked_boards,
                ranking_size=ranking_size,
            )
            if item.get("board_name")
        ]

    def _fetch_concept_board_catalog_from_ths(self) -> List[Dict[str, str]]:
        """Fetch THS concept board catalog with stable name/code pairs."""
        try:
            import akshare as ak
        except Exception:
            return []
        try:
            board_df = ak.stock_board_concept_name_ths()
        except Exception as exc:
            logger.warning(f"[QuantData] fetch THS concept catalog failed: {exc}")
            return []
        if board_df is None or board_df.empty:
            return []
        name_col = next((col for col in ["name", "概念名称", "板块名称"] if col in board_df.columns), None)
        code_col = next((col for col in ["code", "概念代码", "板块代码"] if col in board_df.columns), None)
        if name_col is None:
            return []
        rows: List[Dict[str, str]] = []
        for _, row in board_df.iterrows():
            board_name = self._as_str(row.get(name_col))
            board_code = self._normalize_board_code(row.get(code_col) if code_col else None, board_name)
            if board_name and board_code:
                rows.append({"board_name": board_name, "board_code": board_code})
        return rows

    def _lookup_ths_board_code(self, board_name: str) -> Optional[str]:
        """Resolve a THS board code from the cached catalog when available."""
        target = self._as_str(board_name)
        if not target:
            return None
        catalog = self._ths_board_catalog_cache or self._fetch_concept_board_catalog_from_ths()
        if catalog and not self._ths_board_catalog_cache:
            self._ths_board_catalog_cache = list(catalog)
        for item in catalog:
            if item.get("board_name") == target and item.get("board_code"):
                return str(item["board_code"])
        return None

    def _fetch_concept_board_catalog_from_em(self) -> List[Dict[str, str]]:
        """Fetch Eastmoney concept board catalog as a fallback source."""
        try:
            import akshare as ak
        except Exception:
            return []
        try:
            board_df = ak.stock_board_concept_name_em()
        except Exception as exc:
            logger.warning(f"[QuantData] fetch EM concept catalog failed: {exc}")
            return []
        if board_df is None or board_df.empty:
            return []
        name_col = next((col for col in ["板块名称", "板块", "name"] if col in board_df.columns), None)
        code_col = next((col for col in ["板块代码", "代码", "code"] if col in board_df.columns), None)
        if name_col is None:
            return []
        rows: List[Dict[str, str]] = []
        for _, row in board_df.iterrows():
            board_name = self._as_str(row.get(name_col))
            board_code = self._normalize_board_code(row.get(code_col) if code_col else None, board_name)
            if board_name and board_code:
                rows.append({"board_name": board_name, "board_code": board_code})
        return rows

    @staticmethod
    def _build_concept_name_aliases(board_names: Sequence[str]) -> set:
        """Build a tolerant name alias set for concept board membership filtering."""
        aliases: set = set()
        for raw_name in board_names:
            name = str(raw_name or "").strip()
            if not name:
                continue
            aliases.add(name)
            if name.endswith("概念"):
                aliases.add(name[:-2].strip())
            else:
                aliases.add(f"{name}概念")
        return aliases

    def _board_name_in_concept_universe(self, board_name: Optional[str], concept_universe: set) -> bool:
        """Return True when a board name is recognized by the current concept board universe."""
        name = self._as_str(board_name)
        if not name:
            return False
        if name in concept_universe:
            return True
        if name.endswith("概念") and name[:-2].strip() in concept_universe:
            return True
        if f"{name}概念" in concept_universe:
            return True
        return False

    def _prioritize_concept_boards(self, boards: Optional[List[Dict[str, Any]]]) -> Optional[List[Dict[str, Any]]]:
        """Filter mixed board memberships and keep concept-like items first."""
        if boards is None:
            return None
        if not boards:
            return []
        scored: List[Tuple[int, Dict[str, Any]]] = []
        for item in boards:
            board_name = self._as_str(item.get("board_name") or item.get("name"))
            board_type = self._as_str(item.get("board_type") or item.get("type") or item.get("板块类型"))
            if not board_name:
                continue
            score = self._concept_score(board_name=board_name, board_type=board_type)
            if score < 0:
                continue
            scored.append((score, item))
        if not scored:
            return []
        scored.sort(key=lambda pair: pair[0], reverse=True)
        top_score = scored[0][0]
        if top_score <= 0:
            return []
        return [item for score, item in scored if score >= 2 or score == top_score]

    @staticmethod
    def _ths_board_priority(snapshot: Dict[str, Any]) -> Tuple[float, int, int, int, str]:
        """Build a stable priority tuple for picking a stock's primary THS concept."""
        constituents = snapshot.get("constituents", []) or []
        return (
            float(snapshot.get("board_pct_chg", 0.0) or 0.0),
            int(snapshot.get("strong_stock_count", 0) or 0),
            int(snapshot.get("limit_up_count", 0) or 0),
            len(constituents),
            str(snapshot.get("board_name", "") or ""),
        )

    def _concept_score(self, *, board_name: str, board_type: Optional[str]) -> int:
        """Heuristic concept-board scoring for mixed membership results."""
        name = board_name.strip()
        if not name:
            return -1
        score = 0
        if board_type and "概念" in board_type:
            score += 3
        if "概念" in name:
            score += 3
        if self._is_region_noise(name):
            return -2
        if any(token in name for token in self.CONCEPT_EXCLUDE_KEYWORDS) and "概念" not in name:
            return -2
        score += sum(1 for token in self.CONCEPT_INCLUDE_KEYWORDS if token in name)
        return score

    def _fetch_concept_board_snapshot_from_ths(self, *, board_name: str, board_code: str) -> Dict[str, Any]:
        """Fetch a THS concept board detail page and parse current constituents plus summary stats."""
        url = f"https://q.10jqka.com.cn/gn/detail/code/{board_code}/"
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        response.raise_for_status()
        html = response.text
        info_map = self._parse_ths_board_info_map(html)
        constituent_df = self._parse_ths_board_constituent_table(html)
        records: List[Dict[str, Any]] = []
        for _, row in constituent_df.iterrows():
            code = self._normalize_member_stock_code(row.get("代码"))
            if not code:
                continue
            records.append(
                {
                    "code": code,
                    "name": self._as_str(row.get("名称")) or code,
                    "pct_chg": self._parse_pct_value(row.get("涨跌幅(%)")),
                }
            )
        strong_count = sum(1 for item in records if float(item.get("pct_chg") or 0.0) >= 5.0)
        limit_up_count = sum(1 for item in records if float(item.get("pct_chg") or 0.0) >= 9.8)
        board_pct_chg = self._parse_pct_value(info_map.get("板块涨幅"))
        up_count, down_count = self._parse_up_down_counts(info_map.get("涨跌家数"))
        amount_value = self._parse_amount_in_yi(info_map.get("成交额(亿)"))
        return {
            "board_name": board_name,
            "board_code": board_code,
            "board_pct_chg": board_pct_chg,
            "up_count": up_count,
            "down_count": down_count,
            "amount": amount_value,
            "strong_stock_count": strong_count,
            "limit_up_count": limit_up_count,
            "constituents": records,
        }

    @staticmethod
    def _parse_ths_board_constituent_table(html: str) -> pd.DataFrame:
        """Extract the constituent table from a THS concept detail page."""
        try:
            tables = pd.read_html(StringIO(html))
        except Exception:
            return pd.DataFrame()
        for table in tables:
            if {"代码", "名称"}.issubset(set(table.columns)):
                return table.copy()
        return pd.DataFrame()

    @staticmethod
    def _parse_ths_board_info_map(html: str) -> Dict[str, str]:
        """Extract the summary key-value pairs from a THS concept detail page."""
        try:
            from bs4 import BeautifulSoup
        except Exception:
            return {}
        soup = BeautifulSoup(html, features="lxml")
        block = soup.find(name="div", attrs={"class": "board-infos"})
        if block is None:
            return {}
        keys = [item.get_text(strip=True) for item in block.find_all("dt")]
        values = [item.get_text(strip=True).replace("\n", "/") for item in block.find_all("dd")]
        return {key: value for key, value in zip(keys, values) if key}

    @staticmethod
    def _normalize_member_stock_code(value: Any) -> Optional[str]:
        """Normalize a stock identifier from THS constituent tables."""
        text = re.sub(r"[^0-9A-Za-z]", "", str(value or "").strip())
        if not text:
            return None
        if text.isdigit() and len(text) < 6:
            text = text.zfill(6)
        normalized = normalize_stock_code(text)
        return normalized if normalized else None

    @staticmethod
    def _parse_pct_value(value: Any) -> float:
        """Parse percent-like text into a numeric percentage value."""
        text = str(value or "").strip().replace("%", "")
        try:
            return float(text)
        except Exception:
            return 0.0

    @staticmethod
    def _parse_up_down_counts(value: Any) -> Tuple[int, int]:
        """Parse THS rise/fall counts like '337/44'."""
        text = str(value or "").strip()
        if "/" not in text:
            return 0, 0
        left, right = text.split("/", 1)
        try:
            return int(left), int(right)
        except Exception:
            return 0, 0

    @staticmethod
    def _parse_amount_in_yi(value: Any) -> float:
        """Parse THS amount text in 亿 to absolute numeric amount."""
        text = str(value or "").strip().replace("亿", "")
        try:
            return float(text) * 1e8
        except Exception:
            return 0.0

    def _is_region_noise(self, board_name: str) -> bool:
        provinces = (
            "北京",
            "上海",
            "广东",
            "江苏",
            "浙江",
            "山东",
            "福建",
            "四川",
            "湖北",
            "湖南",
            "安徽",
            "河北",
            "河南",
            "陕西",
            "辽宁",
            "吉林",
            "黑龙江",
            "天津",
            "重庆",
            "海南",
            "云南",
            "贵州",
            "广西",
            "内蒙古",
            "新疆",
            "西藏",
            "青海",
            "宁夏",
            "甘肃",
            "江西",
            "山西",
        )
        return any(board_name.startswith(p) and board_name.endswith(s) for p in provinces for s in self.REGION_BOARD_SUFFIXES)

    @staticmethod
    def _normalize_board_code(board_code: Any, board_name: Any) -> Optional[str]:
        code = str(board_code).strip().upper() if board_code is not None else ""
        if code:
            return code[:32]
        name = str(board_name).strip() if board_name is not None else ""
        if not name:
            return None
        token = md5(name.encode("utf-8")).hexdigest()[:12].upper()
        return f"EM_{token}"

    def _board_name_merge_keys(self, board_name: Any) -> List[str]:
        """Build tolerant board-name keys for supplement fallback matching."""
        name = self._as_str(board_name)
        if not name or name == "未归类概念":
            return []
        aliases: List[str] = []
        for candidate in [name, *self.BOARD_NAME_ALIAS_MAP.get(name, ())]:
            text = self._as_str(candidate)
            if not text:
                continue
            aliases.append(text)
            aliases.append(text.replace(" ", ""))
            if text.endswith("概念"):
                trimmed = text[:-2].strip()
                if trimmed:
                    aliases.append(trimmed)
                    aliases.append(trimmed.replace(" ", ""))
            else:
                concept_name = f"{text}概念"
                aliases.append(concept_name)
                aliases.append(concept_name.replace(" ", ""))
        deduped: List[str] = []
        seen = set()
        for alias in aliases:
            if not alias or alias in seen:
                continue
            seen.add(alias)
            deduped.append(alias)
        return deduped

    @staticmethod
    def _board_supplement_rank(payload: Dict[str, Any]) -> Tuple[int, int, int, float]:
        """Rank supplement candidates so richer board samples win fallback selection."""
        return (
            int(payload.get("member_count") or 0),
            int(payload.get("strong_stock_count") or 0),
            int(payload.get("limit_up_count") or 0),
            float(payload.get("top5_avg_pct") or 0.0),
        )

    def _fetch_stock_concept_boards_from_efinance(self, stock_code: str) -> Optional[List[Dict[str, Any]]]:
        """Fallback concept membership fetcher based on efinance board API."""
        try:
            import efinance as ef
        except Exception:
            return None
        try:
            df = ef.stock.get_belong_board(stock_code)
        except Exception as exc:
            logger.warning(f"[QuantData] efinance get_belong_board failed: {stock_code}, error={exc}")
            return None
        if df is None or df.empty:
            return None
        out = df.copy()
        type_col = next((col for col in ["板块类型", "类别", "type"] if col in out.columns), None)
        if type_col:
            type_series = out[type_col].astype(str)
            concept_mask = type_series.str.contains("概念", na=False)
            if concept_mask.any():
                out = out[concept_mask]
        code_col = next((col for col in ["板块代码", "代码", "board_code"] if col in out.columns), None)
        name_col = next((col for col in ["板块名称", "名称", "board_name"] if col in out.columns), None)
        if name_col is None:
            return None
        records: List[Dict[str, Any]] = []
        for _, row in out.iterrows():
            board_name = self._as_str(row.get(name_col))
            board_type = self._as_str(row.get(type_col)) if type_col else None
            if self._concept_score(board_name=board_name or "", board_type=board_type) < 0:
                continue
            board_code = self._normalize_board_code(row.get(code_col) if code_col else None, board_name)
            if not board_name or not board_code:
                continue
            records.append(
                {
                    "board_code": board_code,
                    "board_name": board_name,
                    "is_primary": len(records) == 0,
                    "board_type": board_type,
                }
            )
        return self._prioritize_concept_boards(records)

    @staticmethod
    def _is_main_board_stock(code: str) -> bool:
        pure_code = normalize_stock_code(code)
        if len(pure_code) != 6 or not pure_code.isdigit():
            return False
        if pure_code.startswith(("300", "301", "688", "689", "8", "4", "92")):
            return False
        return pure_code.startswith(("000", "001", "002", "003", "600", "601", "603", "605"))

    @staticmethod
    def _as_str(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _to_date(value: Any) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            return datetime.strptime(value, "%Y-%m-%d").date()
        raise ValueError(f"Unsupported date value: {value}")

    def _upsert_records(
        self,
        session: Any,
        model: Type[Any],
        records: List[Dict[str, Any]],
        unique_keys: List[str],
    ) -> int:
        if not records:
            return 0

        columns = self._column_names(model)
        if not columns:
            return 0

        saved = 0
        for record in records:
            row = {k: v for k, v in record.items() if k in columns}
            if not row:
                continue

            filter_keys = [k for k in unique_keys if k in columns and row.get(k) is not None]
            existing = None
            if filter_keys:
                filters = {k: row[k] for k in filter_keys}
                existing = session.query(model).filter_by(**filters).one_or_none()

            if existing is None:
                session.add(model(**row))
            else:
                for key, value in row.items():
                    setattr(existing, key, value)
            saved += 1
        return saved

    def _build_index_records(self, index_code: str, df: pd.DataFrame) -> List[Dict[str, Any]]:
        out = df.copy()
        if "date" not in out.columns:
            out = out.rename(columns={"日期": "date"})
        if "close" not in out.columns:
            out = out.rename(columns={"收盘": "close"})
        if "date" not in out.columns or "close" not in out.columns:
            return []

        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out["close"] = pd.to_numeric(out["close"], errors="coerce")
        out = out.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
        if out.empty:
            return []

        out["ma5"] = out["close"].rolling(5, min_periods=1).mean()
        out["ma10"] = out["close"].rolling(10, min_periods=1).mean()
        out["ma20"] = out["close"].rolling(20, min_periods=1).mean()
        out["ma250"] = out["close"].rolling(250, min_periods=1).mean()
        up_flag = (out["close"].diff() > 0).astype(int)
        out["up_day_count_10"] = up_flag.rolling(10, min_periods=1).sum().astype(int)
        out["regime_score"] = (
            (out["close"] > out["ma250"]).astype(int)
            + ((out["ma5"] > out["ma10"]) & (out["ma10"] > out["ma20"])).astype(int)
            + (out["up_day_count_10"] >= 6).astype(int)
        )

        records: List[Dict[str, Any]] = []
        for _, row in out.iterrows():
            records.append(
                {
                    "index_code": index_code,
                    "trade_date": row["date"].date(),
                    "close": float(row["close"]),
                    "ma5": float(row["ma5"]),
                    "ma10": float(row["ma10"]),
                    "ma20": float(row["ma20"]),
                    "ma250": float(row["ma250"]),
                    "up_day_count_10": int(row["up_day_count_10"]),
                    "regime_score": int(row["regime_score"]),
                    "data_source": "AkshareFetcher",
                }
            )
        return records

    def _build_concept_board_records(self, board_name: str, df: pd.DataFrame) -> List[Dict[str, Any]]:
        out = df.copy()
        if "date" not in out.columns:
            out = out.rename(columns={"日期": "date"})
        if "open" not in out.columns:
            out = out.rename(columns={"开盘价": "open"})
        if "high" not in out.columns:
            out = out.rename(columns={"最高价": "high"})
        if "low" not in out.columns:
            out = out.rename(columns={"最低价": "low"})
        if "close" not in out.columns:
            out = out.rename(columns={"收盘价": "close"})
        if "volume" not in out.columns:
            out = out.rename(columns={"成交量": "volume"})
        if "pct_chg" not in out.columns:
            out = out.rename(columns={"涨跌幅": "pct_chg"})
        if "amount" not in out.columns:
            out = out.rename(columns={"成交额": "amount"})
        if "date" not in out.columns:
            return []

        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        if "close" in out.columns and "pct_chg" not in out.columns:
            out["close"] = pd.to_numeric(out["close"], errors="coerce")
            out["pct_chg"] = out["close"].pct_change().fillna(0.0) * 100.0
        out["pct_chg"] = pd.to_numeric(out.get("pct_chg"), errors="coerce")
        out["amount"] = pd.to_numeric(out.get("amount"), errors="coerce")
        out = out.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        if out.empty:
            return []

        records: List[Dict[str, Any]] = []
        board_code = self._normalize_board_code(
            out["board_code"].iloc[-1] if "board_code" in out.columns else None,
            board_name,
        )
        for _, row in out.iterrows():
            records.append(
                {
                    "board_code": board_code,
                    "board_name": board_name,
                    "trade_date": row["date"].date(),
                    "pct_chg": float(row["pct_chg"]) if pd.notna(row.get("pct_chg")) else None,
                    "amount": float(row["amount"]) if pd.notna(row.get("amount")) else None,
                    "data_source": "AkshareFetcher",
                }
            )
        return records

    def _fetch_concept_board_history_from_ths(
        self,
        *,
        board_name: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """Fallback board history fetcher based on THS concept index."""
        try:
            import akshare as ak
        except Exception:
            return None

        candidates = [board_name]
        if board_name.endswith("概念"):
            candidates.append(board_name[:-2])
        stripped = board_name.replace("概念", "").strip()
        if stripped and stripped not in candidates:
            candidates.append(stripped)

        start_fmt = (start_date or "").replace("-", "") or "20200101"
        end_fmt = (end_date or date.today().isoformat()).replace("-", "")
        for candidate in candidates:
            try:
                df = ak.stock_board_concept_index_ths(
                    symbol=candidate,
                    start_date=start_fmt,
                    end_date=end_fmt,
                )
                if df is not None and not df.empty:
                    out = df.copy()
                    out["board_name"] = board_name
                    out["board_code"] = self._normalize_board_code(self._lookup_ths_board_code(board_name), board_name)
                    return out
            except Exception:
                continue
        return None

    def _build_board_map(self, snapshot_rows: Sequence[Any]) -> Dict[str, Tuple[str, str]]:
        mapping: Dict[str, Tuple[str, str]] = {}
        for row in snapshot_rows:
            code = normalize_stock_code(getattr(row, "code", ""))
            board_name = self._as_str(getattr(row, "board_name", None))
            board_code = self._normalize_board_code(getattr(row, "board_code", None), board_name)
            if not code or not board_name or not board_code:
                continue
            if code not in mapping or bool(getattr(row, "is_primary", False)):
                mapping[code] = (board_code, board_name)
        return mapping

    def _build_board_map_by_trade_date(self, rows: Sequence[Any]) -> Dict[date, Dict[str, Tuple[str, str]]]:
        mapping_by_date: Dict[date, Dict[str, Tuple[str, str]]] = {}
        for row in rows:
            trade_date = getattr(row, "trade_date", None)
            if trade_date is None:
                continue
            trade_day = self._to_date(trade_date)
            day_mapping = mapping_by_date.setdefault(trade_day, {})
            code = normalize_stock_code(getattr(row, "code", ""))
            board_name = self._as_str(getattr(row, "board_name", None))
            board_code = self._normalize_board_code(getattr(row, "board_code", None), board_name)
            if not code or not board_name or not board_code:
                continue
            if code not in day_mapping or bool(getattr(row, "is_primary", False)):
                day_mapping[code] = (board_code, board_name)
        return mapping_by_date

    def _to_stock_frame(
        self,
        stock_rows: Sequence[Any],
        *,
        board_map: Dict[str, Tuple[str, str]],
        board_map_by_date: Optional[Dict[date, Dict[str, Tuple[str, str]]]] = None,
    ) -> pd.DataFrame:
        records: List[Dict[str, Any]] = []
        for row in stock_rows:
            code = normalize_stock_code(getattr(row, "code", ""))
            if not code:
                continue
            trade_day = self._to_date(getattr(row, "date"))
            day_board_map = board_map_by_date.get(trade_day, {}) if board_map_by_date else {}
            board_code, board_name = day_board_map.get(
                code,
                board_map.get(
                    code,
                    (
                        self._normalize_board_code(None, "未归类概念"),
                        "未归类概念",
                    ),
                ),
            )
            records.append(
                {
                    "code": code,
                    "trade_date": getattr(row, "date"),
                    "open": float(getattr(row, "open", 0.0) or 0.0),
                    "high": float(getattr(row, "high", 0.0) or 0.0),
                    "low": float(getattr(row, "low", 0.0) or 0.0),
                    "close": float(getattr(row, "close", 0.0) or 0.0),
                    "volume": float(getattr(row, "volume", 0.0) or 0.0),
                    "amount": float(getattr(row, "amount", 0.0) or 0.0),
                    "pct_chg": getattr(row, "pct_chg", None),
                    "board_code": board_code,
                    "board_name": board_name,
                }
            )
        df = pd.DataFrame(records)
        if df.empty:
            return df
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        df["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce")
        df = df.dropna(subset=["trade_date", "close"]).sort_values(["code", "trade_date"]).reset_index(drop=True)
        return df

    def _enrich_stock_frame(self, stock_df: pd.DataFrame) -> pd.DataFrame:
        if stock_df.empty:
            return stock_df
        frames: List[pd.DataFrame] = []
        for _, group in stock_df.groupby("code", sort=False):
            g = group.sort_values("trade_date").copy()
            close = g["close"]
            high = g["high"]
            low = g["low"]
            open_ = g["open"]
            amount = g["amount"]
            volume = g["volume"]

            if g["pct_chg"].isna().all():
                g["pct_chg"] = close.pct_change() * 100.0
            g["pct_chg"] = g["pct_chg"].fillna(0.0)

            g["ma5"] = close.rolling(5, min_periods=1).mean()
            g["ma10"] = close.rolling(10, min_periods=1).mean()
            g["ma20"] = close.rolling(20, min_periods=1).mean()
            g["ma60"] = close.rolling(60, min_periods=1).mean()
            g["ret20"] = close.pct_change(20).fillna(0.0) * 100.0
            g["ret60"] = close.pct_change(60).fillna(0.0) * 100.0
            g["ret5"] = close.pct_change(5).fillna(0.0) * 100.0
            g["median_amount_20"] = amount.rolling(20, min_periods=1).median().fillna(0.0)
            g["median_turnover_20"] = (amount / 1e8).rolling(20, min_periods=1).median().fillna(0.0)

            amount_ma5_prev = amount.rolling(5, min_periods=1).mean().shift(1)
            g["amount_ratio_5"] = (amount / amount_ma5_prev.replace(0, pd.NA)).fillna(1.0)
            g["weak_to_strong_amount_ratio"] = g["amount_ratio_5"]
            pullback_base = amount.rolling(5, min_periods=1).mean().shift(1)
            g["pullback_amount_ratio"] = (amount.rolling(3, min_periods=1).mean() / pullback_base.replace(0, pd.NA)).fillna(1.0)

            platform_high_prev = high.rolling(15, min_periods=5).max().shift(1)
            platform_low_prev = low.rolling(15, min_periods=5).min().shift(1)
            g["platform_width_pct"] = (
                (platform_high_prev - platform_low_prev) / platform_low_prev.replace(0, pd.NA) * 100.0
            ).fillna(0.0)
            g["breakout_pct"] = ((close - platform_high_prev) / platform_high_prev.replace(0, pd.NA) * 100.0).fillna(0.0)
            g["platform_high_prev"] = platform_high_prev.fillna(high.shift(1).fillna(high))
            g["platform_low_prev"] = platform_low_prev.fillna(g["ma20"])
            g["prev_high"] = high.shift(1).fillna(high)
            g["prev_low"] = low.shift(1).fillna(low)

            close_above_ma20 = (close > g["ma20"]).astype(float)
            g["close_above_ma20_ratio"] = close_above_ma20.rolling(10, min_periods=1).mean().fillna(0.0)

            spread = (high - low).replace(0, pd.NA)
            g["close_position_ratio"] = ((close - low) / spread).fillna(0.5).clip(lower=0.0, upper=1.0)
            g["upper_shadow_pct"] = ((high - pd.concat([open_, close], axis=1).max(axis=1)) / close.replace(0, pd.NA) * 100.0).fillna(0.0)

            max_close_prev5 = close.rolling(5, min_periods=2).max().shift(1)
            g["pullback_pct_5d"] = ((close - max_close_prev5) / max_close_prev5.replace(0, pd.NA) * 100.0).fillna(0.0)
            g["low_vs_ma20_pct"] = (low / g["ma20"].replace(0, pd.NA)).fillna(1.0)
            g["low_vs_ma60_pct"] = (low / g["ma60"].replace(0, pd.NA)).fillna(1.0)

            body = (close - open_).abs().replace(0, 0.01)
            lower_shadow = (pd.concat([open_, close], axis=1).min(axis=1) - low).clip(lower=0.0)
            g["lower_shadow_body_ratio"] = (lower_shadow / body).fillna(0.0)
            g["close_ge_open"] = close >= open_
            g["rebound_break_prev_high"] = close > high.shift(1).fillna(close)
            g["limit_up_count_5d"] = (g["pct_chg"] >= 9.8).rolling(5, min_periods=1).sum().fillna(0).astype(int)
            g["prev_close_below_ma5"] = close.shift(1).fillna(close) < g["ma5"].shift(1).fillna(g["ma5"])
            g["close_above_ma5"] = close >= g["ma5"]
            g["close_above_prev_high"] = close > high.shift(1).fillna(high)
            g["close_vs_ma5_pct"] = ((close / g["ma5"].replace(0, pd.NA)) - 1.0).fillna(0.0) * 100.0
            g["listed_days"] = range(1, len(g) + 1)

            g["amount_5d"] = amount.rolling(5, min_periods=1).mean().fillna(0.0)
            rolling_high20 = close.rolling(20, min_periods=1).max()
            g["is_new_high_20"] = (close >= rolling_high20).astype(int)
            g["breakout_count_3d"] = g["is_new_high_20"].rolling(3, min_periods=1).sum().fillna(0).astype(int)
            g["consecutive_new_high_3d"] = g["breakout_count_3d"]
            g["return_2d"] = close.pct_change(2).fillna(0.0) * 100.0
            g["leader_limit_up_3d"] = (g["pct_chg"] >= 9.8).rolling(3, min_periods=1).sum().fillna(0).astype(int)
            max_high_5 = close.rolling(5, min_periods=1).max()
            g["close_to_5d_high_drawdown_pct"] = ((max_high_5 - close) / max_high_5.replace(0, pd.NA) * 100.0).fillna(0.0)
            g["close_above_ma10"] = close >= g["ma10"]
            g["low_above_ma20"] = low >= g["ma20"]
            down_amount = amount.where(close.diff().fillna(0.0) < 0)
            g["pullback_volume_ratio"] = (
                down_amount.rolling(3, min_periods=1).mean() / amount.rolling(5, min_periods=1).mean().replace(0, pd.NA)
            ).fillna(1.0)
            g["single_day_drop_pct"] = g["pct_chg"]
            g["broke_ma10_with_volume"] = (close < g["ma10"]) & (amount > amount.rolling(5, min_periods=1).mean())
            g["broke_ma20"] = close < g["ma20"]
            g["is_limit_down"] = g["pct_chg"] <= -9.8
            g["above_ma60"] = close > g["ma60"]
            g["is_strong"] = g["pct_chg"] > 5.0
            frames.append(g)
        return pd.concat(frames, ignore_index=True)

    def _to_board_history_frame(self, board_rows: Sequence[Any]) -> pd.DataFrame:
        """Normalize persisted concept board history rows into a dataframe."""
        records: List[Dict[str, Any]] = []
        for row in board_rows:
            board_code = self._normalize_board_code(getattr(row, "board_code", None), getattr(row, "board_name", None))
            board_name = self._as_str(getattr(row, "board_name", None))
            trade_date = getattr(row, "trade_date", None)
            if not board_code or not board_name or trade_date is None or board_name == "未归类概念":
                continue
            records.append(
                {
                    "trade_date": pd.Timestamp(trade_date),
                    "board_code": board_code,
                    "board_name": board_name,
                    "pct_chg": float(getattr(row, "pct_chg", 0.0) or 0.0),
                    "amount": float(getattr(row, "amount", 0.0) or 0.0),
                }
            )
        frame = pd.DataFrame(records)
        if frame.empty:
            return frame
        frame = frame.sort_values(["board_code", "trade_date"]).drop_duplicates(
            subset=["trade_date", "board_code"], keep="last"
        )
        return frame.reset_index(drop=True)

    def _build_board_supplement_frame(self, enriched: pd.DataFrame) -> pd.DataFrame:
        """Build stock-sample-derived board supplement fields."""
        if enriched.empty:
            return pd.DataFrame()
        grouped = (
            enriched.groupby(["trade_date", "board_code", "board_name"], as_index=False)
            .agg(
                member_count=("code", "count"),
                strong_stock_count=("is_strong", "sum"),
                limit_up_count=("pct_chg", lambda s: int((s >= 9.8).sum())),
                limit_down_count=("pct_chg", lambda s: int((s <= -9.8).sum())),
                top5_avg_pct=("pct_chg", lambda s: float(s.nlargest(5).mean() if len(s) else 0.0)),
                big_drop_ratio=("pct_chg", lambda s: float((s <= -5.0).mean() if len(s) else 0.0)),
                member_fall20_ratio=("pct_chg", lambda s: float((s <= -2.0).mean() if len(s) else 0.0)),
            )
        )
        grouped["member_count"] = grouped["member_count"].fillna(0).astype(int)
        grouped["strong_stock_count"] = grouped["strong_stock_count"].fillna(0).astype(int)
        grouped["breadth_ratio"] = (
            grouped["strong_stock_count"] / grouped["member_count"].replace(0, pd.NA)
        ).fillna(0.0)
        grouped = grouped.sort_values(["board_code", "trade_date"]).reset_index(drop=True)
        grouped["prev_limit_up_count"] = grouped.groupby("board_code")["limit_up_count"].shift(1).fillna(0).astype(int)

        leader_rows: Dict[Tuple[pd.Timestamp, str], Dict[str, Any]] = {}
        for (trade_date, board_code), frame in enriched.groupby(["trade_date", "board_code"], sort=False):
            if frame.empty:
                continue
            ordered = frame.sort_values(
                by=["ret20", "amount_5d", "breakout_count_3d", "return_2d", "code"],
                ascending=[False, False, False, False, False],
            )
            leader = ordered.iloc[0]
            leader_rows[(trade_date, board_code)] = {
                "leader_stock_code": str(leader["code"]),
                "leader_stock_name": str(leader["code"]),
                "leader_2d_return": float(leader["return_2d"]),
                "leader_limit_up_3d": int(leader["leader_limit_up_3d"]),
                "leader_payload": {
                    "ret20": float(leader["ret20"]),
                    "amount_5d": float(leader["amount_5d"]),
                    "breakout_count_3d": int(leader["breakout_count_3d"]),
                    "consecutive_new_high_3d": int(leader["consecutive_new_high_3d"]),
                    "close_vs_ma5_pct": float(leader["close_vs_ma5_pct"]),
                    "close_above_ma10": bool(leader["close_above_ma10"]),
                    "low_above_ma20": bool(leader["low_above_ma20"]),
                    "pullback_volume_ratio": float(leader["pullback_volume_ratio"]),
                    "single_day_drop_pct": float(leader["single_day_drop_pct"]),
                    "broke_ma10_with_volume": bool(leader["broke_ma10_with_volume"]),
                    "broke_ma20": bool(leader["broke_ma20"]),
                    "is_limit_down": bool(leader["is_limit_down"]),
                    "close_to_5d_high_drawdown_pct": float(leader["close_to_5d_high_drawdown_pct"]),
                    "return_2d": float(leader["return_2d"]),
                    "limit_up_count_3d": int(leader["leader_limit_up_3d"]),
                },
            }
        rows: List[Dict[str, Any]] = []
        for row in grouped.itertuples(index=False):
            row_dict = row._asdict()
            row_dict.update(leader_rows.get((row.trade_date, row.board_code), {}))
            rows.append(row_dict)
        return pd.DataFrame(rows)

    def _build_board_frame_from_history(self, board_history: pd.DataFrame, board_supplement: pd.DataFrame) -> pd.DataFrame:
        """Build board features with market-wide board history as primary source."""
        if board_history.empty:
            return pd.DataFrame()
        base = board_history.copy()
        base["trade_date"] = pd.to_datetime(base["trade_date"], errors="coerce")
        base = base.dropna(subset=["trade_date"]).sort_values(["board_code", "trade_date"]).reset_index(drop=True)

        base["turnover_rank_pct"] = (
            base.groupby("trade_date")["amount"].rank(method="min", ascending=False, pct=True).fillna(1.0)
        )
        base["change_3d_pct"] = (
            base.groupby("board_code")["pct_chg"].rolling(3, min_periods=1).sum().reset_index(level=0, drop=True)
        )
        base["up_days_3d"] = (
            base.groupby("board_code")["pct_chg"].rolling(3, min_periods=1).apply(lambda s: float((s > 0).sum()))
            .reset_index(level=0, drop=True)
            .fillna(0)
            .astype(int)
        )

        supplement_cols = [
            "trade_date",
            "board_code",
            "board_name",
            "member_count",
            "strong_stock_count",
            "limit_up_count",
            "limit_down_count",
            "top5_avg_pct",
            "big_drop_ratio",
            "member_fall20_ratio",
            "breadth_ratio",
            "prev_limit_up_count",
            "leader_stock_code",
            "leader_stock_name",
            "leader_2d_return",
            "leader_limit_up_3d",
            "leader_payload",
        ]
        supplement = pd.DataFrame(columns=supplement_cols)
        if not board_supplement.empty:
            supplement = board_supplement[[col for col in supplement_cols if col in board_supplement.columns]].copy()
            supplement["trade_date"] = pd.to_datetime(supplement["trade_date"], errors="coerce")
        frame = base.merge(supplement, on=["trade_date", "board_code"], how="left", suffixes=("", "_supplement"))

        fallback_fields = [
            "member_count",
            "strong_stock_count",
            "limit_up_count",
            "limit_down_count",
            "top5_avg_pct",
            "big_drop_ratio",
            "member_fall20_ratio",
            "breadth_ratio",
            "prev_limit_up_count",
            "leader_stock_code",
            "leader_stock_name",
            "leader_2d_return",
            "leader_limit_up_3d",
            "leader_payload",
        ]
        if not supplement.empty and "board_name" in supplement.columns:
            fallback_lookup: Dict[Tuple[pd.Timestamp, str], Dict[str, Any]] = {}
            for row in supplement.itertuples(index=False):
                trade_ts = pd.Timestamp(getattr(row, "trade_date"))
                board_name = self._as_str(getattr(row, "board_name", None))
                if pd.isna(trade_ts) or not board_name:
                    continue
                payload = {field: getattr(row, field, None) for field in fallback_fields}
                for alias in self._board_name_merge_keys(board_name):
                    key = (trade_ts, alias)
                    current = fallback_lookup.get(key)
                    if current is None or self._board_supplement_rank(payload) > self._board_supplement_rank(current):
                        fallback_lookup[key] = payload

            missing_mask = frame["member_count"].isna() if "member_count" in frame.columns else pd.Series(dtype=bool)
            if not missing_mask.empty and missing_mask.any():
                for index in frame.index[missing_mask]:
                    trade_ts = pd.Timestamp(frame.at[index, "trade_date"])
                    board_name = self._as_str(frame.at[index, "board_name"])
                    candidate = None
                    for alias in self._board_name_merge_keys(board_name):
                        candidate = fallback_lookup.get((trade_ts, alias))
                        if candidate is not None:
                            break
                    if candidate is None:
                        continue
                    for field in fallback_fields:
                        frame.at[index, field] = candidate.get(field)

        frame["member_count"] = frame["member_count"].fillna(0).astype(int)
        frame["strong_stock_count"] = frame["strong_stock_count"].fillna(0).astype(int)
        frame["limit_up_count"] = frame["limit_up_count"].fillna(0).astype(int)
        frame["limit_down_count"] = frame["limit_down_count"].fillna(0).astype(int)
        frame["top5_avg_pct"] = frame["top5_avg_pct"].fillna(0.0)
        frame["big_drop_ratio"] = frame["big_drop_ratio"].fillna(1.0)
        frame["member_fall20_ratio"] = frame["member_fall20_ratio"].fillna(0.0)
        frame["breadth_ratio"] = frame["breadth_ratio"].fillna(0.0)
        frame["prev_limit_up_count"] = frame["prev_limit_up_count"].fillna(0).astype(int)

        from src.core.quant_features import (
            BoardLeaderSnapshot,
            ConceptBoardSnapshot,
            apply_stage_demotion,
            classify_board_stage,
            compute_theme_score,
        )

        theme_scores: List[int] = []
        consistency_scores: List[int] = []
        stages: List[str] = []
        payloads: List[str] = []
        for row in frame.itertuples(index=False):
            raw_leader_payload = getattr(row, "leader_payload", None)
            leader_payload = raw_leader_payload if isinstance(raw_leader_payload, dict) else {}
            leader = None
            if getattr(row, "leader_stock_code", None):
                leader = BoardLeaderSnapshot(
                    stock_code=str(row.leader_stock_code),
                    stock_name=str(getattr(row, "leader_stock_name", "") or ""),
                    ret20=float(leader_payload.get("ret20", 0.0)),
                    amount_5d=float(leader_payload.get("amount_5d", 0.0)),
                    breakout_count_3d=int(leader_payload.get("breakout_count_3d", 0)),
                    return_2d=float(leader_payload.get("return_2d", 0.0)),
                    limit_up_count_3d=int(leader_payload.get("limit_up_count_3d", 0)),
                    consecutive_new_high_3d=int(leader_payload.get("consecutive_new_high_3d", 0)),
                    close_vs_ma5_pct=float(leader_payload.get("close_vs_ma5_pct", 0.0)),
                    close_above_ma10=bool(leader_payload.get("close_above_ma10", False)),
                    low_above_ma20=bool(leader_payload.get("low_above_ma20", False)),
                    pullback_volume_ratio=float(leader_payload.get("pullback_volume_ratio", 1.0)),
                    single_day_drop_pct=float(leader_payload.get("single_day_drop_pct", 0.0)),
                    broke_ma10_with_volume=bool(leader_payload.get("broke_ma10_with_volume", False)),
                    broke_ma20=bool(leader_payload.get("broke_ma20", False)),
                    is_limit_down=bool(leader_payload.get("is_limit_down", False)),
                    close_to_5d_high_drawdown_pct=float(leader_payload.get("close_to_5d_high_drawdown_pct", 0.0)),
                )
            snapshot = ConceptBoardSnapshot(
                board_code=str(row.board_code),
                board_name=str(row.board_name),
                amount=float(row.amount or 0.0),
                turnover_rank_pct=float(row.turnover_rank_pct or 1.0),
                limit_up_count=int(row.limit_up_count or 0),
                strong_stock_count=int(row.strong_stock_count or 0),
                member_count=int(row.member_count or 0),
                strong_stock_ratio=float(row.breadth_ratio or 0.0),
                change_3d_pct=float(row.change_3d_pct or 0.0),
                up_days_3d=int(row.up_days_3d or 0),
                top5_avg_pct=float(row.top5_avg_pct or 0.0),
                big_drop_ratio=float(row.big_drop_ratio or 1.0),
                limit_down_count=int(row.limit_down_count or 0),
                leader=leader,
                prev_limit_up_count=int(row.prev_limit_up_count or 0),
                member_fall20_ratio=float(row.member_fall20_ratio or 0.0),
            )
            score = compute_theme_score(snapshot)
            stage = apply_stage_demotion(
                classify_board_stage(snapshot, theme_score=score.theme_score),
                snapshot,
                theme_score=score.theme_score,
            )
            theme_scores.append(int(score.theme_score))
            consistency_scores.append(int(score.consistency_score))
            stages.append(stage)
            payloads.append(
                json.dumps(
                    {
                        "member_count": int(row.member_count or 0),
                        "change_3d_pct": float(row.change_3d_pct or 0.0),
                        "up_days_3d": int(row.up_days_3d or 0),
                        "top5_avg_pct": float(row.top5_avg_pct or 0.0),
                        "big_drop_ratio": float(row.big_drop_ratio or 0.0),
                        "limit_down_count": int(row.limit_down_count or 0),
                        "prev_limit_up_count": int(row.prev_limit_up_count or 0),
                        "member_fall20_ratio": float(row.member_fall20_ratio or 0.0),
                        "leader": leader_payload,
                    },
                    ensure_ascii=False,
                )
            )
        frame["theme_score"] = theme_scores
        frame["consistency_score"] = consistency_scores
        frame["stage"] = stages
        frame["raw_payload_json"] = payloads
        return frame

    def _build_board_frame(self, enriched: pd.DataFrame) -> pd.DataFrame:
        """Backward-compatible wrapper used by older test helpers."""
        if enriched.empty:
            return pd.DataFrame()
        supplement = self._build_board_supplement_frame(enriched)
        history_like = (
            enriched.groupby(["trade_date", "board_code", "board_name"], as_index=False)
            .agg(pct_chg=("pct_chg", "mean"), amount=("amount", "sum"))
        )
        return self._build_board_frame_from_history(history_like, supplement)

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        if value is None or pd.isna(value):
            return float(default)
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        if value is None or pd.isna(value):
            return int(default)
        try:
            return int(value)
        except (TypeError, ValueError):
            return int(default)

    def _build_board_feature_records(self, board_frame: pd.DataFrame) -> List[Dict[str, Any]]:
        if board_frame.empty:
            return []
        records: List[Dict[str, Any]] = []
        for row in board_frame.itertuples(index=False):
            records.append(
                {
                    "board_code": str(row.board_code),
                    "board_name": str(row.board_name),
                    "trade_date": pd.Timestamp(row.trade_date).date(),
                    "pct_chg": self._safe_float(row.pct_chg, 0.0),
                    "amount": self._safe_float(row.amount, 0.0),
                    "turnover_rank_pct": self._safe_float(row.turnover_rank_pct, 1.0),
                    "limit_up_count": self._safe_int(row.limit_up_count, 0),
                    "strong_stock_count": self._safe_int(row.strong_stock_count, 0),
                    "breadth_ratio": self._safe_float(row.breadth_ratio, 0.0),
                    "consistency_score": self._safe_float(row.consistency_score, 0.0),
                    "theme_score": self._safe_int(row.theme_score, 0),
                    "leader_stock_code": self._as_str(getattr(row, "leader_stock_code", None)),
                    "leader_stock_name": self._as_str(getattr(row, "leader_stock_name", None)),
                    "leader_2d_return": self._safe_float(getattr(row, "leader_2d_return", 0.0), 0.0),
                    "leader_limit_up_3d": self._safe_int(getattr(row, "leader_limit_up_3d", 0), 0),
                    "stage": str(row.stage),
                    "data_source": "QuantDataService",
                    "raw_payload_json": str(row.raw_payload_json),
                }
            )
        return records

    def _build_stock_feature_records(self, enriched: pd.DataFrame, board_frame: pd.DataFrame) -> List[Dict[str, Any]]:
        if enriched.empty:
            return []
        board_meta: Dict[Tuple[pd.Timestamp, str], Dict[str, Any]] = {}
        board_strong: Dict[Tuple[pd.Timestamp, str], int] = {}
        for row in board_frame.itertuples(index=False):
            key = (pd.Timestamp(row.trade_date), str(row.board_code))
            board_meta[key] = {"stage": str(row.stage), "theme_score": int(row.theme_score or 0)}
            board_strong[key] = int(row.strong_stock_count or 0)

        from src.core.quant_features import (
            STAGE_IGNORE,
            StockSetupSnapshot,
            choose_entry_module,
            passes_universe_filter,
        )

        records: List[Dict[str, Any]] = []
        for row in enriched.itertuples(index=False):
            trade_ts = pd.Timestamp(row.trade_date)
            board_key = (trade_ts, str(row.board_code))
            meta = board_meta.get(board_key, {"stage": STAGE_IGNORE, "theme_score": 0})
            stage = str(meta.get("stage", STAGE_IGNORE))
            strong_total = board_strong.get(board_key, 0)
            peer_confirm_count = max(int(strong_total) - (1 if bool(row.is_strong) else 0), 0)
            code = str(row.code)

            setup = StockSetupSnapshot(
                code=code,
                board_code=str(row.board_code),
                board_name=str(row.board_name),
                close=float(row.close),
                open=float(row.open),
                high=float(row.high),
                low=float(row.low),
                ma5=float(row.ma5),
                ma10=float(row.ma10),
                ma20=float(row.ma20),
                ma60=float(row.ma60),
                ret20=float(row.ret20),
                ret60=float(row.ret60),
                median_amount_20=float(row.median_amount_20),
                median_turnover_20=float(row.median_turnover_20),
                listed_days=int(row.listed_days),
                is_main_board=self._is_main_board_stock(code),
                is_st=False,
                is_suspended=False,
                close_above_ma20_ratio=float(row.close_above_ma20_ratio),
                platform_width_pct=float(row.platform_width_pct),
                breakout_pct=float(row.breakout_pct),
                amount_ratio_5=float(row.amount_ratio_5),
                close_position_ratio=float(row.close_position_ratio),
                upper_shadow_pct=float(row.upper_shadow_pct),
                peer_confirm_count=peer_confirm_count,
                pullback_pct_5d=float(row.pullback_pct_5d),
                pullback_amount_ratio=float(row.pullback_amount_ratio),
                low_vs_ma20_pct=float(row.low_vs_ma20_pct),
                low_vs_ma60_pct=float(row.low_vs_ma60_pct),
                lower_shadow_body_ratio=float(row.lower_shadow_body_ratio),
                close_ge_open=bool(row.close_ge_open),
                rebound_break_prev_high=bool(row.rebound_break_prev_high),
                ret5=float(row.ret5),
                limit_up_count_5d=int(row.limit_up_count_5d),
                prev_close_below_ma5=bool(row.prev_close_below_ma5),
                close_above_ma5=bool(row.close_above_ma5),
                close_above_prev_high=bool(row.close_above_prev_high),
                weak_to_strong_amount_ratio=float(row.weak_to_strong_amount_ratio),
                close_vs_ma5_pct=float(row.close_vs_ma5_pct),
            )
            eligible = passes_universe_filter(setup) and str(row.board_name) != "未归类概念"
            module = choose_entry_module(setup, stage=stage)
            initial_stop = self._compute_initial_stop_price(
                module=module,
                close=float(row.close),
                low=float(row.low),
                ma5=float(row.ma5),
                ma20=float(row.ma20),
                ma60=float(row.ma60),
                platform_low_prev=float(row.platform_low_prev) if pd.notna(row.platform_low_prev) else None,
            )
            score = self._compute_signal_score(
                eligible=eligible,
                module=module,
                ret20=float(row.ret20),
                ret60=float(row.ret60),
                amount_ratio_5=float(row.amount_ratio_5),
                theme_score=int(meta.get("theme_score", 0)),
            )
            raw_payload = {
                "open": float(row.open),
                "high": float(row.high),
                "low": float(row.low),
                "listed_days": int(row.listed_days),
                "is_main_board": self._is_main_board_stock(code),
                "is_st": False,
                "is_suspended": False,
                "close_above_ma20_ratio": float(row.close_above_ma20_ratio),
                "platform_width_pct": float(row.platform_width_pct),
                "breakout_pct": float(row.breakout_pct),
                "amount_ratio_5": float(row.amount_ratio_5),
                "close_position_ratio": float(row.close_position_ratio),
                "upper_shadow_pct": float(row.upper_shadow_pct),
                "peer_confirm_count": peer_confirm_count,
                "pullback_pct_5d": float(row.pullback_pct_5d),
                "pullback_amount_ratio": float(row.pullback_amount_ratio),
                "low_vs_ma20_pct": float(row.low_vs_ma20_pct),
                "low_vs_ma60_pct": float(row.low_vs_ma60_pct),
                "lower_shadow_body_ratio": float(row.lower_shadow_body_ratio),
                "close_ge_open": bool(row.close_ge_open),
                "rebound_break_prev_high": bool(row.rebound_break_prev_high),
                "ret5": float(row.ret5),
                "limit_up_count_5d": int(row.limit_up_count_5d),
                "prev_close_below_ma5": bool(row.prev_close_below_ma5),
                "close_above_ma5": bool(row.close_above_ma5),
                "close_above_prev_high": bool(row.close_above_prev_high),
                "weak_to_strong_amount_ratio": float(row.weak_to_strong_amount_ratio),
                "close_vs_ma5_pct": float(row.close_vs_ma5_pct),
                "platform_high": float(row.platform_high_prev) if pd.notna(row.platform_high_prev) else float(row.high),
                "platform_low": float(row.platform_low_prev) if pd.notna(row.platform_low_prev) else float(row.low),
                "prev_high": float(row.prev_high) if pd.notna(row.prev_high) else float(row.high),
                "prev_low": float(row.prev_low) if pd.notna(row.prev_low) else float(row.low),
                "initial_stop_price": float(initial_stop),
            }
            records.append(
                {
                    "code": code,
                    "trade_date": trade_ts.date(),
                    "board_code": str(row.board_code),
                    "board_name": str(row.board_name),
                    "close": float(row.close),
                    "ma5": float(row.ma5),
                    "ma10": float(row.ma10),
                    "ma20": float(row.ma20),
                    "ma60": float(row.ma60),
                    "ret20": float(row.ret20),
                    "ret60": float(row.ret60),
                    "median_amount_20": float(row.median_amount_20),
                    "median_turnover_20": float(row.median_turnover_20),
                    "above_ma60": bool(row.above_ma60),
                    "eligible_universe": bool(eligible),
                    "signal_score": float(score),
                    "trigger_module": module,
                    "stage": stage,
                    "raw_payload_json": json.dumps(raw_payload, ensure_ascii=False),
                }
            )
        return records

    @staticmethod
    def _compute_initial_stop_price(
        *,
        module: Optional[str],
        close: float,
        low: float,
        ma5: float,
        ma20: float,
        ma60: float,
        platform_low_prev: Optional[float],
    ) -> float:
        """Compute module-aware initial stop level for plan generation."""
        if module == "BREAKOUT":
            base = min(value for value in [platform_low_prev or ma20, low, ma20] if value and value > 0)
        elif module == "PULLBACK":
            candidates = [value for value in [ma20, ma60, low] if value and value > 0]
            base = min(candidates) if candidates else max(close * 0.95, 0.01)
        elif module == "LATE_WEAK_TO_STRONG":
            candidates = [value for value in [ma5, low] if value and value > 0]
            base = min(candidates) if candidates else max(close * 0.96, 0.01)
        else:
            candidates = [value for value in [ma20, low] if value and value > 0]
            base = min(candidates) if candidates else max(close * 0.95, 0.01)
        return round(max(base * 0.995, 0.01), 3)

    @staticmethod
    def _compute_signal_score(
        *,
        eligible: bool,
        module: Optional[str],
        ret20: float,
        ret60: float,
        amount_ratio_5: float,
        theme_score: int,
    ) -> float:
        """Compute base signal score used for candidate sorting."""
        score = 10.0
        score += max(min(ret20, 40.0), -20.0) * 0.45
        score += max(min(ret60, 60.0), -30.0) * 0.25
        score += max(min(amount_ratio_5, 3.0), 0.0) * 5.0
        score += max(min(theme_score, 4), 0) * 4.0
        if eligible:
            score += 22.0
        if module:
            score += 10.0
        return round(max(min(score, 100.0), 0.0), 2)
