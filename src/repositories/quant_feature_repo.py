# -*- coding: utf-8 -*-
"""Repository helpers for quant feature tables."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Type, TypeVar, Union

from sqlalchemy import and_, desc, select

from src.storage import (
    ConceptBoardDailyFeature,
    DatabaseManager,
    IndexDailyFeature,
    StockConceptMembershipDaily,
    StockDailyFeature,
)

TModel = TypeVar("TModel")
RowInput = Union[TModel, Dict[str, Any]]


class QuantFeatureRepository:
    """DB access helpers for quant feature tables."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    def upsert_index_features(self, rows: Sequence[RowInput[IndexDailyFeature]]) -> int:
        return self._upsert_many(
            model=IndexDailyFeature,
            rows=rows,
            key_fields=("index_code", "trade_date"),
            update_fields=(
                "close",
                "ma5",
                "ma10",
                "ma20",
                "ma250",
                "up_day_count_10",
                "regime_score",
                "data_source",
                "updated_at",
            ),
        )

    def upsert_concept_board_features(self, rows: Sequence[RowInput[ConceptBoardDailyFeature]]) -> int:
        return self._upsert_many(
            model=ConceptBoardDailyFeature,
            rows=rows,
            key_fields=("board_code", "trade_date"),
            update_fields=(
                "board_name",
                "pct_chg",
                "amount",
                "turnover_rank_pct",
                "limit_up_count",
                "strong_stock_count",
                "breadth_ratio",
                "consistency_score",
                "theme_score",
                "leader_stock_code",
                "leader_stock_name",
                "leader_2d_return",
                "leader_limit_up_3d",
                "stage",
                "data_source",
                "raw_payload_json",
                "updated_at",
            ),
        )

    def upsert_stock_features(self, rows: Sequence[RowInput[StockDailyFeature]]) -> int:
        return self._upsert_many(
            model=StockDailyFeature,
            rows=rows,
            key_fields=("code", "trade_date"),
            update_fields=(
                "board_code",
                "board_name",
                "close",
                "ma5",
                "ma10",
                "ma20",
                "ma60",
                "ret20",
                "ret60",
                "median_amount_20",
                "median_turnover_20",
                "above_ma60",
                "eligible_universe",
                "trigger_module",
                "signal_score",
                "stage",
                "raw_payload_json",
                "updated_at",
            ),
        )

    def upsert_stock_concept_memberships(self, rows: Sequence[RowInput[StockConceptMembershipDaily]]) -> int:
        return self._upsert_many(
            model=StockConceptMembershipDaily,
            rows=rows,
            key_fields=("code", "trade_date", "board_code"),
            update_fields=("board_name", "is_primary", "updated_at"),
        )

    def replace_stock_concept_memberships(
        self,
        rows: Sequence[RowInput[StockConceptMembershipDaily]],
        *,
        trade_date: date,
    ) -> int:
        with self.db.session_scope() as session:
            session.query(StockConceptMembershipDaily).filter(
                StockConceptMembershipDaily.trade_date == trade_date
            ).delete()
            objects = [self._as_model(StockConceptMembershipDaily, row) for row in rows]
            session.add_all(objects)
            return len(objects)

    def list_index_features(
        self,
        *,
        index_code: Optional[str] = None,
        trade_date: Optional[date] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: Optional[int] = None,
    ) -> List[IndexDailyFeature]:
        with self.db.get_session() as session:
            query = select(IndexDailyFeature)
            if index_code:
                query = query.where(IndexDailyFeature.index_code == index_code)
            if trade_date:
                query = query.where(IndexDailyFeature.trade_date == trade_date)
            if start_date:
                query = query.where(IndexDailyFeature.trade_date >= start_date)
            if end_date:
                query = query.where(IndexDailyFeature.trade_date <= end_date)
            query = query.order_by(IndexDailyFeature.trade_date, IndexDailyFeature.index_code)
            if limit:
                query = query.limit(limit)
            return list(session.execute(query).scalars().all())

    def list_concept_board_features(
        self,
        *,
        board_code: Optional[str] = None,
        trade_date: Optional[date] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: Optional[int] = None,
    ) -> List[ConceptBoardDailyFeature]:
        with self.db.get_session() as session:
            query = select(ConceptBoardDailyFeature)
            if board_code:
                query = query.where(ConceptBoardDailyFeature.board_code == board_code)
            if trade_date:
                query = query.where(ConceptBoardDailyFeature.trade_date == trade_date)
            if start_date:
                query = query.where(ConceptBoardDailyFeature.trade_date >= start_date)
            if end_date:
                query = query.where(ConceptBoardDailyFeature.trade_date <= end_date)
            query = query.order_by(
                desc(ConceptBoardDailyFeature.trade_date),
                desc(ConceptBoardDailyFeature.theme_score),
            )
            if limit:
                query = query.limit(limit)
            return list(session.execute(query).scalars().all())

    def list_stock_features(
        self,
        *,
        code: Optional[str] = None,
        trade_date: Optional[date] = None,
        eligible_only: bool = False,
        limit: Optional[int] = None,
    ) -> List[StockDailyFeature]:
        with self.db.get_session() as session:
            query = select(StockDailyFeature)
            if code:
                query = query.where(StockDailyFeature.code == code)
            if trade_date:
                query = query.where(StockDailyFeature.trade_date == trade_date)
            if eligible_only:
                query = query.where(StockDailyFeature.eligible_universe.is_(True))
            query = query.order_by(desc(StockDailyFeature.trade_date), desc(StockDailyFeature.signal_score))
            if limit:
                query = query.limit(limit)
            return list(session.execute(query).scalars().all())

    def list_stock_concept_memberships(
        self,
        *,
        code: Optional[str] = None,
        trade_date: Optional[date] = None,
        board_code: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[StockConceptMembershipDaily]:
        with self.db.get_session() as session:
            query = select(StockConceptMembershipDaily)
            if code:
                query = query.where(StockConceptMembershipDaily.code == code)
            if trade_date:
                query = query.where(StockConceptMembershipDaily.trade_date == trade_date)
            if board_code:
                query = query.where(StockConceptMembershipDaily.board_code == board_code)
            query = query.order_by(
                desc(StockConceptMembershipDaily.trade_date),
                StockConceptMembershipDaily.code,
                StockConceptMembershipDaily.board_code,
            )
            if limit:
                query = query.limit(limit)
            return list(session.execute(query).scalars().all())

    def list_board_features(self, *, trade_date: Optional[date] = None) -> List[ConceptBoardDailyFeature]:
        """Backward-compatible alias."""
        return self.list_concept_board_features(trade_date=trade_date)

    def list_stock_memberships(
        self,
        *,
        trade_date: Optional[date] = None,
        code: Optional[str] = None,
    ) -> List[StockConceptMembershipDaily]:
        """Backward-compatible alias."""
        return self.list_stock_concept_memberships(trade_date=trade_date, code=code)

    def get_latest_trade_date(self) -> Optional[date]:
        with self.db.get_session() as session:
            return session.execute(
                select(StockDailyFeature.trade_date)
                .order_by(desc(StockDailyFeature.trade_date))
                .limit(1)
            ).scalar_one_or_none()

    def _upsert_many(
        self,
        *,
        model: Type[TModel],
        rows: Sequence[RowInput[TModel]],
        key_fields: Tuple[str, ...],
        update_fields: Iterable[str],
    ) -> int:
        if not rows:
            return 0

        objects = [self._as_model(model, row) for row in rows]
        with self.db.session_scope() as session:
            for row in objects:
                filters = [getattr(model, field) == getattr(row, field) for field in key_fields]
                existing = session.execute(select(model).where(and_(*filters)).limit(1)).scalar_one_or_none()
                if existing is None:
                    session.add(row)
                    continue
                for field in update_fields:
                    if hasattr(row, field):
                        setattr(existing, field, getattr(row, field))
            return len(objects)

    @staticmethod
    def _as_model(model: Type[TModel], row: RowInput[TModel]) -> TModel:
        if isinstance(row, model):
            return row
        if isinstance(row, dict):
            return model(**row)
        raise TypeError(f"Unsupported row type for {model.__name__}: {type(row)}")
