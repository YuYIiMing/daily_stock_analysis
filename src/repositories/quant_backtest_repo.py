# -*- coding: utf-8 -*-
"""Repository helpers for quant backtest tables."""

from __future__ import annotations

import json
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple, Type, TypeVar, Union

from sqlalchemy import and_, desc, select

from src.storage import (
    DatabaseManager,
    QuantBacktestRun,
    QuantDailyTradeSignal,
    QuantPortfolioEquityCurve,
    QuantTradeLedger,
)

TModel = TypeVar("TModel")
RowInput = Union[TModel, Dict[str, Any]]


class QuantBacktestRepository:
    """DB access helpers for quant backtest tables."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    def create_run(
        self,
        *,
        strategy_name: str,
        market_scope: str,
        board_source: str,
        start_date: date,
        end_date: date,
        initial_capital: float,
        status: str = "pending",
        summary_json: Optional[str] = None,
    ) -> QuantBacktestRun:
        run = QuantBacktestRun(
            strategy_name=strategy_name,
            market_scope=market_scope,
            board_source=board_source,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            status=status,
            summary_json=summary_json,
        )
        with self.db.session_scope() as session:
            session.add(run)
            session.flush()
            session.refresh(run)
            session.expunge(run)
            return run

    def update_run_status(
        self,
        run_id: int,
        *,
        status: str,
        summary: Optional[Dict[str, Any]] = None,
        summary_json: Optional[str] = None,
    ) -> bool:
        with self.db.session_scope() as session:
            run = session.get(QuantBacktestRun, run_id)
            if run is None:
                return False
            run.status = status
            if summary_json is not None:
                run.summary_json = summary_json
            elif summary is not None:
                run.summary_json = json.dumps(summary, ensure_ascii=False)
            return True

    def save_signals(
        self,
        run_id: int,
        signals: Sequence[RowInput[QuantDailyTradeSignal]],
    ) -> int:
        if not signals:
            return 0
        return self._upsert_many(
            model=QuantDailyTradeSignal,
            rows=signals,
            key_fields=("run_id", "trade_date", "code", "entry_module"),
            update_fields=(
                "board_code",
                "board_name",
                "stage",
                "direction",
                "signal_score",
                "planned_entry_price",
                "initial_stop_price",
                "planned_position_pct",
                "reason_json",
                "blocked_reason",
                "updated_at",
            ),
            forced_fields={"run_id": run_id},
        )

    def append_trades(
        self,
        run_id: int,
        trades: Sequence[RowInput[QuantTradeLedger]],
    ) -> int:
        if not trades:
            return 0
        return self._upsert_many(
            model=QuantTradeLedger,
            rows=trades,
            key_fields=("run_id", "code", "entry_date", "entry_module"),
            update_fields=(
                "board_code",
                "board_name",
                "exit_date",
                "entry_price",
                "exit_price",
                "shares",
                "stage",
                "status",
                "pnl_pct",
                "pnl_amount",
                "exit_reason",
                "blocked_exit",
                "updated_at",
            ),
            forced_fields={"run_id": run_id},
        )

    def save_equity_curve(
        self,
        run_id: int,
        points: Sequence[RowInput[QuantPortfolioEquityCurve]],
    ) -> int:
        if not points:
            return 0
        return self._upsert_many(
            model=QuantPortfolioEquityCurve,
            rows=points,
            key_fields=("run_id", "trade_date"),
            update_fields=("cash", "market_value", "equity", "drawdown_pct", "exposure_pct", "updated_at"),
            forced_fields={"run_id": run_id},
        )

    def get_run(self, run_id: int) -> Optional[QuantBacktestRun]:
        with self.db.get_session() as session:
            return session.get(QuantBacktestRun, run_id)

    def get_latest_run(self) -> Optional[QuantBacktestRun]:
        with self.db.get_session() as session:
            return session.execute(
                select(QuantBacktestRun)
                .order_by(desc(QuantBacktestRun.id))
                .limit(1)
            ).scalar_one_or_none()

    def list_trades(self, run_id: int) -> List[QuantTradeLedger]:
        with self.db.get_session() as session:
            rows = session.execute(
                select(QuantTradeLedger)
                .where(QuantTradeLedger.run_id == run_id)
                .order_by(QuantTradeLedger.entry_date, QuantTradeLedger.code)
            ).scalars().all()
            return list(rows)

    def get_equity_curve(self, run_id: int) -> List[QuantPortfolioEquityCurve]:
        with self.db.get_session() as session:
            rows = session.execute(
                select(QuantPortfolioEquityCurve)
                .where(QuantPortfolioEquityCurve.run_id == run_id)
                .order_by(QuantPortfolioEquityCurve.trade_date)
            ).scalars().all()
            return list(rows)

    def list_signals(self, run_id: int, *, trade_date: Optional[date] = None) -> List[QuantDailyTradeSignal]:
        with self.db.get_session() as session:
            query = select(QuantDailyTradeSignal).where(QuantDailyTradeSignal.run_id == run_id)
            if trade_date:
                query = query.where(QuantDailyTradeSignal.trade_date == trade_date)
            query = query.order_by(QuantDailyTradeSignal.trade_date, desc(QuantDailyTradeSignal.signal_score))
            return list(session.execute(query).scalars().all())

    def _upsert_many(
        self,
        *,
        model: Type[TModel],
        rows: Sequence[RowInput[TModel]],
        key_fields: Tuple[str, ...],
        update_fields: Tuple[str, ...],
        forced_fields: Optional[Dict[str, Any]] = None,
    ) -> int:
        objects = [self._as_model(model, row, forced_fields=forced_fields) for row in rows]
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
    def _as_model(
        model: Type[TModel],
        row: RowInput[TModel],
        *,
        forced_fields: Optional[Dict[str, Any]] = None,
    ) -> TModel:
        if isinstance(row, model):
            obj = row
        elif isinstance(row, dict):
            payload = dict(row)
            if forced_fields:
                payload.update(forced_fields)
            obj = model(**payload)
        else:
            raise TypeError(f"Unsupported row type for {model.__name__}: {type(row)}")

        if forced_fields:
            for field, value in forced_fields.items():
                setattr(obj, field, value)

        if isinstance(obj, QuantTradeLedger) and obj.blocked_exit is None:
            obj.blocked_exit = False
        return obj
