# -*- coding: utf-8 -*-
"""Repository for stock fund flow data."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Sequence, Union

from sqlalchemy import and_, delete, select
from sqlalchemy.orm import Session

from src.storage import DatabaseManager, StockFundFlowDaily


class StockFundFlowRepository:
    """DB access helpers for stock fund flow table."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    def upsert_batch(self, records: Sequence[Union[StockFundFlowDaily, Dict[str, Any], Any]]) -> int:
        """Upsert fund flow records. Supports dict, StockFundFlowDaily, or dataclass."""
        if not records:
            return 0

        session = self.db.get_session()
        count = 0

        for rec in records:
            if isinstance(rec, dict):
                rec_dict = rec
            elif hasattr(rec, "__dataclass_fields__"):
                from dataclasses import asdict
                rec_dict = asdict(rec)
            else:
                rec_dict = {c.name: getattr(rec, c.name) for c in rec.__table__.columns if c.name != "id"}

            trade_date = rec_dict.get("trade_date")
            code = rec_dict.get("code")

            existing = session.query(StockFundFlowDaily).filter(
                and_(
                    StockFundFlowDaily.code == code,
                    StockFundFlowDaily.trade_date == trade_date,
                )
            ).first()

            if existing:
                for key, val in rec_dict.items():
                    if key.startswith("_") or key in ("id", "created_at", "updated_at"):
                        continue
                    if hasattr(existing, key) and val is not None:
                        setattr(existing, key, val)
            else:
                obj = StockFundFlowDaily(**rec_dict)
                session.add(obj)
            count += 1

        session.commit()
        return count

    def get_latest_date(self) -> Optional[date]:
        """Get the latest trade_date in fund flow table."""
        session = self.db.get_session()
        result = session.query(StockFundFlowDaily.trade_date).order_by(
            StockFundFlowDaily.trade_date.desc()
        ).first()
        return result[0] if result else None

    def get_fund_flow(self, code: str, trade_date: date) -> Optional[StockFundFlowDaily]:
        """Get fund flow for a single stock on a specific date."""
        session = self.db.get_session()
        return session.query(StockFundFlowDaily).filter(
            and_(
                StockFundFlowDaily.code == code,
                StockFundFlowDaily.trade_date == trade_date,
            )
        ).first()

    def get_fund_flow_range(
        self,
        code: str,
        start_date: date,
        end_date: date,
    ) -> List[StockFundFlowDaily]:
        """Get fund flow for a stock over a date range."""
        session = self.db.get_session()
        return session.query(StockFundFlowDaily).filter(
            and_(
                StockFundFlowDaily.code == code,
                StockFundFlowDaily.trade_date >= start_date,
                StockFundFlowDaily.trade_date <= end_date,
            )
        ).order_by(StockFundFlowDaily.trade_date).all()

    def get_rolling_stats(
        self,
        code: str,
        trade_date: date,
        window: int = 5,
    ) -> Dict[str, float]:
        """Get rolling fund flow statistics for a stock."""
        session = self.db.get_session()
        start_date = trade_date - timedelta(days=window + 10)

        rows = session.query(StockFundFlowDaily).filter(
            and_(
                StockFundFlowDaily.code == code,
                StockFundFlowDaily.trade_date >= start_date,
                StockFundFlowDaily.trade_date <= trade_date,
            )
        ).order_by(StockFundFlowDaily.trade_date.desc()).limit(window).all()

        if not rows:
            return {
                "main_net_inflow_5d": 0.0,
                "main_net_inflow_ratio_avg": 0.0,
                "super_large_net_5d": 0.0,
                "large_net_5d": 0.0,
                "latest_main_inflow": 0.0,
                "latest_main_inflow_pct": 0.0,
            }

        main_net_inflow_5d = sum(r.main_net_inflow for r in rows if r)
        main_net_inflow_ratio_avg = sum(r.main_net_inflow_pct for r in rows if r) / len(rows)
        super_large_net_5d = sum(r.super_large_net for r in rows if r)
        large_net_5d = sum(r.large_net for r in rows if r)

        return {
            "main_net_inflow_5d": main_net_inflow_5d,
            "main_net_inflow_ratio_avg": main_net_inflow_ratio_avg,
            "super_large_net_5d": super_large_net_5d,
            "large_net_5d": large_net_5d,
            "latest_main_inflow": rows[0].main_net_inflow if rows else 0.0,
            "latest_main_inflow_pct": rows[0].main_net_inflow_pct if rows else 0.0,
        }

    def delete_before_date(self, cutoff_date: date) -> int:
        """Delete fund flow records before a date."""
        session = self.db.get_session()
        result = session.query(StockFundFlowDaily).filter(
            StockFundFlowDaily.trade_date < cutoff_date
        ).delete()
        session.commit()
        return result

    def count_records(self, trade_date: Optional[date] = None) -> int:
        """Count fund flow records."""
        session = self.db.get_session()
        query = session.query(StockFundFlowDaily)
        if trade_date:
            query = query.filter(StockFundFlowDaily.trade_date == trade_date)
        return query.count()