# -*- coding: utf-8 -*-
"""
===================================
FundFlowFetcher - Individual Stock Fund Flow Data
===================================

Data source: AKShare stock_individual_fund_flow
Features: Main force net inflow, super large/large/medium/small order flow

This module fetches daily fund flow data for individual stocks,
which is used as a signal quality factor in the quant strategy.
"""

import logging
import random
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Optional, Dict, Any

import pandas as pd

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.config import get_config

logger = logging.getLogger(__name__)


@dataclass
class StockFundFlowRecord:
    """Single stock fund flow record."""
    trade_date: date
    code: str
    close: float
    pct_chg: float
    main_net_inflow: float
    main_net_inflow_pct: float
    super_large_net: float
    super_large_pct: float
    large_net: float
    large_pct: float
    medium_net: float
    medium_pct: float
    small_net: float
    small_pct: float
    turnover_rate: float = 0.0


class FundFlowFetcher:
    """Fetch individual stock fund flow data from AKShare."""

    MARKET_PREFIX = {
        "600": "sh", "601": "sh", "603": "sh", "605": "sh", "688": "sh",
        "000": "sz", "001": "sz", "002": "sz", "003": "sz", "300": "sz", "301": "sz",
    }

    def __init__(self):
        self._akshare = None
        self._circuit_breaker_count = 0
        self._circuit_breaker_threshold = 5
        self._circuit_breaker_cooldown = 60

    @property
    def akshare(self):
        if self._akshare is None:
            import akshare as ak
            self._akshare = ak
        return self._akshare

    def _get_market(self, code: str) -> str:
        """Get market prefix for stock code."""
        for prefix, market in self.MARKET_PREFIX.items():
            if code.startswith(prefix):
                return market
        return "sh" if code.startswith("6") else "sz"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    def fetch_single_stock(self, code: str, lookback_days: int = 120) -> List[StockFundFlowRecord]:
        """Fetch fund flow data for a single stock."""
        if self._circuit_breaker_count >= self._circuit_breaker_threshold:
            logger.warning(f"[FundFlow] Circuit breaker open, skipping {code}")
            return []

        market = self._get_market(code)
        try:
            time.sleep(random.uniform(0.5, 1.5))
            df = self.akshare.stock_individual_fund_flow(stock=code, market=market)
            
            if df is None or df.empty:
                return []

            records = self._parse_dataframe(code, df, lookback_days)
            self._circuit_breaker_count = 0
            return records

        except Exception as e:
            self._circuit_breaker_count += 1
            logger.warning(f"[FundFlow] Failed to fetch {code}: {e}")
            raise

    def _parse_dataframe(self, code: str, df: pd.DataFrame, lookback_days: int) -> List[StockFundFlowRecord]:
        """Parse AKShare DataFrame into StockFundFlowRecord list."""
        records = []

        column_map = {
            "日期": "trade_date",
            "收盘价": "close",
            "涨跌幅": "pct_chg",
            "主力净流入-净额": "main_net_inflow",
            "主力净流入-净占比": "main_net_inflow_pct",
            "超大单净流入-净额": "super_large_net",
            "超大单净流入-净占比": "super_large_pct",
            "大单净流入-净额": "large_net",
            "大单净流入-净占比": "large_pct",
            "中单净流入-净额": "medium_net",
            "中单净流入-净占比": "medium_pct",
            "小单净流入-净额": "small_net",
            "小单净流入-净占比": "small_pct",
        }

        df = df.rename(columns=column_map)

        for _, row in df.iterrows():
            try:
                trade_date_raw = row.get("trade_date")
                if pd.isna(trade_date_raw):
                    continue
                
                if isinstance(trade_date_raw, date):
                    trade_date = trade_date_raw
                elif isinstance(trade_date_raw, str):
                    if " " in trade_date_raw:
                        trade_date_raw = trade_date_raw.split(" ")[0]
                    trade_date = datetime.strptime(trade_date_raw, "%Y-%m-%d").date()
                elif isinstance(trade_date_raw, (datetime, pd.Timestamp)):
                    trade_date = trade_date_raw.date() if hasattr(trade_date_raw, "date") else trade_date_raw
                else:
                    continue

                def safe_float(val, default=0.0):
                    if pd.isna(val):
                        return default
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        return default

                record = StockFundFlowRecord(
                    trade_date=trade_date,
                    code=code,
                    close=safe_float(row.get("close", 0)),
                    pct_chg=safe_float(row.get("pct_chg", 0)),
                    main_net_inflow=safe_float(row.get("main_net_inflow", 0)),
                    main_net_inflow_pct=safe_float(row.get("main_net_inflow_pct", 0)),
                    super_large_net=safe_float(row.get("super_large_net", 0)),
                    super_large_pct=safe_float(row.get("super_large_pct", 0)),
                    large_net=safe_float(row.get("large_net", 0)),
                    large_pct=safe_float(row.get("large_pct", 0)),
                    medium_net=safe_float(row.get("medium_net", 0)),
                    medium_pct=safe_float(row.get("medium_pct", 0)),
                    small_net=safe_float(row.get("small_net", 0)),
                    small_pct=safe_float(row.get("small_pct", 0)),
                )
                records.append(record)
            except Exception as e:
                logger.debug(f"[FundFlow] Parse error for {code} row: {e}")
                continue

        return records

    def fetch_batch(
        self,
        codes: List[str],
        lookback_days: int = 120,
        progress_callback: Optional[callable] = None,
    ) -> Dict[str, List[StockFundFlowRecord]]:
        """Fetch fund flow data for multiple stocks."""
        results: Dict[str, List[StockFundFlowRecord]] = {}
        total = len(codes)

        for i, code in enumerate(codes):
            try:
                records = self.fetch_single_stock(code, lookback_days)
                if records:
                    results[code] = records
            except Exception as e:
                logger.warning(f"[FundFlow] Skip {code} after retries: {e}")
            
            if progress_callback and (i + 1) % 10 == 0:
                progress_callback(i + 1, total, len(results))

        return results

    def compute_rolling_stats(
        self,
        records: List[StockFundFlowRecord],
        window: int = 5,
    ) -> Dict[str, Any]:
        """Compute rolling fund flow statistics."""
        if not records:
            return {
                "main_net_inflow_5d": 0.0,
                "main_net_inflow_ratio_avg": 0.0,
                "super_large_net_5d": 0.0,
                "large_net_5d": 0.0,
            }

        sorted_records = sorted(records, key=lambda x: x.trade_date, reverse=True)[:window]
        
        main_net_inflow_5d = sum(r.main_net_inflow for r in sorted_records)
        main_net_inflow_ratio_avg = sum(r.main_net_inflow_pct for r in sorted_records) / len(sorted_records)
        super_large_net_5d = sum(r.super_large_net for r in sorted_records)
        large_net_5d = sum(r.large_net for r in sorted_records)

        return {
            "main_net_inflow_5d": main_net_inflow_5d,
            "main_net_inflow_ratio_avg": main_net_inflow_ratio_avg,
            "super_large_net_5d": super_large_net_5d,
            "large_net_5d": large_net_5d,
            "latest_main_inflow": sorted_records[0].main_net_inflow if sorted_records else 0.0,
            "latest_main_inflow_pct": sorted_records[0].main_net_inflow_pct if sorted_records else 0.0,
        }