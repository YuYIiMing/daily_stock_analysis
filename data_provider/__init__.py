# -*- coding: utf-8 -*-
"""
===================================
数据源策略层 - 包初始化
===================================

This package exports fetchers lazily so lightweight callers can import
shared helpers without pulling every optional data-source dependency.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "BaseFetcher",
    "DataFetcherManager",
    "EfinanceFetcher",
    "AkshareFetcher",
    "TushareFetcher",
    "PytdxFetcher",
    "BaostockFetcher",
    "YfinanceFetcher",
    "is_us_index_code",
    "is_us_stock_code",
    "is_hk_stock_code",
    "get_us_index_yf_symbol",
    "US_INDEX_MAPPING",
]

_LAZY_IMPORTS = {
    "BaseFetcher": ("data_provider.base", "BaseFetcher"),
    "DataFetcherManager": ("data_provider.base", "DataFetcherManager"),
    "EfinanceFetcher": ("data_provider.efinance_fetcher", "EfinanceFetcher"),
    "AkshareFetcher": ("data_provider.akshare_fetcher", "AkshareFetcher"),
    "TushareFetcher": ("data_provider.tushare_fetcher", "TushareFetcher"),
    "PytdxFetcher": ("data_provider.pytdx_fetcher", "PytdxFetcher"),
    "BaostockFetcher": ("data_provider.baostock_fetcher", "BaostockFetcher"),
    "YfinanceFetcher": ("data_provider.yfinance_fetcher", "YfinanceFetcher"),
    "is_us_index_code": ("data_provider.us_index_mapping", "is_us_index_code"),
    "is_us_stock_code": ("data_provider.us_index_mapping", "is_us_stock_code"),
    "is_hk_stock_code": ("data_provider.akshare_fetcher", "is_hk_stock_code"),
    "get_us_index_yf_symbol": ("data_provider.us_index_mapping", "get_us_index_yf_symbol"),
    "US_INDEX_MAPPING": ("data_provider.us_index_mapping", "US_INDEX_MAPPING"),
}


def __getattr__(name: str) -> Any:
    """Resolve package exports lazily."""
    if name not in _LAZY_IMPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = _LAZY_IMPORTS[name]
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value

