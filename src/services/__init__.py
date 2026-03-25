# -*- coding: utf-8 -*-
"""
===================================
服务层模块初始化
===================================

职责：
1. 导出服务类
2. Use lazy imports to avoid importing optional runtime dependencies
   when only a specific service module is needed.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "AnalysisService",
    "BacktestService",
    "HistoryService",
    "StockService",
    "TaskService",
    "get_task_service",
]

_LAZY_IMPORTS = {
    "AnalysisService": ("src.services.analysis_service", "AnalysisService"),
    "BacktestService": ("src.services.backtest_service", "BacktestService"),
    "HistoryService": ("src.services.history_service", "HistoryService"),
    "StockService": ("src.services.stock_service", "StockService"),
    "TaskService": ("src.services.task_service", "TaskService"),
    "get_task_service": ("src.services.task_service", "get_task_service"),
}


def __getattr__(name: str) -> Any:
    """Resolve service exports lazily."""
    if name not in _LAZY_IMPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = _LAZY_IMPORTS[name]
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value

