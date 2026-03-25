# -*- coding: utf-8 -*-
"""Schemas for structured quant strategy endpoints."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class QuantBacktestRunRequest(BaseModel):
    start_date: date = Field(..., description="回测开始日期")
    end_date: date = Field(..., description="回测结束日期")
    initial_capital: float = Field(1_000_000.0, ge=10_000, description="初始资金")
    strategy_name: str = Field("concept_trend_v1", description="策略名称")


class QuantBacktestSummary(BaseModel):
    total_return_pct: float = 0.0
    final_equity: float = 0.0
    max_drawdown_pct: float = 0.0
    trade_count: float = 0.0
    win_count: float = 0.0
    loss_count: float = 0.0
    win_rate_pct: float = 0.0


class QuantBacktestRunResponse(BaseModel):
    run_id: int
    status: str
    summary: QuantBacktestSummary
    trade_plan_days: int
    trade_count: int


class QuantBacktestDetailResponse(BaseModel):
    run_id: int
    strategy_name: str
    market_scope: str
    board_source: str
    start_date: str
    end_date: str
    initial_capital: float
    status: str
    summary: Dict[str, Any] = Field(default_factory=dict)


class QuantTradeItem(BaseModel):
    id: Optional[int] = None
    code: str
    stock_name: Optional[str] = None
    board_code: Optional[str] = None
    board_name: Optional[str] = None
    entry_date: Optional[str] = None
    exit_date: Optional[str] = None
    entry_price: Optional[float] = None
    exit_price: Optional[float] = None
    entry_amount: Optional[float] = None
    exit_amount: Optional[float] = None
    shares: int
    entry_module: str
    stage: Optional[str] = None
    status: str
    pnl_pct: Optional[float] = None
    pnl_amount: Optional[float] = None
    exit_reason: Optional[str] = None
    blocked_exit: bool = False


class QuantTradeListResponse(BaseModel):
    run_id: int
    message: Optional[str] = None
    items: List[QuantTradeItem] = Field(default_factory=list)


class QuantEquityPoint(BaseModel):
    trade_date: str
    cash: float
    market_value: float
    equity: float
    drawdown_pct: float
    exposure_pct: float


class QuantEquityCurveResponse(BaseModel):
    run_id: int
    items: List[QuantEquityPoint] = Field(default_factory=list)


class QuantTradePlanItem(BaseModel):
    code: str
    board_code: Optional[str] = None
    board_name: Optional[str] = None
    stage: str
    entry_module: str
    signal_score: float
    planned_entry_price: Optional[float] = None
    initial_stop_price: Optional[float] = None
    planned_position_pct: float = 0.0
    blocked_reason: Optional[str] = None
    reason: Dict[str, Any] = Field(default_factory=dict)


class QuantTradePlanDiagnosticBoard(BaseModel):
    board_name: str
    stock_count: int = 0
    stage: Optional[str] = None
    theme_score: Optional[int] = None
    feature_trade_date: Optional[str] = None


class QuantTradePlanDiagnostics(BaseModel):
    eligible_stock_count: int = 0
    same_day_board_match_count: int = 0
    recent_board_fallback_count: int = 0
    missing_board_feature_count: int = 0
    trade_allowed_stock_count: int = 0
    stage_ready_stock_count: int = 0
    candidate_stock_count: int = 0
    mapped_stage_distribution: Dict[str, int] = Field(default_factory=dict)
    stage_ready_distribution: Dict[str, int] = Field(default_factory=dict)
    setup_blocker_counts: Dict[str, int] = Field(default_factory=dict)
    top_missing_boards: List[QuantTradePlanDiagnosticBoard] = Field(default_factory=list)
    trade_allowed_boards: List[QuantTradePlanDiagnosticBoard] = Field(default_factory=list)
    primary_blocker: Optional[str] = None
    summary: Optional[str] = None


class QuantTradePlanResponse(BaseModel):
    as_of_date: str
    regime: str
    market_score: float
    max_exposure_pct: float
    message: Optional[str] = None
    items: List[QuantTradePlanItem] = Field(default_factory=list)
    diagnostics: QuantTradePlanDiagnostics = Field(default_factory=QuantTradePlanDiagnostics)


class QuantSyncRequest(BaseModel):
    history_days: int = Field(130, ge=60, le=365, description="回补与特征构建回看天数")
    include_ranked_boards: bool = Field(True, description="是否尝试补充概念板块排行样本")
    as_of_date: Optional[date] = Field(None, description="指定信号日期，默认 stock_daily 最新交易日")
    latest_feature_only: bool = Field(False, description="是否仅重建最新信号日特征")


class QuantSyncResponse(BaseModel):
    status: str
    message: str
    summary: Dict[str, Any] = Field(default_factory=dict)


class QuantSyncDatasetStatus(BaseModel):
    ready: bool = False
    model_available: bool = False
    latest_trade_date: Optional[str] = None
    records: int = 0
    saved: int = 0
    errors: List[str] = Field(default_factory=list)


class QuantSyncStatusDatasets(BaseModel):
    stock_history: QuantSyncDatasetStatus = Field(default_factory=QuantSyncDatasetStatus)
    index_history: QuantSyncDatasetStatus = Field(default_factory=QuantSyncDatasetStatus)
    concept_membership: QuantSyncDatasetStatus = Field(default_factory=QuantSyncDatasetStatus)
    concept_board_history: QuantSyncDatasetStatus = Field(default_factory=QuantSyncDatasetStatus)
    board_feature: QuantSyncDatasetStatus = Field(default_factory=QuantSyncDatasetStatus)
    stock_feature: QuantSyncDatasetStatus = Field(default_factory=QuantSyncDatasetStatus)


class QuantSyncStatusResponse(BaseModel):
    status: str
    message: str
    as_of_date: Optional[str] = None
    stock_pool_size: int = 0
    membership_distinct_codes: int = 0
    latest_membership_date: Optional[str] = None
    latest_membership_count: int = 0
    concept_board_coverage_count: int = 0
    latest_board_date: Optional[str] = None
    latest_board_count: int = 0
    latest_stock_feature_date: Optional[str] = None
    latest_stock_feature_count: int = 0
    latest_index_feature_date: Optional[str] = None
    latest_stock_daily_date: Optional[str] = None
    earliest_stock_daily_date: Optional[str] = None
    stock_daily_distinct_codes: int = 0
    stock_feature_distinct_codes: int = 0
    main_board_stock_pool_size: int = 0
    index_feature_count: int = 0
    datasets: QuantSyncStatusDatasets = Field(default_factory=QuantSyncStatusDatasets)
    errors: List[str] = Field(default_factory=list)
