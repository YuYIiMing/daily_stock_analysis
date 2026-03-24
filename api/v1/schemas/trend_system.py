# -*- coding: utf-8 -*-
"""Pydantic schemas for trend-system APIs."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


Stage = Literal["initial", "middle", "late", "choppy"]
SignalType = Literal["breakout", "pullback", "late_reclaim", "compensation"]
RiskMode = Literal["normal", "reduced_risk", "elite_disabled", "breakout_paused", "cooldown", "degraded_system"]
SectorView = Literal["concept", "industry"]
PositionAction = Literal["hold", "reduce", "exit"]
TradeGate = Literal["allowed", "blocked"]
SnapshotType = Literal["daily_close", "preopen", "manual_recompute"]
AlertType = Literal["leader_break", "leader_limit_down", "stop_loss", "take_profit", "trend_exit", "emotion_exit"]


class PositionRule(BaseModel):
    key: str
    label: str
    matched: bool
    value: Any = None


class PositionResponse(BaseModel):
    recommended_position_pct: int
    matched_rules: int
    rules: List[PositionRule]
    index_code: str


class SectorOverrideInfo(BaseModel):
    id: int
    override_date: str
    sector_view: str
    sector_key: str
    sector_name: Optional[str] = None
    original_stage: Stage
    target_stage: Stage
    reason: str
    operator: str
    created_at: Optional[str] = None


class SectorMemberBrief(BaseModel):
    code: str
    name: str
    pct_chg: float
    amount_b: Optional[float] = None
    gain_20d: Optional[float] = None


class SectorBreadth(BaseModel):
    strong_member_count: int
    limit_up_count: int
    top5_avg_pct: Optional[float] = None
    consistency_score: int


class SectorDecisionItem(BaseModel):
    sector_key: str
    sector_name: str
    sector_view: SectorView
    member_count: int
    latest_amount_b: float
    top_amount_rank: int
    matched_conditions: int
    conditions: Dict[str, bool]
    trade_allowed: bool
    quant_stage: Stage
    final_stage: Stage
    stage_meta: Dict[str, Any] = Field(default_factory=dict)
    override: Optional[SectorOverrideInfo] = None
    leader: Optional[SectorMemberBrief] = None
    leader_2: Optional[SectorMemberBrief] = None
    frontline_members: List[SectorMemberBrief] = Field(default_factory=list)
    sector_breadth: SectorBreadth
    members: List[Dict[str, Any]] = Field(default_factory=list)


class RiskStateResponse(BaseModel):
    current_mode: RiskMode
    flags: Dict[str, bool]
    consecutive_stop_losses: int
    consecutive_non_stop_losses: int
    recent_breakout_failures: int
    cooldown_until: Optional[str] = None
    new_position_limit_pct: Optional[int] = None
    reasons: List[str]


class OverviewSector(BaseModel):
    sector_key: str
    sector_name: str
    sector_view: SectorView
    final_stage: Stage
    trade_allowed: bool


class TrendSystemOverviewResponse(BaseModel):
    as_of: str
    generated_at: Optional[str] = None
    position: PositionResponse
    trade_allowed: bool
    trade_gate: TradeGate
    primary_stage: Stage
    main_sectors: List[OverviewSector]
    risk_state: RiskStateResponse
    candidate_count: int
    snapshot_status: str = "ready"
    empty_reason: Optional[str] = None


class CandidateItem(BaseModel):
    code: str
    name: str
    sector_key: str
    sector_name: str
    sector_view: SectorView
    final_stage: Stage
    signal_type: SignalType
    signal_label: str
    signal_score: int
    actionable: bool
    action_block_reason: Optional[str] = None
    suggested_entry: Optional[float] = None
    invalid_if: Optional[str] = None
    stop_loss: Optional[float] = None
    position_limit_pct: int
    recommended_position_pct: int
    reason_checks: Dict[str, bool]
    is_elite_candidate: bool
    latest_close: Optional[float] = None
    gain_20d: Optional[float] = None
    gain_60d: Optional[float] = None
    filter_reasons: List[str] = Field(default_factory=list)


class TrendDiagnosticsResponse(BaseModel):
    mode: str
    total_market_symbols: int
    db_backed_symbols: int
    scan_limit: int
    scanned_symbols: int
    etf_excluded: int
    index_excluded: int
    missing_history: int
    sector_resolution_failures: int
    sector_resolved: int
    float_cap_resolved: int
    coverage_ratio: Optional[float] = None
    alert_count: int = 0
    open_position_count: int = 0
    open_position_pct: float = 0
    source_notes: List[str] = Field(default_factory=list)
    candidate_filters: Dict[str, Any] = Field(default_factory=dict)


class PositionSignals(BaseModel):
    strong_stop_loss: bool
    weak_stop_loss: bool
    take_profit_10: bool
    take_profit_20: bool
    trend_exit_ma10: bool
    trend_exit_ma20: bool
    emotion_exit: bool


class PortfolioPositionItem(BaseModel):
    id: int
    code: str
    name: Optional[str] = None
    sector_key: Optional[str] = None
    sector_name: Optional[str] = None
    open_type: str
    position_pct: float
    entry_price: float
    latest_close: Optional[float] = None
    profit_pct: Optional[float] = None
    initial_stop_loss: Optional[float] = None
    current_stop_loss: Optional[float] = None
    trend_exit_line: Optional[float] = None
    take_profit_stage: int
    action: PositionAction
    action_reason: str
    suggested_sell_pct: int
    signals: PositionSignals
    status: str


class PortfolioSummary(BaseModel):
    open_count: int
    total_position_pct: float
    reduce_count: int
    exit_count: int


class PortfolioResponse(BaseModel):
    summary: PortfolioSummary
    items: List[PortfolioPositionItem]


class TrendPlanResponse(BaseModel):
    generated_at: str
    recommended_position_pct: int
    trade_allowed: bool
    main_sectors: List[OverviewSector]
    primary_stage: Stage
    risk_state: RiskStateResponse
    candidates: List[CandidateItem]
    portfolio_actions: List[PortfolioPositionItem] = Field(default_factory=list)
    blocked_rules: List[str] = Field(default_factory=list)
    discipline_notes: List[str]
    empty_reason: Optional[str] = None
    diagnostics_summary: Dict[str, Any] = Field(default_factory=dict)


class StageOverrideRequest(BaseModel):
    sector_view: SectorView
    sector_key: str
    sector_name: str
    original_stage: Stage
    target_stage: Stage
    reason: str = Field(..., min_length=1)
    operator: str = "web"


class TradeRecordCreateRequest(BaseModel):
    code: str
    name: Optional[str] = None
    sector_view: SectorView = "concept"
    sector_key: Optional[str] = None
    sector_name: Optional[str] = None
    open_date: date
    open_type: SignalType
    entry_price: float = Field(..., gt=0)
    initial_stop_loss: Optional[float] = Field(None, gt=0)
    position_pct: float = Field(..., gt=0, le=100)
    is_elite_strategy: bool = False
    close_date: Optional[date] = None
    exit_price: Optional[float] = Field(None, gt=0)
    exit_reason: Optional[str] = None
    is_stop_loss: Optional[bool] = None
    breakout_failed: Optional[bool] = None


class TradeRecordUpdateRequest(BaseModel):
    name: Optional[str] = None
    sector_view: Optional[SectorView] = None
    sector_key: Optional[str] = None
    sector_name: Optional[str] = None
    initial_stop_loss: Optional[float] = Field(None, gt=0)
    position_pct: Optional[float] = Field(None, gt=0, le=100)
    is_elite_strategy: Optional[bool] = None
    close_date: Optional[date] = None
    exit_price: Optional[float] = Field(None, gt=0)
    exit_reason: Optional[str] = None
    is_stop_loss: Optional[bool] = None
    breakout_failed: Optional[bool] = None


class TradeRecordResponse(BaseModel):
    id: int
    code: str
    name: Optional[str] = None
    sector_view: str
    sector_key: Optional[str] = None
    sector_name: Optional[str] = None
    open_date: str
    open_type: str
    entry_price: float
    initial_stop_loss: Optional[float] = None
    position_pct: float
    is_elite_strategy: bool
    close_date: Optional[str] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    is_stop_loss: Optional[bool] = None
    breakout_failed: Optional[bool] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class TradeListResponse(BaseModel):
    items: List[TradeRecordResponse]
    total: int


class PositionRecordCreateRequest(BaseModel):
    code: str
    name: Optional[str] = None
    sector_view: SectorView = "concept"
    sector_key: Optional[str] = None
    sector_name: Optional[str] = None
    open_date: date
    open_type: SignalType
    entry_price: float = Field(..., gt=0)
    initial_stop_loss: Optional[float] = Field(None, gt=0)
    current_stop_loss: Optional[float] = Field(None, gt=0)
    trend_exit_line: Optional[float] = Field(None, gt=0)
    position_pct: float = Field(..., gt=0, le=100)
    shares: Optional[float] = Field(None, gt=0)
    is_elite_strategy: bool = False
    take_profit_stage: int = Field(0, ge=0, le=2)
    notes: Optional[str] = None


class PositionRecordUpdateRequest(BaseModel):
    name: Optional[str] = None
    sector_view: Optional[SectorView] = None
    sector_key: Optional[str] = None
    sector_name: Optional[str] = None
    initial_stop_loss: Optional[float] = Field(None, gt=0)
    current_stop_loss: Optional[float] = Field(None, gt=0)
    trend_exit_line: Optional[float] = Field(None, gt=0)
    position_pct: Optional[float] = Field(None, gt=0, le=100)
    shares: Optional[float] = Field(None, gt=0)
    is_elite_strategy: Optional[bool] = None
    take_profit_stage: Optional[int] = Field(None, ge=0, le=2)
    status: Optional[str] = None
    close_date: Optional[date] = None
    exit_price: Optional[float] = Field(None, gt=0)
    exit_reason: Optional[str] = None
    notes: Optional[str] = None


class PositionRecordResponse(BaseModel):
    id: int
    code: str
    name: Optional[str] = None
    sector_view: str
    sector_key: Optional[str] = None
    sector_name: Optional[str] = None
    open_date: str
    open_type: str
    entry_price: float
    initial_stop_loss: Optional[float] = None
    current_stop_loss: Optional[float] = None
    trend_exit_line: Optional[float] = None
    position_pct: float
    shares: Optional[float] = None
    is_elite_strategy: bool
    take_profit_stage: int
    status: str
    linked_trade_id: Optional[int] = None
    close_date: Optional[str] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    notes: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class PositionListResponse(BaseModel):
    items: List[PositionRecordResponse]
    total: int


class TrendAlertResponse(BaseModel):
    id: int
    alert_date: str
    alert_type: str
    code: Optional[str] = None
    name: Optional[str] = None
    sector_key: Optional[str] = None
    message: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    acked: bool
    acked_at: Optional[str] = None
    created_at: Optional[str] = None


class TrendAlertListResponse(BaseModel):
    items: List[TrendAlertResponse]
    total: int


class TrendStatusEntry(BaseModel):
    status: str
    snapshot_date: Optional[str] = None
    generated_at: Optional[str] = None
    source: Optional[str] = None


class TrendRecomputeState(BaseModel):
    running: bool = False
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    last_error: Optional[str] = None


class TrendStatusResponse(BaseModel):
    as_of: str
    daily_snapshot: TrendStatusEntry
    preopen_snapshot: TrendStatusEntry
    recompute_state: TrendRecomputeState = Field(default_factory=TrendRecomputeState)


class TrendRecomputeRequest(BaseModel):
    snapshot_type: SnapshotType = "manual_recompute"
    background: bool = True


class TrendRecomputeResponse(BaseModel):
    status: str
    snapshot_type: SnapshotType | str
    generated_at: str
    snapshot_date: str
