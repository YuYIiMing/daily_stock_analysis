# -*- coding: utf-8 -*-
"""Trend-system API endpoints."""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_config_dep, get_database_manager
from api.v1.schemas.common import ErrorResponse
from api.v1.schemas.trend_system import (
    CandidateItem,
    PortfolioResponse,
    PositionListResponse,
    PositionRecordCreateRequest,
    PositionRecordResponse,
    PositionRecordUpdateRequest,
    PositionResponse,
    RiskStateResponse,
    SectorDecisionItem,
    SectorOverrideInfo,
    StageOverrideRequest,
    TradeListResponse,
    TradeRecordCreateRequest,
    TradeRecordResponse,
    TradeRecordUpdateRequest,
    TrendAlertListResponse,
    TrendAlertResponse,
    TrendDiagnosticsResponse,
    TrendPlanResponse,
    TrendRecomputeRequest,
    TrendRecomputeResponse,
    TrendStatusResponse,
    TrendSystemOverviewResponse,
)
from src.config import Config
from src.services.trend_system_service import TrendSystemService
from src.storage import DatabaseManager

logger = logging.getLogger(__name__)

router = APIRouter()


def get_trend_system_service(
    config: Config = Depends(get_config_dep),
    db: DatabaseManager = Depends(get_database_manager),
) -> TrendSystemService:
    """Return trend-system service instance."""
    return TrendSystemService(config=config, db=db)


@router.get("/overview", response_model=TrendSystemOverviewResponse, responses={500: {"model": ErrorResponse}})
def get_overview(service: TrendSystemService = Depends(get_trend_system_service)) -> TrendSystemOverviewResponse:
    try:
        return TrendSystemOverviewResponse.model_validate(service.get_overview())
    except Exception as exc:
        logger.error("Failed to load trend overview: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": "Failed to load overview"})


@router.get("/position", response_model=PositionResponse, responses={500: {"model": ErrorResponse}})
def get_position(service: TrendSystemService = Depends(get_trend_system_service)) -> PositionResponse:
    try:
        return PositionResponse.model_validate(service.get_position())
    except Exception as exc:
        logger.error("Failed to load trend position: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": "Failed to load position"})


@router.get("/sectors", response_model=list[SectorDecisionItem], responses={500: {"model": ErrorResponse}})
def get_sectors(
    view: str = Query("concept", pattern="^(concept|industry)$"),
    service: TrendSystemService = Depends(get_trend_system_service),
) -> list[SectorDecisionItem]:
    try:
        return [SectorDecisionItem.model_validate(item) for item in service.get_sectors(view)]
    except Exception as exc:
        logger.error("Failed to load trend sectors: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": "Failed to load sectors"})


@router.get("/candidates", response_model=list[CandidateItem], responses={500: {"model": ErrorResponse}})
def get_candidates(service: TrendSystemService = Depends(get_trend_system_service)) -> list[CandidateItem]:
    try:
        return [CandidateItem.model_validate(item) for item in service.get_candidates()]
    except Exception as exc:
        logger.error("Failed to load trend candidates: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": "Failed to load candidates"})


@router.get("/portfolio", response_model=PortfolioResponse, responses={500: {"model": ErrorResponse}})
def get_portfolio(service: TrendSystemService = Depends(get_trend_system_service)) -> PortfolioResponse:
    try:
        return PortfolioResponse.model_validate(service.get_portfolio())
    except Exception as exc:
        logger.error("Failed to load trend portfolio: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": "Failed to load portfolio"})


@router.get("/risk-state", response_model=RiskStateResponse, responses={500: {"model": ErrorResponse}})
def get_risk_state(service: TrendSystemService = Depends(get_trend_system_service)) -> RiskStateResponse:
    try:
        return RiskStateResponse.model_validate(service.get_risk_state())
    except Exception as exc:
        logger.error("Failed to load trend risk state: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": "Failed to load risk state"})


@router.get("/plan", response_model=TrendPlanResponse, responses={500: {"model": ErrorResponse}})
def get_plan(service: TrendSystemService = Depends(get_trend_system_service)) -> TrendPlanResponse:
    try:
        return TrendPlanResponse.model_validate(service.get_plan())
    except Exception as exc:
        logger.error("Failed to load trend plan: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": "Failed to load plan"})


@router.get("/diagnostics", response_model=TrendDiagnosticsResponse, responses={500: {"model": ErrorResponse}})
def get_diagnostics(service: TrendSystemService = Depends(get_trend_system_service)) -> TrendDiagnosticsResponse:
    try:
        return TrendDiagnosticsResponse.model_validate(service.get_diagnostics())
    except Exception as exc:
        logger.error("Failed to load trend diagnostics: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": "Failed to load diagnostics"},
        )


@router.get("/status", response_model=TrendStatusResponse, responses={500: {"model": ErrorResponse}})
def get_status(service: TrendSystemService = Depends(get_trend_system_service)) -> TrendStatusResponse:
    try:
        return TrendStatusResponse.model_validate(service.get_status())
    except Exception as exc:
        logger.error("Failed to load trend status: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": "Failed to load status"})


@router.post("/recompute", response_model=TrendRecomputeResponse, responses={500: {"model": ErrorResponse}})
def recompute(
    request: TrendRecomputeRequest,
    service: TrendSystemService = Depends(get_trend_system_service),
) -> TrendRecomputeResponse:
    try:
        return TrendRecomputeResponse.model_validate(
            service.recompute(snapshot_type=request.snapshot_type, wait=not request.background)
        )
    except Exception as exc:
        logger.error("Failed to recompute trend snapshot: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": "Failed to recompute"})


@router.post(
    "/stage-override",
    response_model=SectorOverrideInfo,
    responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
def create_stage_override(
    request: StageOverrideRequest,
    service: TrendSystemService = Depends(get_trend_system_service),
) -> SectorOverrideInfo:
    try:
        return SectorOverrideInfo.model_validate(service.create_stage_override(**request.model_dump()))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": "validation_error", "message": str(exc)})
    except Exception as exc:
        logger.error("Failed to create stage override: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": "Failed to create stage override"},
        )


@router.post("/trades", response_model=TradeRecordResponse, responses={500: {"model": ErrorResponse}})
def create_trade(
    request: TradeRecordCreateRequest,
    service: TrendSystemService = Depends(get_trend_system_service),
) -> TradeRecordResponse:
    try:
        return TradeRecordResponse.model_validate(service.create_trade(request.model_dump()))
    except Exception as exc:
        logger.error("Failed to create trend trade: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": "Failed to create trade"})


@router.patch(
    "/trades/{trade_id}",
    response_model=TradeRecordResponse,
    responses={404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
def update_trade(
    trade_id: int,
    request: TradeRecordUpdateRequest,
    service: TrendSystemService = Depends(get_trend_system_service),
) -> TradeRecordResponse:
    try:
        updated = service.update_trade(trade_id, request.model_dump(exclude_none=True))
        if updated is None:
            raise HTTPException(status_code=404, detail={"error": "not_found", "message": "Trade record not found"})
        return TradeRecordResponse.model_validate(updated)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to update trend trade: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": "Failed to update trade"})


@router.get("/trades", response_model=TradeListResponse, responses={500: {"model": ErrorResponse}})
def list_trades(
    code: Optional[str] = Query(None),
    sector_key: Optional[str] = Query(None),
    status: Optional[str] = Query(None, pattern="^(open|closed)$"),
    limit: int = Query(100, ge=1, le=500),
    service: TrendSystemService = Depends(get_trend_system_service),
) -> TradeListResponse:
    try:
        items = service.list_trades(code=code, sector_key=sector_key, status=status, limit=limit)
        return TradeListResponse(items=[TradeRecordResponse.model_validate(item) for item in items], total=len(items))
    except Exception as exc:
        logger.error("Failed to list trend trades: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": "Failed to load trades"})


@router.post("/positions", response_model=PositionRecordResponse, responses={500: {"model": ErrorResponse}})
def create_position(
    request: PositionRecordCreateRequest,
    service: TrendSystemService = Depends(get_trend_system_service),
) -> PositionRecordResponse:
    try:
        return PositionRecordResponse.model_validate(service.create_position(request.model_dump()))
    except Exception as exc:
        logger.error("Failed to create position: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": "Failed to create position"})


@router.patch(
    "/positions/{position_id}",
    response_model=PositionRecordResponse,
    responses={404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
def update_position(
    position_id: int,
    request: PositionRecordUpdateRequest,
    service: TrendSystemService = Depends(get_trend_system_service),
) -> PositionRecordResponse:
    try:
        updated = service.update_position(position_id, request.model_dump(exclude_none=True))
        if updated is None:
            raise HTTPException(status_code=404, detail={"error": "not_found", "message": "Position record not found"})
        return PositionRecordResponse.model_validate(updated)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to update position: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": "Failed to update position"})


@router.get("/positions", response_model=PositionListResponse, responses={500: {"model": ErrorResponse}})
def list_positions(
    code: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    service: TrendSystemService = Depends(get_trend_system_service),
) -> PositionListResponse:
    try:
        items = service.list_positions(code=code, status=status, limit=limit)
        return PositionListResponse(
            items=[PositionRecordResponse.model_validate(item) for item in items],
            total=len(items),
        )
    except Exception as exc:
        logger.error("Failed to list positions: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": "Failed to load positions"})


@router.get("/alerts", response_model=TrendAlertListResponse, responses={500: {"model": ErrorResponse}})
def list_alerts(
    days: int = Query(5, ge=1, le=30),
    limit: int = Query(100, ge=1, le=500),
    service: TrendSystemService = Depends(get_trend_system_service),
) -> TrendAlertListResponse:
    try:
        items = service.list_alerts(days=days, limit=limit)
        return TrendAlertListResponse(
            items=[TrendAlertResponse.model_validate(item) for item in items],
            total=len(items),
        )
    except Exception as exc:
        logger.error("Failed to list alerts: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": "Failed to load alerts"})


@router.post(
    "/alerts/{alert_id}/ack",
    response_model=TrendAlertResponse,
    responses={404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
def ack_alert(alert_id: int, service: TrendSystemService = Depends(get_trend_system_service)) -> TrendAlertResponse:
    try:
        item = service.ack_alert(alert_id)
        if item is None:
            raise HTTPException(status_code=404, detail={"error": "not_found", "message": "Alert not found"})
        return TrendAlertResponse.model_validate(item)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to ack alert: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": "Failed to ack alert"})
