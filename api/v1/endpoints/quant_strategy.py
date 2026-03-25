# -*- coding: utf-8 -*-
"""Endpoints for structured quant strategy backtests and trade plans."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_database_manager
from api.v1.schemas.common import ErrorResponse
from api.v1.schemas.quant_strategy import (
    QuantBacktestDetailResponse,
    QuantBacktestRunRequest,
    QuantBacktestRunResponse,
    QuantBacktestSummary,
    QuantEquityCurveResponse,
    QuantEquityPoint,
    QuantTradeItem,
    QuantTradePlanDiagnostics,
    QuantTradeListResponse,
    QuantTradePlanItem,
    QuantTradePlanResponse,
    QuantSyncRequest,
    QuantSyncResponse,
    QuantSyncDatasetStatus,
    QuantSyncStatusDatasets,
    QuantSyncStatusResponse,
)
from src.services.quant_backtest_service import QuantBacktestService
from src.services.quant_data_service import QuantDataService
from src.storage import DatabaseManager

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post(
    "/sync",
    response_model=QuantSyncResponse,
    responses={500: {"description": "服务器错误", "model": ErrorResponse}},
    summary="同步量化策略数据快照",
)
def sync_quant_dataset(
    request: QuantSyncRequest,
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> QuantSyncResponse:
    """Run one-click quant data refresh for feature tables."""
    try:
        service = QuantDataService(db_manager=db_manager)
        summary = service.refresh_quant_dataset(
            as_of_date=request.as_of_date.isoformat() if request.as_of_date else None,
            history_days=int(request.history_days),
            include_ranked_boards=bool(request.include_ranked_boards),
            latest_feature_only=bool(request.latest_feature_only),
        )
        errors = summary.get("errors", []) if isinstance(summary, dict) else []
        if errors:
            return QuantSyncResponse(
                status="partial",
                message="量化数据同步完成，但存在部分失败项，请检查 summary.errors。",
                summary=summary,
            )
        return QuantSyncResponse(
            status="ok",
            message="量化数据同步完成，可继续生成交易计划或运行回测。",
            summary=summary,
        )
    except Exception as exc:
        logger.error(f"量化数据同步失败: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"量化数据同步失败: {str(exc)}"},
        )


@router.get(
    "/sync-status",
    response_model=QuantSyncStatusResponse,
    summary="获取量化数据同步状态",
)
def get_quant_sync_status(
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> QuantSyncStatusResponse:
    service = QuantDataService(db_manager=db_manager)
    try:
        summary = _resolve_sync_status_summary(service)
    except Exception as exc:
        logger.warning(f"获取量化同步状态失败，返回空状态: {exc}", exc_info=True)
        summary = {}
    return _build_sync_status_response(summary)


@router.post(
    "/backtests/run",
    response_model=QuantBacktestRunResponse,
    responses={500: {"description": "服务器错误", "model": ErrorResponse}},
    summary="运行结构化量化回测",
)
def run_quant_backtest(
    request: QuantBacktestRunRequest,
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> QuantBacktestRunResponse:
    try:
        service = QuantBacktestService(db_manager)
        result = service.run_backtest(
            start_date=request.start_date,
            end_date=request.end_date,
            initial_capital=request.initial_capital,
            strategy_name=request.strategy_name,
        )
        return QuantBacktestRunResponse(
            run_id=int(result["run_id"]),
            status=str(result["status"]),
            summary=QuantBacktestSummary(**result.get("summary", {})),
            trade_plan_days=int(result.get("trade_plan_days", 0)),
            trade_count=int(result.get("trade_count", 0)),
        )
    except Exception as exc:
        logger.error(f"结构化量化回测执行失败: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"结构化量化回测执行失败: {str(exc)}"},
        )


@router.get(
    "/backtests/latest",
    response_model=QuantBacktestDetailResponse,
    responses={404: {"description": "未找到运行记录", "model": ErrorResponse}},
    summary="获取最近一次量化回测详情",
)
def get_latest_quant_backtest_detail(
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> QuantBacktestDetailResponse:
    service = QuantBacktestService(db_manager)
    detail = service.get_latest_backtest_detail()
    if detail is None:
        raise HTTPException(status_code=404, detail={"error": "not_found", "message": "暂无量化回测记录"})
    return QuantBacktestDetailResponse(**detail)


@router.get(
    "/backtests/{run_id}",
    response_model=QuantBacktestDetailResponse,
    responses={404: {"description": "未找到运行记录", "model": ErrorResponse}},
    summary="获取量化回测详情",
)
def get_quant_backtest_detail(
    run_id: int,
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> QuantBacktestDetailResponse:
    service = QuantBacktestService(db_manager)
    detail = service.get_backtest_detail(run_id)
    if detail is None:
        raise HTTPException(status_code=404, detail={"error": "not_found", "message": f"未找到回测运行: {run_id}"})
    return QuantBacktestDetailResponse(**detail)


@router.get(
    "/backtests/{run_id}/trades",
    response_model=QuantTradeListResponse,
    summary="获取量化回测逐笔交易",
)
def get_quant_backtest_trades(
    run_id: int,
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> QuantTradeListResponse:
    service = QuantBacktestService(db_manager)
    items = [QuantTradeItem(**item) for item in service.get_trades(run_id)]
    message = None
    if not items:
        message = "本次回测无成交交易，可能处于市场防守阶段或暂无符合条件的候选股。"
    return QuantTradeListResponse(run_id=run_id, message=message, items=items)


@router.get(
    "/backtests/{run_id}/equity",
    response_model=QuantEquityCurveResponse,
    summary="获取量化回测净值曲线",
)
def get_quant_backtest_equity(
    run_id: int,
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> QuantEquityCurveResponse:
    service = QuantBacktestService(db_manager)
    items = [QuantEquityPoint(**item) for item in service.get_equity_curve(run_id)]
    return QuantEquityCurveResponse(run_id=run_id, items=items)


@router.get(
    "/trade-plan",
    response_model=QuantTradePlanResponse,
    summary="获取次日交易清单",
)
def get_quant_trade_plan(
    as_of_date: Optional[date] = Query(None, description="信号日期，默认最新交易日"),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> QuantTradePlanResponse:
    service = QuantBacktestService(db_manager)
    if as_of_date is None:
        latest_trade_date = service.feature_service.repo.get_latest_trade_date()
        if latest_trade_date is None:
            as_of_date = date.today()
            return QuantTradePlanResponse(
                as_of_date=as_of_date.isoformat(),
                regime="RiskOff",
                market_score=0.0,
                max_exposure_pct=0.0,
                message="暂无量化特征数据，请先同步指数、概念板块和个股特征快照。",
                items=[],
            )
        as_of_date = latest_trade_date
    data = service.get_trade_plan(as_of_date=as_of_date)
    plan_message = _resolve_trade_plan_message(data)
    return QuantTradePlanResponse(
        as_of_date=data["as_of_date"],
        regime=data["regime"],
        market_score=float(data["market_score"]),
        max_exposure_pct=float(data["max_exposure_pct"]),
        message=plan_message,
        items=[QuantTradePlanItem(**item) for item in data.get("items", [])],
        diagnostics=QuantTradePlanDiagnostics(**(data.get("diagnostics") or {})),
    )


def _resolve_trade_plan_message(data: dict) -> Optional[str]:
    """Build human-readable explanation for empty/non-executable plans."""
    message = data.get("message")
    if message:
        return message

    regime = str(data.get("regime") or "")
    items = data.get("items") or []
    executable = [item for item in items if float(item.get("planned_position_pct", 0.0) or 0.0) > 0]
    if executable:
        return None
    if regime == "RiskOff":
        return "市场防守无新开仓（RiskOff），今日以观察和风控为主。"
    if items:
        return "当前有信号但均未通过仓位或风控约束，今日无新开仓。"
    return "暂无符合条件的候选股，今日无新开仓。"


def _resolve_sync_status_summary(service: QuantDataService) -> Dict[str, Any]:
    method_candidates = (
        "get_sync_status_summary",
        "get_quant_sync_status_summary",
        "get_sync_status",
    )
    for method_name in method_candidates:
        method = getattr(service, method_name, None)
        if callable(method):
            result = method()
            if isinstance(result, dict):
                return result
            return {}
    return {}


def _build_sync_status_response(summary: Dict[str, Any]) -> QuantSyncStatusResponse:
    summary = summary if isinstance(summary, dict) else {}
    as_of_date = _as_text(summary.get("as_of_date"))
    latest_stock_daily_date = _as_text(summary.get("latest_stock_daily_date")) or _as_text(summary.get("latest_trade_date"))
    earliest_stock_daily_date = _as_text(summary.get("earliest_stock_daily_date"))
    concept_board_coverage_count = _to_int(
        summary.get("concept_board_coverage_count", summary.get("concept_board_distinct_count"))
    )
    latest_board_date = _as_text(summary.get("latest_board_date")) or _as_text(summary.get("latest_concept_board_date"))
    latest_board_count = _to_int(summary.get("latest_board_count", summary.get("latest_concept_board_count")))
    stock_pool_size = _to_int(summary.get("stock_pool_size", summary.get("main_board_stock_pool_size")))
    main_board_stock_pool_size = _to_int(summary.get("main_board_stock_pool_size", summary.get("stock_pool_size")))
    membership_distinct_codes = _to_int(summary.get("membership_distinct_codes"))
    latest_membership_date = _as_text(summary.get("latest_membership_date"))
    latest_membership_count = _to_int(summary.get("latest_membership_count"))
    stock_daily_distinct_codes = _to_int(summary.get("stock_daily_distinct_codes"))
    stock_feature_distinct_codes = _to_int(summary.get("stock_feature_distinct_codes"))
    latest_stock_feature_date = _as_text(summary.get("latest_stock_feature_date"))
    latest_stock_feature_count = _to_int(summary.get("latest_stock_feature_count"))
    latest_index_feature_date = _as_text(summary.get("latest_index_feature_date")) or _as_text(
        summary.get("index_feature_latest_date")
    )
    index_feature_count = _to_int(summary.get("index_feature_count"))

    fallback_latest_date = as_of_date or latest_stock_daily_date
    datasets = QuantSyncStatusDatasets(
        stock_history=_build_dataset_status(
            summary.get("stock_history_sync"),
            fallback_latest_date,
            default_ready=stock_daily_distinct_codes > 0,
            default_records=stock_daily_distinct_codes,
        ),
        index_history=_build_dataset_status(
            summary.get("index_sync"),
            latest_index_feature_date or fallback_latest_date,
            default_ready=index_feature_count > 0,
            default_records=index_feature_count,
        ),
        concept_membership=_build_dataset_status(
            summary.get("membership_sync"),
            latest_membership_date or fallback_latest_date,
            default_ready=latest_membership_count > 0 or membership_distinct_codes > 0,
            default_records=latest_membership_count or membership_distinct_codes,
        ),
        concept_board_history=_build_dataset_status(
            summary.get("board_history_sync"),
            latest_board_date or fallback_latest_date,
            default_ready=concept_board_coverage_count > 0,
            default_records=concept_board_coverage_count,
        ),
        board_feature=_build_dataset_status(
            summary.get("board_feature_build"),
            latest_board_date or fallback_latest_date,
            default_ready=latest_board_count > 0,
            default_records=latest_board_count,
        ),
        stock_feature=_build_dataset_status(
            summary.get("stock_feature_build"),
            latest_stock_feature_date or fallback_latest_date,
            default_ready=latest_stock_feature_count > 0,
            default_records=latest_stock_feature_count,
        ),
    )
    errors = _normalize_errors(summary.get("errors"))
    direct_status_has_data = any(
        [
            concept_board_coverage_count > 0,
            latest_board_count > 0,
            stock_pool_size > 0,
            membership_distinct_codes > 0,
            latest_stock_feature_count > 0,
            stock_daily_distinct_codes > 0,
            index_feature_count > 0,
        ]
    )
    ready_count = sum(
        1
        for item in [
            datasets.stock_history,
            datasets.index_history,
            datasets.concept_membership,
            datasets.concept_board_history,
            datasets.board_feature,
            datasets.stock_feature,
        ]
        if item.ready
    )
    if ready_count == 0 and not direct_status_has_data:
        status = "empty"
        message = "暂无可用量化同步状态，请先执行同步任务。"
    elif errors:
        status = "partial"
        message = "已获取量化同步状态，但存在部分异常项。"
    else:
        status = "ok"
        message = "量化同步状态正常。"

    return QuantSyncStatusResponse(
        status=status,
        message=message,
        as_of_date=as_of_date,
        stock_pool_size=stock_pool_size,
        membership_distinct_codes=membership_distinct_codes,
        latest_membership_date=latest_membership_date,
        latest_membership_count=latest_membership_count,
        concept_board_coverage_count=concept_board_coverage_count,
        latest_board_date=latest_board_date,
        latest_board_count=latest_board_count,
        latest_stock_feature_date=latest_stock_feature_date,
        latest_stock_feature_count=latest_stock_feature_count,
        latest_index_feature_date=latest_index_feature_date,
        latest_stock_daily_date=latest_stock_daily_date,
        earliest_stock_daily_date=earliest_stock_daily_date,
        stock_daily_distinct_codes=stock_daily_distinct_codes,
        stock_feature_distinct_codes=stock_feature_distinct_codes,
        main_board_stock_pool_size=main_board_stock_pool_size,
        index_feature_count=index_feature_count,
        datasets=datasets,
        errors=errors,
    )


def _build_dataset_status(
    raw: Any,
    fallback_latest_date: Optional[str],
    *,
    default_ready: bool = False,
    default_records: int = 0,
) -> QuantSyncDatasetStatus:
    raw_dict = raw if isinstance(raw, dict) else {}
    errors = _normalize_errors(raw_dict.get("errors"))
    latest_trade_date = (
        _as_text(raw_dict.get("latest_trade_date"))
        or _as_text(raw_dict.get("trade_date"))
        or _as_text(raw_dict.get("as_of_date"))
        or fallback_latest_date
    )
    records = _to_int(raw_dict.get("records", raw_dict.get("count", default_records)))
    saved = _to_int(raw_dict.get("saved"))
    explicit_ready = raw_dict.get("ready")
    if isinstance(explicit_ready, bool):
        ready = explicit_ready
    else:
        ready = records > 0 or saved > 0 or bool(raw_dict.get("has_data")) or default_ready
    return QuantSyncDatasetStatus(
        ready=ready,
        model_available=bool(raw_dict.get("model_available", ready)),
        latest_trade_date=latest_trade_date,
        records=records,
        saved=saved,
        errors=errors,
    )


def _normalize_errors(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        value = raw.strip()
        return [value] if value else []
    if isinstance(raw, list):
        normalized: List[str] = []
        for item in raw:
            if isinstance(item, str):
                value = item.strip()
                if value:
                    normalized.append(value)
                continue
            if isinstance(item, dict):
                detail = item.get("error") or item.get("message") or str(item)
                detail_text = str(detail).strip()
                if detail_text:
                    normalized.append(detail_text)
                continue
            detail_text = str(item).strip()
            if detail_text:
                normalized.append(detail_text)
        return normalized
    detail_text = str(raw).strip()
    return [detail_text] if detail_text else []


def _as_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_int(value: Any) -> int:
    try:
        if value is None:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0
