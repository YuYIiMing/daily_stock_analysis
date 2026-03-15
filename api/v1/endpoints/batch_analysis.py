# -*- coding: utf-8 -*-
"""
===================================
批量分析接口
==================================

职责：
1. 提供 POST /api/v1/analysis/batch 触发自选股批量分析
2. 支持完整流程（个股+大盘复盘+通知发送）
3. 防止重复提交批量任务
"""

import logging
import uuid
from datetime import datetime
from typing import Optional, Dict, Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from api.v1.schemas.analysis import (
    BatchTaskAccepted,
    DuplicateBatchTaskErrorResponse,
)
from api.v1.schemas.common import ErrorResponse

logger = logging.getLogger(__name__)

router = APIRouter()


class BatchTaskManager:
    """批量任务管理器 - 防止重复提交"""

    _instance: Optional['BatchTaskManager'] = None
    _current_task_id: Optional[str] = None
    _current_task_status: Optional[str] = None
    _lock: bool = False

    @classmethod
    def get_instance(cls) -> 'BatchTaskManager':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def is_running(self) -> bool:
        """检查是否有批量任务正在进行"""
        return self._lock and self._current_task_status in ("pending", "processing")

    def submit_task(self, task_id: str, stock_count: int) -> bool:
        """提交新任务，如果已有任务在进行则返回False"""
        if self.is_running():
            return False
        self._lock = True
        self._current_task_id = task_id
        self._current_task_status = "pending"
        logger.info(f"[BatchTaskManager] 批量任务已提交: {task_id}, 股票数量: {stock_count}")
        return True

    def set_status(self, status: str) -> None:
        """更新任务状态"""
        self._current_task_status = status

    def complete(self, task_id: str) -> None:
        """标记任务完成"""
        if self._current_task_id == task_id:
            self._current_task_status = "completed"
            self._lock = False
            logger.info(f"[BatchTaskManager] 批量任务已完成: {task_id}")

    def fail(self, task_id: str, error: str) -> None:
        """标记任务失败"""
        if self._current_task_id == task_id:
            self._current_task_status = "failed"
            self._lock = False
            logger.error(f"[BatchTaskManager] 批量任务失败: {task_id}, error: {error}")

    def get_current_task_id(self) -> Optional[str]:
        """获取当前任务ID"""
        return self._current_task_id if self.is_running() else None


batch_task_manager = BatchTaskManager.get_instance()


@router.post(
    "/batch",
    response_model=BatchTaskAccepted,
    responses={
        202: {"description": "批量分析任务已接受", "model": BatchTaskAccepted},
        400: {"description": "参数错误或配置无效", "model": ErrorResponse},
        409: {"description": "已有批量任务在进行中", "model": DuplicateBatchTaskErrorResponse},
        500: {"description": "任务启动失败", "model": ErrorResponse},
    },
    summary="触发批量分析",
    description="触发批量分析流程，支持三种模式：stocks_only(仅个股), market_only(仅大盘), full(全部)。无视交易日检查。"
)
def trigger_batch_analysis(
    mode: str = Query("full", description="分析模式: stocks_only(仅个股), market_only(仅大盘), full(全部)")
) -> JSONResponse:
    """
    触发批量分析

    支持三种模式：
    1. stocks_only: 仅分析自选股列表中的个股
    2. market_only: 仅执行大盘复盘分析
    3. full: 完整流程（个股分析 + 大盘复盘）

    特性：
    - 无视交易日检查 (force_run=True)
    - 支持 analysis_delay 配置（默认10秒）
    - 支持 market_review_region 配置（默认'cn'）
    - 防重复提交（任何模式都互斥）

    Returns:
        BatchTaskAccepted: 任务已接受响应

    Raises:
        HTTPException: 400 - 模式参数错误或配置无效
        HTTPException: 409 - 已有批量任务在进行中
        HTTPException: 500 - 任务启动失败
    """
    # 验证模式参数
    valid_modes = ["stocks_only", "market_only", "full"]
    if mode not in valid_modes:
        error_response = ErrorResponse(
            error="invalid_mode",
            message=f"无效的分析模式: {mode}，有效模式: {', '.join(valid_modes)}"
        )
        return JSONResponse(
            status_code=400,
            content=error_response.model_dump()
        )

    task_id = f"batch_{uuid.uuid4().hex[:12]}_{mode}"

    if not batch_task_manager.submit_task(task_id, 0):
        existing_task_id = batch_task_manager.get_current_task_id()
        error_response = DuplicateBatchTaskErrorResponse(
            error="duplicate_batch_task",
            message="已有批量分析任务正在进行中，请等待完成后再提交",
            existing_task_id=existing_task_id or "unknown",
        )
        return JSONResponse(
            status_code=409,
            content=error_response.model_dump()
        )

    import threading

    def run_batch_task(mode: str):
        stock_count = 0
        try:
            batch_task_manager.set_status("processing")
            logger.info(f"[BatchAnalysis] 开始{mode}模式分析，无视交易日检查")

            from src.config import get_config
            from main import StockAnalysisPipeline
            from src.core.market_review import run_market_review

            config = get_config()
            config.refresh_stock_list()

            # 验证配置
            if mode in ["market_only", "full"] and not config.market_review_enabled:
                error_msg = "大盘复盘功能未启用，请在设置中启用 MARKET_REVIEW_ENABLED"
                logger.error(f"[BatchAnalysis] {error_msg}")
                batch_task_manager.fail(task_id, error_msg)
                return

            # 模拟 args 对象，设置 force_run=True 无视交易日检查
            class MockArgs:
                force_run = True  # 关键：无视交易日检查
                no_market_review = (mode == 'stocks_only')  # 仅个股模式跳过大盘复盘
                no_notify = False  # 发送通知
                dry_run = False
                single_notify = False
                workers = 1  # 默认并发数
                
            mock_args = MockArgs()

            # 获取股票列表（仅个股模式需要）
            stock_list = []
            if mode in ["stocks_only", "full"]:
                stock_list = config.stock_list
                if not stock_list:
                    logger.warning("[BatchAnalysis] 自选股列表为空")
                    if mode == "stocks_only":
                        batch_task_manager.complete(task_id)
                        return
                    # full模式可以继续执行大盘复盘

            stock_count = len(stock_list)
            
            # 计算合并推送
            merge_notification = (
                getattr(config, 'merge_email_notification', False)
                and config.market_review_enabled
                and mode == 'full'  # 仅full模式需要合并推送
            )

            query_id = uuid.uuid4().hex
            pipeline = StockAnalysisPipeline(
                config=config,
                query_id=query_id,
                query_source="api_batch"
            )

            results = []
            market_report = ""
            
            # 1. 执行个股分析（如果需要）
            if mode in ["stocks_only", "full"] and stock_list:
                logger.info(f"[BatchAnalysis] 开始个股分析，共 {stock_count} 只股票")
                results = pipeline.run(
                    stock_codes=stock_list,
                    dry_run=False,
                    send_notification=True,  # 始终发送推送通知
                    merge_notification=merge_notification
                )
                logger.info(f"[BatchAnalysis] 个股分析完成，成功 {len(results)} 只")

            # 2. 执行大盘复盘（如果需要）
            if mode in ["market_only", "full"]:
                # 应用 analysis_delay（默认10秒）
                analysis_delay = getattr(config, 'analysis_delay', 10)
                if analysis_delay > 0 and mode == 'full' and results:
                    logger.info(f"[BatchAnalysis] 等待 {analysis_delay} 秒后执行大盘复盘...")
                    import time
                    time.sleep(analysis_delay)
                
                # 获取 market_review_region 配置（默认'cn'）
                region = getattr(config, 'market_review_region', 'cn')
                logger.info(f"[BatchAnalysis] 使用 market_review_region: {region}")
                
                logger.info("[BatchAnalysis] 开始大盘复盘分析")
                review_result = run_market_review(
                    notifier=pipeline.notifier,
                    analyzer=pipeline.analyzer,
                    search_service=pipeline.search_service,
                    send_notification=not merge_notification,  # 非合并模式立即推送，合并模式由后续统一推送
                    merge_notification=merge_notification,
                    override_region=region,
                )
                
                if review_result:
                    market_report = review_result
                    logger.info("[BatchAnalysis] 大盘复盘完成")
                else:
                    logger.warning("[BatchAnalysis] 大盘复盘返回空结果")

            # 3. 合并推送（仅full模式且启用合并推送）
            if merge_notification and (results or market_report):
                logger.info("[BatchAnalysis] 执行合并推送（个股+大盘复盘）")
                parts = []
                if market_report:
                    parts.append(f"# 📈 大盘复盘\n\n{market_report}")
                if results:
                    dashboard_content = pipeline.notifier.generate_aggregate_report(
                        results,
                        getattr(config, 'report_type', 'simple'),
                    )
                    parts.append(f"# 🚀 个股决策仪表盘\n\n{dashboard_content}")
                if parts:
                    combined_content = "\n\n---\n\n".join(parts)
                    if pipeline.notifier.is_available():
                        if pipeline.notifier.send(combined_content, email_send_to_all=True):
                            logger.info("[BatchAnalysis] 合并推送成功")
                        else:
                            logger.warning("[BatchAnalysis] 合并推送失败")

            logger.info(f"[BatchAnalysis] {mode}模式分析完成")
            batch_task_manager.complete(task_id)

        except Exception as e:
            logger.error(f"[BatchAnalysis] 批量分析失败: {e}", exc_info=True)
            batch_task_manager.fail(task_id, str(e))

    thread = threading.Thread(target=run_batch_task, args=(mode,), daemon=True)
    thread.start()

    # 准备响应数据
    from src.config import get_config
    config = get_config()
    config.refresh_stock_list()
    
    stock_count = 0
    message = ""
    
    if mode == "stocks_only":
        stock_count = len(config.stock_list)
        message = f"个股分析任务已加入队列，将分析 {stock_count} 只自选股"
    elif mode == "market_only":
        message = "大盘复盘任务已加入队列"
    elif mode == "full":
        stock_count = len(config.stock_list)
        message = f"完整分析任务已加入队列，将分析 {stock_count} 只自选股并执行大盘复盘"

    return JSONResponse(
        status_code=202,
        content=BatchTaskAccepted(
            task_id=task_id,
            status="pending",
            stock_count=stock_count,
            message=message
        ).model_dump()
    )
