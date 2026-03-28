# -*- coding: utf-8 -*-
"""Service layer for quant backtest runs and trade plans."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, desc, select

from src.core.quant_backtest_engine import BacktestRunResult, DailyBar, QuantBacktestEngine
from src.core.quant_strategy_engine import MarketRegimeSnapshot, StrategyRiskState, TradePlanCandidate
from src.data.stock_mapping import STOCK_NAME_MAP, is_meaningful_stock_name
from src.repositories.quant_backtest_repo import QuantBacktestRepository
from src.services.quant_feature_service import QuantFeatureService
from src.storage import (
    AnalysisHistory,
    DatabaseManager,
    NewsIntel,
    QuantDailyTradeSignal,
    QuantPortfolioEquityCurve,
    QuantTradeLedger,
    StockDirectory,
    StockDaily,
)


def _resolve_stock_name(stock_code: str) -> Optional[str]:
    """Resolve stock name from local mappings and local database history only."""
    code = str(stock_code or "").strip()
    if not code:
        return None
    db = DatabaseManager.get_instance()
    try:
        with db.get_session() as session:
            directory_name = session.execute(
                select(StockDirectory.name)
                .where(and_(StockDirectory.code == code, StockDirectory.name.is_not(None), StockDirectory.name != ""))
                .limit(1)
            ).scalar_one_or_none()
            if is_meaningful_stock_name(directory_name, code):
                STOCK_NAME_MAP[code] = directory_name
                return directory_name
            static_name = STOCK_NAME_MAP.get(code)
            if is_meaningful_stock_name(static_name, code):
                return static_name
            analysis_name = session.execute(
                select(AnalysisHistory.name)
                .where(and_(AnalysisHistory.code == code, AnalysisHistory.name.is_not(None), AnalysisHistory.name != ""))
                .order_by(desc(AnalysisHistory.created_at))
                .limit(1)
            ).scalar_one_or_none()
            if is_meaningful_stock_name(analysis_name, code):
                STOCK_NAME_MAP[code] = analysis_name
                return analysis_name
            news_name = session.execute(
                select(NewsIntel.name)
                .where(and_(NewsIntel.code == code, NewsIntel.name.is_not(None), NewsIntel.name != ""))
                .order_by(desc(NewsIntel.fetched_at))
                .limit(1)
            ).scalar_one_or_none()
            if is_meaningful_stock_name(news_name, code):
                STOCK_NAME_MAP[code] = news_name
                return news_name
    except Exception:
        static_name = STOCK_NAME_MAP.get(code)
        if is_meaningful_stock_name(static_name, code):
            return static_name
    return None


def _display_stock_name(stock_code: str) -> str:
    """Return a stable user-facing stock label for trade records."""
    code = str(stock_code or "").strip()
    if not code:
        return "--"
    resolved_name = _resolve_stock_name(code)
    if is_meaningful_stock_name(resolved_name, code):
        return resolved_name
    return f"股票{code}"


class QuantBacktestService:
    """Orchestrate quant feature candidates and portfolio backtests."""

    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        feature_service: Optional[QuantFeatureService] = None,
        repository: Optional[QuantBacktestRepository] = None,
        engine: Optional[QuantBacktestEngine] = None,
    ):
        self.db = db_manager or DatabaseManager.get_instance()
        self.feature_service = feature_service or QuantFeatureService(self.db)
        self.repo = repository or QuantBacktestRepository(self.db)
        self.engine = engine or QuantBacktestEngine()

    def run_backtest(
        self,
        *,
        start_date: date,
        end_date: date,
        initial_capital: float = 1_000_000.0,
        strategy_name: str = "concept_trend_v1",
    ) -> Dict[str, Any]:
        run = self.repo.create_run(
            strategy_name=strategy_name,
            market_scope="cn_main_board",
            board_source="ths_concept",
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            status="running",
        )

        trading_dates = self._get_trading_dates(start_date, end_date)
        market_regimes: Dict[date, MarketRegimeSnapshot] = {}
        candidates_by_signal_date: Dict[date, List[TradePlanCandidate]] = {}
        signal_rows: List[QuantDailyTradeSignal] = []

        for trade_date in trading_dates:
            regime = self.feature_service.get_market_regime(trade_date)
            market_regimes[trade_date] = MarketRegimeSnapshot(
                regime=regime.regime,
                max_exposure_pct=regime.max_exposure_pct,
                score=regime.score,
            )
            candidates = self.feature_service.get_trade_candidates(trade_date)
            candidates_by_signal_date[trade_date] = [
                TradePlanCandidate(
                    signal_date=trade_date,
                    code=item.code,
                    board_code=item.board_code,
                    board_name=item.board_name,
                    stage=item.stage,
                    entry_module=item.entry_module,
                    signal_score=item.signal_score,
                    planned_entry_price=float(item.planned_entry_price or 0.0),
                    initial_stop_price=float(item.initial_stop_price or 0.0),
                    reason=item.reason,
                )
                for item in candidates
            ]
            for item in candidates:
                signal_rows.append(
                    QuantDailyTradeSignal(
                        run_id=run.id,
                        trade_date=trade_date,
                        code=item.code,
                        board_code=item.board_code,
                        board_name=item.board_name,
                        stage=item.stage,
                        entry_module=item.entry_module,
                        direction="long",
                        signal_score=item.signal_score,
                        planned_entry_price=item.planned_entry_price,
                        initial_stop_price=item.initial_stop_price,
                        planned_position_pct=0.0,
                        reason_json=json.dumps(item.reason, ensure_ascii=False),
                    )
                )

        bars_by_code = self._load_price_bars(start_date, end_date)
        result = self.engine.run(
            trading_dates=trading_dates,
            market_regimes=market_regimes,
            candidates_by_signal_date=candidates_by_signal_date,
            bars_by_code=bars_by_code,
            initial_capital=initial_capital,
        )

        merged_trades = self._merge_trade_rows(result.trades)
        normalized_summary = self._normalize_summary_with_merged_trades(result.summary, merged_trades)
        self.repo.save_signals(run.id, signal_rows)
        self.repo.append_trades(run.id, merged_trades)
        self.repo.save_equity_curve(run.id, [self._equity_to_row(run.id, point) for point in result.equity_curve])
        self.repo.update_run_status(run.id, status="completed", summary=normalized_summary)

        return {
            "run_id": run.id,
            "status": "completed",
            "summary": normalized_summary,
            "trade_plan_days": len(result.trade_plans),
            "trade_count": len(merged_trades),
        }

    def get_backtest_detail(self, run_id: int) -> Optional[Dict[str, Any]]:
        run = self.repo.get_run(run_id)
        if run is None:
            return None
        return self._run_to_detail_dict(run)

    def get_latest_backtest_detail(self) -> Optional[Dict[str, Any]]:
        run = self.repo.get_latest_run()
        if run is None:
            return None
        return self._run_to_detail_dict(run)

    @staticmethod
    def _run_to_detail_dict(run) -> Dict[str, Any]:
        summary = {}
        if run.summary_json:
            try:
                summary = json.loads(run.summary_json)
            except Exception:
                summary = {}
        return {
            "run_id": run.id,
            "strategy_name": run.strategy_name,
            "market_scope": run.market_scope,
            "board_source": run.board_source,
            "start_date": run.start_date.isoformat(),
            "end_date": run.end_date.isoformat(),
            "initial_capital": run.initial_capital,
            "status": run.status,
            "summary": summary,
        }

    def get_trades(self, run_id: int) -> List[Dict[str, Any]]:
        rows = self.repo.list_trades(run_id)
        return [self._trade_row_to_dict(row) for row in rows]

    def get_equity_curve(self, run_id: int) -> List[Dict[str, Any]]:
        return [self._equity_row_to_dict(row) for row in self.repo.get_equity_curve(run_id)]

    def get_trade_plan(self, *, as_of_date: date) -> Dict[str, Any]:
        regime = self.feature_service.get_market_regime(as_of_date)
        candidates = self.feature_service.get_trade_candidates(as_of_date)
        diagnostics = self._build_trade_plan_diagnostics(as_of_date=as_of_date, candidates=candidates)
        plan_items = self.engine.strategy_engine.build_trade_plan(
            candidates=[
                TradePlanCandidate(
                    signal_date=as_of_date,
                    code=item.code,
                    board_code=item.board_code,
                    board_name=item.board_name,
                    stage=item.stage,
                    entry_module=item.entry_module,
                    signal_score=item.signal_score,
                    planned_entry_price=float(item.planned_entry_price or 0.0),
                    initial_stop_price=float(item.initial_stop_price or 0.0),
                    reason=item.reason,
                )
                for item in candidates
            ],
            market_regime=MarketRegimeSnapshot(regime=regime.regime, max_exposure_pct=regime.max_exposure_pct, score=regime.score),
            risk_state=StrategyRiskState(),
        )
        return {
            "as_of_date": as_of_date.isoformat(),
            "regime": regime.regime,
            "market_score": regime.score,
            "max_exposure_pct": regime.max_exposure_pct,
            "items": [
                {
                    "code": item.code,
                    "board_code": item.board_code,
                    "board_name": item.board_name,
                    "stage": item.stage,
                    "entry_module": item.entry_module,
                    "signal_score": item.signal_score,
                    "planned_entry_price": item.planned_entry_price,
                    "initial_stop_price": item.initial_stop_price,
                    "planned_position_pct": item.planned_position_pct,
                    "blocked_reason": item.blocked_reason,
                    "reason": item.reason,
                }
                for item in plan_items
            ],
            "diagnostics": diagnostics,
        }

    def _build_trade_plan_diagnostics(self, *, as_of_date: date, candidates: List[Any]) -> Dict[str, Any]:
        stock_rows = self.feature_service.repo.list_stock_features(trade_date=as_of_date, eligible_only=True)
        board_map = self.feature_service.get_board_stage_map(as_of_date)
        board_name_exact_map, board_name_alias_map = self.feature_service.build_board_name_lookup(board_map)
        recent_board_name_exact_map, recent_board_name_alias_map = self.feature_service.build_recent_board_name_lookup(as_of_date)

        mapped_stage_distribution: Counter[str] = Counter()
        missing_board_counter: Counter[str] = Counter()
        trade_allowed_counter: Dict[tuple, int] = defaultdict(int)
        stage_ready_distribution: Counter[str] = Counter()
        setup_blocker_counts: Counter[str] = Counter()
        same_day_board_match_count = 0
        recent_board_fallback_count = 0
        trade_allowed_stock_count = 0
        stage_ready_stock_count = 0

        for row in stock_rows:
            board_meta, match_source = self.feature_service.resolve_board_meta(
                board_code=row.board_code,
                board_name=row.board_name,
                board_map=board_map,
                board_name_exact_lookup=board_name_exact_map,
                board_name_alias_lookup=board_name_alias_map,
                recent_board_name_exact_lookup=recent_board_name_exact_map,
                recent_board_name_alias_lookup=recent_board_name_alias_map,
            )
            if match_source in {"same_day", "same_day_alias"}:
                same_day_board_match_count += 1
            elif match_source == "recent_fallback":
                recent_board_fallback_count += 1
            else:
                missing_board_counter[str(row.board_name or "未标记板块")] += 1
                continue

            stage = str(board_meta.get("stage") or "IGNORE")
            mapped_stage_distribution[stage] += 1
            if stage in {"EMERGING", "TREND", "CLIMAX"}:
                stage_ready_stock_count += 1
            if bool(board_meta.get("trade_allowed", False)):
                trade_allowed_stock_count += 1
                if stage in {"EMERGING", "TREND", "CLIMAX"}:
                    stage_ready_distribution[stage] += 1
                trade_allowed_counter[
                    (
                        str(board_meta.get("board_name") or row.board_name or ""),
                        stage,
                        int(board_meta.get("theme_score") or 0),
                        str(board_meta.get("feature_trade_date") or ""),
                    )
                ] += 1

        missing_board_feature_count = sum(missing_board_counter.values())
        candidate_stock_count = len(candidates)
        if candidate_stock_count == 0:
            if stage_ready_distribution.get("CLIMAX", 0) > 0:
                setup_blocker_counts["climax_no_trigger"] = int(stage_ready_distribution.get("CLIMAX", 0))
                setup_blocker_counts["climax_setup_not_ready"] = int(stage_ready_distribution.get("CLIMAX", 0))
            if stage_ready_distribution.get("EMERGING", 0) > 0:
                setup_blocker_counts["emerging_setup_not_ready"] = int(stage_ready_distribution.get("EMERGING", 0))
            if stage_ready_distribution.get("TREND", 0) > 0:
                setup_blocker_counts["trend_setup_not_ready"] = int(stage_ready_distribution.get("TREND", 0))

        if candidate_stock_count > 0:
            primary_blocker = "candidates_ready"
            summary = "已有候选股通过市场、板块和个股条件，可继续按计划执行。"
        elif stage_ready_stock_count > 0 and stage_ready_distribution.get("CLIMAX", 0) >= max(stage_ready_stock_count * 0.7, 1):
            primary_blocker = "late_stage_no_trade"
            summary = "多数阶段就绪股票已处于后期/CLIMAX，当前版本仅在强趋势回踩或弱转强时参与，今日未触发后期入场。"
        elif stage_ready_stock_count == 0 and missing_board_feature_count > 0:
            primary_blocker = "board_stage_and_feature_gap"
            summary = "当前已映射板块均未进入可交易阶段，且仍有部分板块特征缺口。"
        elif stage_ready_stock_count == 0:
            primary_blocker = "board_stage_not_ready"
            summary = "当前已映射板块尚未进入初期/中期可交易阶段，策略选择继续等待。"
        elif trade_allowed_stock_count > 0:
            primary_blocker = "stock_setup_not_ready"
            summary = "市场和部分板块允许交易，但个股未满足突破/回调入场条件。"
        elif missing_board_feature_count > 0:
            primary_blocker = "board_feature_gap"
            summary = "候选为空同时存在板块特征缺口，需结合缺失板块范围判断是否属于假空仓。"
        elif mapped_stage_distribution.get("IGNORE", 0) > 0:
            primary_blocker = "board_stage_not_ready"
            summary = "当前有股票进入观察池，但板块阶段多处于震荡/IGNORE，策略选择继续等待。"
        elif not stock_rows:
            primary_blocker = "no_eligible_universe"
            summary = "当前没有股票通过基础股票池过滤，暂不生成候选。"
        else:
            primary_blocker = "no_signal"
            summary = "当前无候选股，说明市场、板块和个股条件未形成共振。"

        top_missing_boards = [
            {
                "board_name": board_name,
                "stock_count": stock_count,
                "stage": None,
                "theme_score": None,
                "feature_trade_date": None,
            }
            for board_name, stock_count in missing_board_counter.most_common(8)
        ]
        trade_allowed_boards = [
            {
                "board_name": board_name,
                "stock_count": stock_count,
                "stage": stage,
                "theme_score": theme_score,
                "feature_trade_date": feature_trade_date or None,
            }
            for (board_name, stage, theme_score, feature_trade_date), stock_count in sorted(
                trade_allowed_counter.items(),
                key=lambda item: (-item[1], item[0][0]),
            )[:8]
        ]

        return {
            "eligible_stock_count": len(stock_rows),
            "same_day_board_match_count": same_day_board_match_count,
            "recent_board_fallback_count": recent_board_fallback_count,
            "missing_board_feature_count": missing_board_feature_count,
            "trade_allowed_stock_count": trade_allowed_stock_count,
            "stage_ready_stock_count": stage_ready_stock_count,
            "candidate_stock_count": candidate_stock_count,
            "mapped_stage_distribution": dict(mapped_stage_distribution),
            "stage_ready_distribution": dict(stage_ready_distribution),
            "setup_blocker_counts": dict(setup_blocker_counts),
            "top_missing_boards": top_missing_boards,
            "trade_allowed_boards": trade_allowed_boards,
            "primary_blocker": primary_blocker,
            "summary": summary,
        }

    def _get_trading_dates(self, start_date: date, end_date: date) -> List[date]:
        with self.db.get_session() as session:
            rows = session.execute(
                select(StockDaily.date)
                .where(and_(StockDaily.date >= start_date, StockDaily.date <= end_date))
                .distinct()
                .order_by(StockDaily.date)
            ).scalars().all()
            return list(rows)

    def _load_price_bars(self, start_date: date, end_date: date) -> Dict[str, Dict[date, DailyBar]]:
        with self.db.get_session() as session:
            rows = session.execute(
                select(StockDaily)
                .where(and_(StockDaily.date >= start_date, StockDaily.date <= end_date))
                .order_by(StockDaily.code, StockDaily.date)
            ).scalars().all()
        bars: Dict[str, Dict[date, DailyBar]] = defaultdict(dict)
        prev_close_by_code: Dict[str, float] = {}
        for row in rows:
            prev_close = prev_close_by_code.get(row.code, row.close or 0.0)
            upper_limit = round(prev_close * 1.1, 2) if prev_close else None
            lower_limit = round(prev_close * 0.9, 2) if prev_close else None
            bars[row.code][row.date] = DailyBar(
                trade_date=row.date,
                open=float(row.open or row.close or 0.0),
                high=float(row.high or row.close or 0.0),
                low=float(row.low or row.close or 0.0),
                close=float(row.close or 0.0),
                upper_limit_price=upper_limit,
                lower_limit_price=lower_limit,
            )
            prev_close_by_code[row.code] = float(row.close or prev_close or 0.0)
        return bars

    @staticmethod
    def _trade_to_row(run_id: int, trade) -> QuantTradeLedger:
        return QuantTradeLedger(
            run_id=run_id,
            code=trade.code,
            board_code=trade.board_code,
            board_name=trade.board_name,
            entry_date=trade.entry_date,
            exit_date=trade.exit_date,
            entry_price=trade.entry_price,
            exit_price=trade.exit_price,
            shares=trade.shares,
            entry_module=trade.entry_module,
            stage=trade.stage,
            status="closed",
            pnl_pct=trade.pnl_pct,
            pnl_amount=trade.pnl_amount,
            exit_reason=trade.exit_reason,
            blocked_exit=trade.blocked_exit,
        )

    def _merge_trade_rows(self, trades) -> List[QuantTradeLedger]:
        merged: Dict[tuple, QuantTradeLedger] = {}
        for trade in trades:
            key = (trade.code, trade.entry_date, trade.entry_module)
            row = self._trade_to_row(0, trade)
            existing = merged.get(key)
            if existing is None:
                merged[key] = row
                continue
            total_shares = int(existing.shares or 0) + int(row.shares or 0)
            if total_shares <= 0:
                continue
            existing.exit_date = max(filter(None, [existing.exit_date, row.exit_date]))
            existing.exit_price = row.exit_price
            existing.shares = total_shares
            existing.pnl_amount = round(float(existing.pnl_amount or 0.0) + float(row.pnl_amount or 0.0), 2)
            existing.pnl_pct = round((existing.pnl_amount / (existing.entry_price * total_shares) * 100.0), 2) if existing.entry_price else 0.0
            existing.exit_reason = row.exit_reason
            existing.status = row.status
            existing.blocked_exit = bool(existing.blocked_exit or row.blocked_exit)
        return list(merged.values())

    @staticmethod
    def _normalize_summary_with_merged_trades(
        summary: Dict[str, Any],
        merged_trades: List[QuantTradeLedger],
    ) -> Dict[str, float]:
        normalized = dict(summary or {})
        trade_count = len(merged_trades)
        win_count = sum(1 for row in merged_trades if float(row.pnl_amount or 0.0) > 0.0)
        loss_count = sum(1 for row in merged_trades if float(row.pnl_amount or 0.0) < 0.0)
        win_rate_pct = (win_count / trade_count * 100.0) if trade_count > 0 else 0.0
        normalized["trade_count"] = float(trade_count)
        normalized["win_count"] = float(win_count)
        normalized["loss_count"] = float(loss_count)
        normalized["win_rate_pct"] = round(win_rate_pct, 2)
        return normalized

    @staticmethod
    def _equity_to_row(run_id: int, point) -> QuantPortfolioEquityCurve:
        return QuantPortfolioEquityCurve(
            run_id=run_id,
            trade_date=point.trade_date,
            cash=point.cash,
            market_value=point.market_value,
            equity=point.equity,
            drawdown_pct=point.drawdown_pct,
            exposure_pct=point.exposure_pct,
        )

    @staticmethod
    def _trade_row_to_dict(row: QuantTradeLedger) -> Dict[str, Any]:
        code = str(row.code or "")
        shares = int(row.shares or 0)
        entry_price = float(row.entry_price) if row.entry_price is not None else None
        exit_price = float(row.exit_price) if row.exit_price is not None else None
        return {
            "id": row.id,
            "code": code,
            "stock_name": _display_stock_name(code),
            "board_code": row.board_code,
            "board_name": row.board_name,
            "entry_date": row.entry_date.isoformat() if row.entry_date else None,
            "exit_date": row.exit_date.isoformat() if row.exit_date else None,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "entry_amount": round(entry_price * shares, 2) if entry_price is not None else None,
            "exit_amount": round(exit_price * shares, 2) if exit_price is not None else None,
            "shares": shares,
            "entry_module": row.entry_module,
            "stage": row.stage,
            "status": row.status,
            "pnl_pct": row.pnl_pct,
            "pnl_amount": row.pnl_amount,
            "exit_reason": row.exit_reason,
            "blocked_exit": row.blocked_exit,
        }

    @staticmethod
    def _equity_row_to_dict(row: QuantPortfolioEquityCurve) -> Dict[str, Any]:
        return {
            "trade_date": row.trade_date.isoformat() if row.trade_date else None,
            "cash": row.cash,
            "market_value": row.market_value,
            "equity": row.equity,
            "drawdown_pct": row.drawdown_pct,
            "exposure_pct": row.exposure_pct,
        }
