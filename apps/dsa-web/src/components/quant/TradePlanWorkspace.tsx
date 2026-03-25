import type React from 'react';
import { AlertTriangle, ArrowUpRight, ShieldCheck } from 'lucide-react';
import { Badge, Button, Card } from '../common';
import type { QuantTradeItem, QuantTradePlanDiagnostics, QuantTradePlanItem } from '../../types/quantStrategy';
import { getBlockedReasonLabel, getModuleLabel, getStageLabel } from './labels';

interface TradePlanWorkspaceProps {
  planItems: QuantTradePlanItem[];
  planDiagnostics?: QuantTradePlanDiagnostics | null;
  trades: QuantTradeItem[];
  onOpenTradeDetail: (trade: QuantTradeItem) => void;
}

function moduleTone(module: string): 'aurora' | 'nebula' | 'warning' | 'default' {
  if (module === 'BREAKOUT') return 'aurora';
  if (module === 'PULLBACK') return 'nebula';
  if (module === 'LATE_WEAK_TO_STRONG') return 'warning';
  return 'default';
}

function stageTone(stage: string): 'success' | 'warning' | 'danger' | 'default' {
  if (stage === 'TREND' || stage === 'EMERGING') return 'success';
  if (stage === 'CLIMAX') return 'warning';
  if (stage === 'IGNORE') return 'danger';
  return 'default';
}

function getPrimaryBlockerLabel(primaryBlocker?: string | null): string {
  if (primaryBlocker === 'candidates_ready') return '已有候选';
  if (primaryBlocker === 'stock_setup_not_ready') return '个股形态未触发';
  if (primaryBlocker === 'late_stage_no_trade') return '后期默认不做';
  if (primaryBlocker === 'board_feature_gap') return '板块特征有缺口';
  if (primaryBlocker === 'board_stage_and_feature_gap') return '阶段未到位且有缺口';
  if (primaryBlocker === 'board_stage_not_ready') return '板块阶段未到位';
  if (primaryBlocker === 'no_eligible_universe') return '无合格股票池';
  if (primaryBlocker === 'no_signal') return '暂无有效信号';
  return '待观察';
}

function getStageCount(distribution: Record<string, number> | undefined, stage: string): number {
  if (!distribution) return 0;
  return Number(distribution[stage] ?? 0);
}

function getBlockerCount(counts: Record<string, number> | undefined, key: string): number {
  if (!counts) return 0;
  return Number(counts[key] ?? 0);
}

function buildEmptyPlanHeadline(planDiagnostics?: QuantTradePlanDiagnostics | null): string {
  if (!planDiagnostics) {
    return '当前没有符合条件的候选，建议保持防守并等待下一批信号。';
  }
  const eligibleStockCount = planDiagnostics.eligibleStockCount ?? 0;
  const tradeAllowedStockCount = planDiagnostics.tradeAllowedStockCount ?? 0;
  const stageReadyStockCount = planDiagnostics.stageReadyStockCount ?? 0;
  const candidateStockCount = planDiagnostics.candidateStockCount ?? 0;
  return `今天共有 ${eligibleStockCount} 只股票进入观察池，其中 ${tradeAllowedStockCount} 只所在板块允许交易，${stageReadyStockCount} 只进入阶段就绪，但最终只有 ${candidateStockCount} 只转化为可执行候选。`;
}

function buildEmptyPlanFocus(planDiagnostics?: QuantTradePlanDiagnostics | null): string {
  if (!planDiagnostics?.summary) return '当前没有符合条件的候选，建议保持防守并等待下一批信号。';
  return planDiagnostics.summary;
}

export const TradePlanWorkspace: React.FC<TradePlanWorkspaceProps> = ({
  planItems,
  planDiagnostics,
  trades,
  onOpenTradeDetail,
}) => {
  const climaxCount = getStageCount(planDiagnostics?.stageReadyDistribution, 'CLIMAX');
  const trendCount = getStageCount(planDiagnostics?.stageReadyDistribution, 'TREND');
  const emergingCount = getStageCount(planDiagnostics?.stageReadyDistribution, 'EMERGING');
  const climaxBlocked = getBlockerCount(planDiagnostics?.setupBlockerCounts, 'climax_no_trigger');
  const trendBlocked = getBlockerCount(planDiagnostics?.setupBlockerCounts, 'trend_setup_not_ready');
  const emergingBlocked = getBlockerCount(planDiagnostics?.setupBlockerCounts, 'emerging_setup_not_ready');

  return (
    <div className="grid gap-4 xl:grid-cols-[1.1fr_0.9fr]">
      <Card variant="data" padding="md" className="overflow-hidden">
        <div className="mb-4 flex items-center justify-between">
          <div>
            <p className="text-sm font-semibold text-content-primary">下一交易日计划</p>
            <p className="text-xs text-content-tertiary mt-1">先看结论，再看卡点，最后再看细节</p>
          </div>
          <Badge variant="default">{planItems.length} 个候选</Badge>
        </div>

        {planItems.length === 0 ? (
          <div className="space-y-4">
            <div className="rounded-xl border border-white/8 bg-black/15 p-5">
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div className="max-w-2xl">
                  <p className="text-sm font-semibold text-content-primary">今天为什么没有计划单</p>
                  <p className="mt-2 text-sm text-content-secondary">{buildEmptyPlanHeadline(planDiagnostics)}</p>
                  <p className="mt-2 text-sm text-content-tertiary">{buildEmptyPlanFocus(planDiagnostics)}</p>
                </div>
                <Badge variant="warning">{getPrimaryBlockerLabel(planDiagnostics?.primaryBlocker)}</Badge>
              </div>
            </div>

            {planDiagnostics ? (
              <div className="space-y-3">
                <div className="rounded-xl border border-white/8 bg-black/10 p-4">
                  <p className="text-xs font-semibold text-content-primary">三层筛选路径</p>
                  <div className="mt-3 grid gap-2 md:grid-cols-4 text-xs">
                    <div className="rounded-lg bg-white/5 p-3">
                      <span className="text-content-quaternary">基础池</span>
                      <p className="mt-1 text-lg font-semibold text-content-primary">{planDiagnostics.eligibleStockCount ?? 0}</p>
                      <p className="mt-1 text-content-tertiary">先通过均线、涨幅、流动性等基础过滤</p>
                    </div>
                    <div className="rounded-lg bg-white/5 p-3">
                      <span className="text-content-quaternary">板块允许</span>
                      <p className="mt-1 text-lg font-semibold text-content-primary">{planDiagnostics.tradeAllowedStockCount ?? 0}</p>
                      <p className="mt-1 text-content-tertiary">所在概念板块达到允许交易条件</p>
                    </div>
                    <div className="rounded-lg bg-white/5 p-3">
                      <span className="text-content-quaternary">阶段就绪</span>
                      <p className="mt-1 text-lg font-semibold text-content-primary">{planDiagnostics.stageReadyStockCount ?? 0}</p>
                      <p className="mt-1 text-content-tertiary">板块进入初期、中期或后期</p>
                    </div>
                    <div className="rounded-lg bg-white/5 p-3">
                      <span className="text-content-quaternary">最终候选</span>
                      <p className="mt-1 text-lg font-semibold text-content-primary">{planDiagnostics.candidateStockCount ?? 0}</p>
                      <p className="mt-1 text-content-tertiary">同时满足个股买点与风险约束</p>
                    </div>
                  </div>
                </div>

                <div className="grid gap-3 md:grid-cols-3">
                  <div className="rounded-xl border border-white/8 bg-black/10 p-4">
                    <p className="text-xs font-semibold text-content-primary">阶段卡点</p>
                    <p className="mt-2 text-sm text-content-secondary">
                      当前阶段就绪股票里，后期 {climaxCount} 只，中期 {trendCount} 只，初期 {emergingCount} 只。
                    </p>
                    <div className="mt-3 flex flex-wrap gap-2">
                      {climaxCount > 0 ? <Badge variant="warning">后期 {climaxCount} 只</Badge> : null}
                      {trendCount > 0 ? <Badge variant="success">中期 {trendCount} 只</Badge> : null}
                      {emergingCount > 0 ? <Badge variant="success">初期 {emergingCount} 只</Badge> : null}
                    </div>
                  </div>

                  <div className="rounded-xl border border-white/8 bg-black/10 p-4">
                    <p className="text-xs font-semibold text-content-primary">个股买点卡点</p>
                    <p className="mt-2 text-sm text-content-secondary">
                      后期默认不做 {climaxBlocked} 只；中期形态未成 {trendBlocked} 只；初期形态未成 {emergingBlocked} 只。
                    </p>
                    <div className="mt-3 flex flex-wrap gap-2">
                      {climaxBlocked > 0 ? <Badge variant="warning">后期默认不做 {climaxBlocked}</Badge> : null}
                      {trendBlocked > 0 ? <Badge variant="default">中期形态未成 {trendBlocked}</Badge> : null}
                      {emergingBlocked > 0 ? <Badge variant="default">初期形态未成 {emergingBlocked}</Badge> : null}
                    </div>
                  </div>

                  <div className="rounded-xl border border-white/8 bg-black/10 p-4">
                    <p className="text-xs font-semibold text-content-primary">数据完整度</p>
                    <p className="mt-2 text-sm text-content-secondary">
                      当日板块命中 {planDiagnostics.sameDayBoardMatchCount ?? 0} 只，近端回退 {planDiagnostics.recentBoardFallbackCount ?? 0} 只，仍有 {planDiagnostics.missingBoardFeatureCount ?? 0} 只存在板块特征缺口。
                    </p>
                    <div className="mt-3 flex flex-wrap gap-2">
                      <Badge variant="default">当日命中 {(planDiagnostics.sameDayBoardMatchCount ?? 0).toString()}</Badge>
                      <Badge variant="default">近端回退 {(planDiagnostics.recentBoardFallbackCount ?? 0).toString()}</Badge>
                      {(planDiagnostics.missingBoardFeatureCount ?? 0) > 0 ? (
                        <Badge variant="warning">缺口 {(planDiagnostics.missingBoardFeatureCount ?? 0).toString()}</Badge>
                      ) : null}
                    </div>
                  </div>
                </div>

                <details className="rounded-xl border border-white/8 bg-black/10 p-4">
                  <summary className="cursor-pointer list-none text-xs font-semibold text-content-primary">
                    展开看详细板块分布
                  </summary>
                  {planDiagnostics.tradeAllowedBoards && planDiagnostics.tradeAllowedBoards.length > 0 ? (
                    <div className="mt-4">
                      <p className="text-[11px] uppercase tracking-wider text-content-quaternary">允许交易板块</p>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {planDiagnostics.tradeAllowedBoards.map((board) => (
                          <Badge key={`${board.boardName}-${board.featureTradeDate ?? 'na'}`} variant={stageTone(board.stage ?? 'IGNORE')}>
                            {board.boardName} · {getStageLabel(board.stage ?? 'IGNORE')} · {board.stockCount} 只
                          </Badge>
                        ))}
                      </div>
                    </div>
                  ) : null}
                  {planDiagnostics.topMissingBoards && planDiagnostics.topMissingBoards.length > 0 ? (
                    <div className="mt-4">
                      <p className="text-[11px] uppercase tracking-wider text-content-quaternary">缺失板块特征 Top</p>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {planDiagnostics.topMissingBoards.map((board) => (
                          <Badge key={board.boardName} variant="default">
                            {board.boardName} · {board.stockCount} 只
                          </Badge>
                        ))}
                      </div>
                    </div>
                  ) : null}
                </details>
              </div>
            ) : null}
          </div>
        ) : (
          <div className="space-y-3">
            {planItems.slice(0, 6).map((item) => (
              <div
                key={`${item.code}-${item.entryModule}`}
                className="rounded-xl border border-white/10 bg-black/15 p-3 transition-all duration-200 hover:border-white/20"
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div>
                    <p className="text-base font-semibold text-content-primary">{item.code}</p>
                    <p className="text-xs text-content-tertiary">{item.boardName ?? item.boardCode ?? '--'}</p>
                  </div>
                  <div className="flex items-center gap-2">
                    <Badge variant={moduleTone(item.entryModule)}>{getModuleLabel(item.entryModule)}</Badge>
                    <Badge variant={stageTone(item.stage)}>{getStageLabel(item.stage)}</Badge>
                  </div>
                </div>
                <div className="mt-3 grid grid-cols-3 gap-2 text-xs">
                  <div className="rounded-lg bg-white/5 p-2">
                    <span className="text-content-quaternary">计划买点</span>
                    <p className="mt-1 font-mono text-content-primary">{item.plannedEntryPrice?.toFixed(2) ?? '--'}</p>
                  </div>
                  <div className="rounded-lg bg-white/5 p-2">
                    <span className="text-content-quaternary">止损位</span>
                    <p className="mt-1 font-mono text-semantic-danger">{item.initialStopPrice?.toFixed(2) ?? '--'}</p>
                  </div>
                  <div className="rounded-lg bg-white/5 p-2">
                    <span className="text-content-quaternary">信号分</span>
                    <p className="mt-1 font-mono text-semantic-success">{item.signalScore?.toFixed(2) ?? '--'}</p>
                  </div>
                </div>
                <div className="mt-3 flex items-center justify-between gap-2">
                  <span className="text-xs text-content-tertiary">
                    {item.blockedReason ? `阻断原因：${getBlockedReasonLabel(item.blockedReason)}` : '已就绪，可按次日开盘计划执行'}
                  </span>
                  <span className="text-xs text-content-secondary">
                    计划仓位 {item.plannedPositionPct?.toFixed(2) ?? '0.00'}%
                  </span>
                </div>
              </div>
            ))}
          </div>
        )}
      </Card>

      <Card variant="data" padding="md">
        <div className="mb-4 flex items-center justify-between">
          <div>
            <p className="text-sm font-semibold text-content-primary">已执行交易</p>
            <p className="mt-1 text-xs text-content-tertiary">回测执行轨迹与出场原因</p>
          </div>
          <Badge variant="default">{trades.length} 笔交易</Badge>
        </div>

        {trades.length === 0 ? (
          <div className="rounded-xl border border-white/8 bg-black/15 p-6 text-sm text-content-tertiary">
            暂无交易记录，运行一次结构化回测后会显示逐笔明细。
          </div>
        ) : (
          <div className="overflow-hidden rounded-xl border border-white/8">
            <div className="hidden grid-cols-[0.9fr_0.8fr_0.9fr_0.8fr_0.8fr] gap-2 bg-white/5 px-3 py-2 text-[11px] uppercase tracking-wider text-content-quaternary md:grid">
              <span>股票</span>
              <span>模块</span>
              <span>进出场</span>
              <span>买卖金额 / 收益</span>
              <span className="text-right">操作</span>
            </div>
            <div className="max-h-[420px] overflow-y-auto">
              {trades.map((trade) => (
                <div
                  key={`${trade.code}-${trade.entryDate}-${trade.exitDate ?? 'open'}`}
                  className="grid grid-cols-1 gap-2 border-t border-white/5 px-3 py-3 text-xs md:grid-cols-[0.9fr_0.8fr_0.9fr_0.8fr_0.8fr]"
                >
                  <div>
                    <p className="font-semibold text-content-primary">{trade.stockName ?? trade.code}</p>
                    <p className="mt-1 text-content-tertiary">
                      {trade.code} · {getStageLabel(trade.stage)}
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    <Badge variant={moduleTone(trade.entryModule)}>{getModuleLabel(trade.entryModule)}</Badge>
                  </div>
                  <div className="font-mono text-content-secondary">
                    <p>{trade.entryDate}</p>
                    <p className="mt-1">{trade.exitDate ?? '--'}</p>
                  </div>
                  <div>
                    <p className="font-mono text-content-secondary">
                      买{' '}
                      {trade.entryAmount?.toLocaleString(undefined, {
                        minimumFractionDigits: 2,
                        maximumFractionDigits: 2,
                      }) ?? '--'}
                    </p>
                    <p className="mt-1 font-mono text-content-secondary">
                      卖{' '}
                      {trade.exitAmount?.toLocaleString(undefined, {
                        minimumFractionDigits: 2,
                        maximumFractionDigits: 2,
                      }) ?? '--'}
                    </p>
                    <p
                      className={`mt-1 font-semibold ${
                        Number(trade.pnlPct ?? 0) >= 0 ? 'text-semantic-success' : 'text-semantic-danger'
                      }`}
                    >
                      {trade.pnlPct == null ? '--' : `${trade.pnlPct.toFixed(2)}%`} ·{' '}
                      {trade.pnlAmount?.toLocaleString(undefined, {
                        minimumFractionDigits: 2,
                        maximumFractionDigits: 2,
                      }) ?? '--'}
                    </p>
                  </div>
                  <div className="flex items-center justify-start gap-2 md:justify-end">
                    {trade.blockedExit ? (
                      <span className="inline-flex items-center gap-1 text-semantic-warning">
                        <AlertTriangle className="h-3.5 w-3.5" />
                        卖出受阻
                      </span>
                    ) : (
                      <span className="inline-flex items-center gap-1 text-semantic-success">
                        <ShieldCheck className="h-3.5 w-3.5" />
                        正常成交
                      </span>
                    )}
                    <Button variant="ghost" size="sm" onClick={() => onOpenTradeDetail(trade)}>
                      <ArrowUpRight className="h-3.5 w-3.5" />
                      详情
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </Card>
    </div>
  );
};
