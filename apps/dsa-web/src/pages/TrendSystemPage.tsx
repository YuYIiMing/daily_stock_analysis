import type React from 'react';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { RefreshCcw, ShieldAlert } from 'lucide-react';
import { ApiErrorAlert, Badge, Button, Card } from '../components/common';
import { getParsedApiError, type ParsedApiError } from '../api/error';
import { trendSystemApi } from '../api/trendSystem';
import type {
  CandidateItem,
  PortfolioPositionItem,
  PortfolioResponse,
  PositionRecord,
  RiskMode,
  SectorDecisionItem,
  Stage,
  TrendAlert,
  TrendDiagnosticsResponse,
  TrendPlanResponse,
  TrendStatusResponse,
  TrendSystemOverviewResponse,
} from '../types/trendSystem';

const STAGE_LABEL: Record<Stage, string> = {
  initial: '初期',
  middle: '中期',
  late: '后期',
  choppy: '震荡',
};

const stageVariant = (stage: Stage) => {
  switch (stage) {
    case 'initial':
      return 'aurora';
    case 'middle':
      return 'success';
    case 'late':
      return 'warning';
    default:
      return 'default';
  }
};

const riskVariant = (mode: RiskMode) => {
  switch (mode) {
    case 'cooldown':
      return 'danger';
    case 'degraded_system':
    case 'breakout_paused':
      return 'warning';
    case 'elite_disabled':
    case 'reduced_risk':
      return 'nebula';
    default:
      return 'success';
  }
};

const actionVariant = (action: PortfolioPositionItem['action']) => {
  switch (action) {
    case 'exit':
      return 'danger';
    case 'reduce':
      return 'warning';
    default:
      return 'default';
  }
};

const boolText = (value: boolean) => (value ? '满足' : '未满足');

const snapshotStateText = (status?: string) => {
  switch (status) {
    case 'ready':
      return '快照可用';
    case 'stale':
      return '历史快照';
    case 'failed':
      return '重建失败';
    case 'missing':
      return '快照缺失';
    case 'legacy':
      return '旧版快照失效';
    default:
      return status || '未知状态';
  }
};

const snapshotStateVariant = (status?: string) => {
  switch (status) {
    case 'ready':
      return 'success';
    case 'stale':
    case 'legacy':
      return 'warning';
    case 'failed':
      return 'danger';
    case 'missing':
      return 'default';
    default:
      return 'default';
  }
};

const TrendSystemPage: React.FC = () => {
  const [overview, setOverview] = useState<TrendSystemOverviewResponse | null>(null);
  const [status, setStatus] = useState<TrendStatusResponse | null>(null);
  const [conceptSectors, setConceptSectors] = useState<SectorDecisionItem[]>([]);
  const [industrySectors, setIndustrySectors] = useState<SectorDecisionItem[]>([]);
  const [candidates, setCandidates] = useState<CandidateItem[]>([]);
  const [portfolio, setPortfolio] = useState<PortfolioResponse | null>(null);
  const [plan, setPlan] = useState<TrendPlanResponse | null>(null);
  const [diagnostics, setDiagnostics] = useState<TrendDiagnosticsResponse | null>(null);
  const [positions, setPositions] = useState<PositionRecord[]>([]);
  const [alerts, setAlerts] = useState<TrendAlert[]>([]);
  const [activeView, setActiveView] = useState<'concept' | 'industry'>('concept');
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<ParsedApiError | null>(null);
  const [actionError, setActionError] = useState<ParsedApiError | null>(null);
  const [recomputing, setRecomputing] = useState(false);
  const [positionSaving, setPositionSaving] = useState(false);
  const [overrideState, setOverrideState] = useState<Record<string, { targetStage: Stage; reason: string; saving: boolean }>>({});
  const [positionForm, setPositionForm] = useState({
    code: '',
    name: '',
    openDate: new Date().toISOString().slice(0, 10),
    openType: 'breakout',
    entryPrice: '',
    initialStopLoss: '',
    positionPct: '10',
    notes: '',
  });

  const loadAll = useCallback(async (silent = false) => {
    if (silent) {
      setRefreshing(true);
    } else {
      setLoading(true);
    }
    try {
      const [
        overviewData,
        statusData,
        conceptData,
        industryData,
        candidateData,
        portfolioData,
        planData,
        diagnosticsData,
        positionsData,
        alertsData,
      ] = await Promise.all([
        trendSystemApi.getOverview(),
        trendSystemApi.getStatus(),
        trendSystemApi.getSectors('concept'),
        trendSystemApi.getSectors('industry'),
        trendSystemApi.getCandidates(),
        trendSystemApi.getPortfolio(),
        trendSystemApi.getPlan(),
        trendSystemApi.getDiagnostics(),
        trendSystemApi.listPositions(),
        trendSystemApi.listAlerts(),
      ]);

      setOverview(overviewData);
      setStatus(statusData);
      setConceptSectors(conceptData);
      setIndustrySectors(industryData);
      setCandidates(candidateData);
      setPortfolio(portfolioData);
      setPlan(planData);
      setDiagnostics(diagnosticsData);
      setPositions(positionsData);
      setAlerts(alertsData);
      setError(null);
    } catch (err) {
      setError(getParsedApiError(err));
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    void loadAll();
  }, [loadAll]);

  const handleRecompute = async () => {
    setRecomputing(true);
    setActionError(null);
    try {
      await trendSystemApi.recompute('manual_recompute');
      await loadAll(true);
    } catch (err) {
      setActionError(getParsedApiError(err));
    } finally {
      setRecomputing(false);
    }
  };

  const handlePositionSubmit = async () => {
    setPositionSaving(true);
    setActionError(null);
    try {
      await trendSystemApi.createPosition({
        code: positionForm.code.trim().toUpperCase(),
        name: positionForm.name.trim() || undefined,
        openDate: positionForm.openDate,
        openType: positionForm.openType,
        entryPrice: Number(positionForm.entryPrice),
        initialStopLoss: positionForm.initialStopLoss ? Number(positionForm.initialStopLoss) : undefined,
        positionPct: Number(positionForm.positionPct),
        notes: positionForm.notes.trim() || undefined,
      });
      setPositionForm(prev => ({
        ...prev,
        code: '',
        name: '',
        entryPrice: '',
        initialStopLoss: '',
        notes: '',
      }));
      await loadAll(true);
    } catch (err) {
      setActionError(getParsedApiError(err));
    } finally {
      setPositionSaving(false);
    }
  };

  const handleClosePosition = async (item: PortfolioPositionItem) => {
    setActionError(null);
    try {
      await trendSystemApi.updatePosition(item.id, {
        status: 'closed',
        closeDate: new Date().toISOString().slice(0, 10),
        exitReason: item.actionReason,
      });
      await loadAll(true);
    } catch (err) {
      setActionError(getParsedApiError(err));
    }
  };

  const handleAckAlert = async (alertId: number) => {
    setActionError(null);
    try {
      await trendSystemApi.ackAlert(alertId);
      await loadAll(true);
    } catch (err) {
      setActionError(getParsedApiError(err));
    }
  };

  const handleOverride = async (sector: SectorDecisionItem) => {
    const current = overrideState[sector.sectorKey];
    if (!current?.reason?.trim()) return;
    setOverrideState(prev => ({
      ...prev,
      [sector.sectorKey]: { ...current, saving: true },
    }));
    try {
      await trendSystemApi.createStageOverride({
        sectorView: sector.sectorView,
        sectorKey: sector.sectorKey,
        sectorName: sector.sectorName,
        originalStage: sector.quantStage,
        targetStage: current.targetStage,
        reason: current.reason,
      });
      await loadAll(true);
    } catch (err) {
      setActionError(getParsedApiError(err));
    } finally {
      setOverrideState(prev => ({
        ...prev,
        [sector.sectorKey]: { ...(prev[sector.sectorKey] || current), saving: false },
      }));
    }
  };

  const visibleSectors = activeView === 'concept' ? conceptSectors : industrySectors;
  const actionableCandidates = useMemo(() => candidates.filter(item => item.actionable).slice(0, 8), [candidates]);
  const liveAlerts = useMemo(() => alerts.filter(item => !item.acked).slice(0, 6), [alerts]);

  if (loading) {
    return (
      <div className="min-h-screen px-4 pb-6 pt-4 md:px-6 md:ml-20">
        <Card title="趋势系统" subtitle="Full-market Trend System" variant="gradient">
          <div className="text-sm text-content-secondary">加载中...</div>
        </Card>
      </div>
    );
  }

  return (
    <div className="min-h-screen px-4 pb-6 pt-4 md:px-6 md:ml-20 space-y-6">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-content-primary">趋势系统</h1>
          <p className="mt-2 text-sm text-content-secondary">
            全市场主线扫描、盘前定仓、持仓纪律与明日计划
          </p>
        </div>
        <div className="flex gap-3">
          <Button variant="ghost" onClick={() => void loadAll(true)} isLoading={refreshing}>
            刷新数据
          </Button>
          <Button variant="aurora" onClick={() => void handleRecompute()} isLoading={recomputing}>
            <RefreshCcw className="h-4 w-4" />
            手动重算
          </Button>
        </div>
      </div>

      {error && <ApiErrorAlert error={error} />}
      {actionError && <ApiErrorAlert error={actionError} />}
      {status && (
        <Card subtitle="Snapshot State" title="快照状态" variant="data">
          <div className="flex flex-wrap items-center gap-3 text-sm text-content-secondary">
            <Badge variant={snapshotStateVariant(overview?.snapshotStatus)}>
              {snapshotStateText(overview?.snapshotStatus)}
            </Badge>
            {status.recomputeState?.running ? (
              <Badge variant="warning">正在重建快照</Badge>
            ) : (
              <Badge variant="default">当前为只读快照模式</Badge>
            )}
            {status.recomputeState?.lastError && (
              <Badge variant="danger">最近重建失败</Badge>
            )}
            <span>日终：{status.dailySnapshot.snapshotDate || '暂无'} / {status.dailySnapshot.status}</span>
            <span>盘前：{status.preopenSnapshot.snapshotDate || '暂无'} / {status.preopenSnapshot.status}</span>
            {status.recomputeState?.finishedAt && <span>最近重建：{status.recomputeState.finishedAt}</span>}
          </div>
          {status.recomputeState?.lastError && (
            <div className="mt-3 rounded-xl border border-red-500/40 bg-red-500/5 p-3 text-xs text-red-200">
              最近重建错误：{status.recomputeState.lastError}
            </div>
          )}
        </Card>
      )}

      <Card subtitle="Overview" title="今日总览" variant="gradient">
        <div className="grid gap-4 md:grid-cols-5">
          <MetricCard label="建议仓位" value={`${overview?.position.recommendedPositionPct ?? 0}%`} />
          <MetricCard
            label="交易许可"
            value={overview?.tradeAllowed ? '允许' : '空仓'}
            badge={<Badge variant={overview?.tradeAllowed ? 'success' : 'warning'}>{overview?.tradeGate}</Badge>}
          />
          <MetricCard
            label="主阶段"
            value={STAGE_LABEL[overview?.primaryStage ?? 'choppy']}
            badge={<Badge variant={stageVariant(overview?.primaryStage ?? 'choppy')}>{overview?.primaryStage}</Badge>}
          />
          <MetricCard
            label="风险模式"
            value={overview?.riskState.currentMode ?? 'normal'}
            badge={<Badge variant={riskVariant(overview?.riskState.currentMode ?? 'normal')}>{overview?.riskState.currentMode}</Badge>}
          />
          <MetricCard
            label="主线板块"
            value={overview?.mainSectors.map(item => item.sectorName).join('、') || '暂无'}
          />
        </div>
        {overview?.emptyReason && (
          <div className="mt-4 rounded-xl border border-white/10 bg-surface-4 p-3 text-sm text-content-secondary">
            空状态说明：{overview.emptyReason}
          </div>
        )}
      </Card>

      <div className="grid gap-6 lg:grid-cols-2">
        <Card subtitle="Position" title="指数总控" variant="data">
          <div className="space-y-3">
            {overview?.position.rules.map(rule => (
              <div key={rule.key} className="flex items-center justify-between rounded-xl border border-white/8 bg-surface-4 px-4 py-3">
                <div>
                  <div className="text-sm font-medium text-content-primary">{rule.label}</div>
                  <div className="text-xs text-content-secondary">{JSON.stringify(rule.value)}</div>
                </div>
                <Badge variant={rule.matched ? 'success' : 'default'}>{boolText(rule.matched)}</Badge>
              </div>
            ))}
          </div>
        </Card>

        <Card subtitle="Diagnostics" title="系统诊断" variant="data">
          <div className="grid gap-3 md:grid-cols-2">
            <InfoRow label="全市场股票数" value={String(diagnostics?.totalMarketSymbols ?? 0)} />
            <InfoRow label="本地缓存样本" value={String(diagnostics?.dbBackedSymbols ?? 0)} />
            <InfoRow label="实际扫描数" value={String(diagnostics?.scannedSymbols ?? 0)} />
            <InfoRow label="ETF 排除数" value={String(diagnostics?.etfExcluded ?? 0)} />
            <InfoRow label="板块解析成功" value={String(diagnostics?.sectorResolved ?? 0)} />
            <InfoRow label="板块解析失败" value={String(diagnostics?.sectorResolutionFailures ?? 0)} />
            <InfoRow label="候选可执行数" value={String((diagnostics?.candidateFilters?.actionableCandidates as number | undefined) ?? 0)} />
            <InfoRow label="覆盖率" value={`${diagnostics?.coverageRatio ?? 0}%`} />
          </div>
          {!!diagnostics?.sourceNotes?.length && (
            <div className="mt-4 space-y-2">
              {diagnostics.sourceNotes.map(note => (
                <div key={note} className="text-xs text-content-secondary">{note}</div>
              ))}
            </div>
          )}
        </Card>
      </div>

      <Card subtitle="Sectors" title="主线板块" variant="data">
        <div className="mb-4 flex gap-3">
          <Button variant={activeView === 'concept' ? 'aurora' : 'ghost'} size="sm" onClick={() => setActiveView('concept')}>
            概念板块
          </Button>
          <Button variant={activeView === 'industry' ? 'aurora' : 'ghost'} size="sm" onClick={() => setActiveView('industry')}>
            行业板块
          </Button>
        </div>
        <div className="space-y-4">
          {visibleSectors.slice(0, 8).map(sector => {
            const override = overrideState[sector.sectorKey] || {
              targetStage: sector.quantStage === 'late' ? 'middle' : sector.quantStage === 'middle' ? 'initial' : 'choppy',
              reason: '',
              saving: false,
            };
            return (
              <div key={sector.sectorKey} className="rounded-2xl border border-white/10 bg-surface-4 p-4">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <div className="flex items-center gap-2">
                      <h3 className="text-base font-semibold text-content-primary">{sector.sectorName}</h3>
                      <Badge variant={sector.tradeAllowed ? 'success' : 'default'}>{sector.tradeAllowed ? '可交易' : '观察'}</Badge>
                      <Badge variant={stageVariant(sector.finalStage)}>{STAGE_LABEL[sector.finalStage]}</Badge>
                    </div>
                    <div className="mt-2 text-xs text-content-secondary">
                      龙头：{sector.leader?.name || '暂无'} / 龙二：{sector.leader2?.name || '暂无'} / 前排：{sector.frontlineMembers.map(item => item.name).join('、') || '暂无'}
                    </div>
                  </div>
                  <div className="grid gap-1 text-xs text-content-secondary">
                    <span>成交额排名：{sector.topAmountRank}</span>
                    <span>强势家数：{sector.sectorBreadth.strongMemberCount}</span>
                    <span>涨停家数：{sector.sectorBreadth.limitUpCount}</span>
                    <span>一致性：{sector.sectorBreadth.consistencyScore}</span>
                  </div>
                </div>
                <div className="mt-4 grid gap-2 md:grid-cols-3">
                  {Object.entries(sector.conditions).map(([key, value]) => (
                    <InfoRow key={key} label={key} value={boolText(value)} compact />
                  ))}
                </div>
                <div className="mt-4 rounded-xl border border-white/8 bg-surface-5 p-3">
                  <div className="mb-2 text-xs font-semibold uppercase tracking-wider text-content-secondary">阶段降级</div>
                  <div className="grid gap-3 md:grid-cols-[140px_1fr_120px]">
                    <select
                      className="rounded-xl border border-white/10 bg-surface-4 px-3 py-2 text-sm text-content-primary"
                      value={override.targetStage}
                      onChange={e => setOverrideState(prev => ({
                        ...prev,
                        [sector.sectorKey]: { ...override, targetStage: e.target.value as Stage },
                      }))}
                    >
                      {sector.quantStage === 'late' && <option value="middle">middle</option>}
                      {sector.quantStage !== 'choppy' && <option value="initial">initial</option>}
                      <option value="choppy">choppy</option>
                    </select>
                    <input
                      className="rounded-xl border border-white/10 bg-surface-4 px-3 py-2 text-sm text-content-primary"
                      placeholder="记录降级原因"
                      value={override.reason}
                      onChange={e => setOverrideState(prev => ({
                        ...prev,
                        [sector.sectorKey]: { ...override, reason: e.target.value },
                      }))}
                    />
                    <Button variant="ghost" onClick={() => void handleOverride(sector)} isLoading={override.saving}>
                      保存降级
                    </Button>
                  </div>
                  {sector.override && (
                    <div className="mt-2 text-xs text-content-secondary">
                      当前已降级为 {sector.override.targetStage}，原因：{sector.override.reason}
                    </div>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </Card>

      <div className="grid gap-6 xl:grid-cols-2">
        <Card subtitle="Candidates" title="候选股与买点" variant="data">
          <div className="space-y-3">
            {actionableCandidates.length === 0 ? (
              <div className="text-sm text-content-secondary">暂无可执行候选。</div>
            ) : actionableCandidates.map(item => (
              <div key={item.code} className="rounded-xl border border-white/10 bg-surface-4 p-4">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <div className="flex items-center gap-2">
                      <span className="font-semibold text-content-primary">{item.name} {item.code}</span>
                      <Badge variant={item.isEliteCandidate ? 'nebula' : 'default'}>{item.isEliteCandidate ? '精英池' : '普通池'}</Badge>
                      <Badge variant={stageVariant(item.finalStage)}>{STAGE_LABEL[item.finalStage]}</Badge>
                    </div>
                    <div className="mt-1 text-xs text-content-secondary">{item.sectorName} / {item.signalLabel}</div>
                  </div>
                  <Badge variant={item.actionable ? 'success' : 'warning'}>{item.actionable ? '可执行' : item.actionBlockReason || '观察'}</Badge>
                </div>
                <div className="mt-3 grid gap-2 md:grid-cols-2">
                  <InfoRow label="建议买点" value={item.suggestedEntry ? `${item.suggestedEntry}` : '--'} compact />
                  <InfoRow label="止损位" value={item.stopLoss ? `${item.stopLoss}` : '--'} compact />
                  <InfoRow label="推荐仓位" value={`${item.recommendedPositionPct}%`} compact />
                  <InfoRow label="失效条件" value={item.invalidIf || '--'} compact />
                </div>
              </div>
            ))}
          </div>
        </Card>

        <Card subtitle="Portfolio" title="持仓管理" variant="data">
          <div className="mb-4 grid gap-3 md:grid-cols-4">
            <InfoRow label="持仓数" value={String(portfolio?.summary.openCount ?? 0)} compact />
            <InfoRow label="总持仓" value={`${portfolio?.summary.totalPositionPct ?? 0}%`} compact />
            <InfoRow label="减仓提示" value={String(portfolio?.summary.reduceCount ?? 0)} compact />
            <InfoRow label="清仓提示" value={String(portfolio?.summary.exitCount ?? 0)} compact />
          </div>
          <div className="space-y-3">
            {(portfolio?.items || []).length === 0 ? (
              <div className="text-sm text-content-secondary">暂无持仓记录。</div>
            ) : portfolio?.items.map(item => (
              <div key={item.id} className="rounded-xl border border-white/10 bg-surface-4 p-4">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <div className="flex items-center gap-2">
                      <span className="font-semibold text-content-primary">{item.name} {item.code}</span>
                      <Badge variant={actionVariant(item.action)}>{item.action}</Badge>
                    </div>
                    <div className="mt-1 text-xs text-content-secondary">{item.actionReason}</div>
                  </div>
                  <div className="grid gap-1 text-xs text-content-secondary">
                    <span>收益：{item.profitPct ?? 0}%</span>
                    <span>仓位：{item.positionPct}%</span>
                    <span>建议卖出：{item.suggestedSellPct}%</span>
                  </div>
                </div>
                <div className="mt-3 flex flex-wrap gap-2">
                  {Object.entries(item.signals).map(([key, value]) => (
                    <Badge key={key} variant={value ? 'warning' : 'default'}>{key}</Badge>
                  ))}
                </div>
                {item.action !== 'hold' && (
                  <div className="mt-3">
                    <Button variant={item.action === 'exit' ? 'danger' : 'ghost'} size="sm" onClick={() => void handleClosePosition(item)}>
                      按建议更新持仓
                    </Button>
                  </div>
                )}
              </div>
            ))}
          </div>
        </Card>
      </div>

      <div className="grid gap-6 xl:grid-cols-2">
        <Card subtitle="Risk State" title="风控与纪律" variant="data">
          <div className="grid gap-3 md:grid-cols-2">
            <InfoRow label="当前模式" value={overview?.riskState.currentMode || 'normal'} />
            <InfoRow label="连续止损" value={String(overview?.riskState.consecutiveStopLosses ?? 0)} />
            <InfoRow label="突破失败" value={String(overview?.riskState.recentBreakoutFailures ?? 0)} />
            <InfoRow label="新开仓上限" value={`${overview?.riskState.newPositionLimitPct ?? '--'}%`} />
          </div>
          <div className="mt-4 flex flex-wrap gap-2">
            {Object.entries(overview?.riskState.flags || {}).map(([key, value]) => (
              <Badge key={key} variant={value ? 'warning' : 'default'}>{key}</Badge>
            ))}
          </div>
          {!!liveAlerts.length && (
            <div className="mt-4 space-y-3">
              {liveAlerts.map(alert => (
                <div key={alert.id} className="rounded-xl border border-semantic-warning/20 bg-semantic-warning/5 p-3">
                  <div className="flex items-start justify-between gap-3">
                    <div className="flex items-start gap-2">
                      <ShieldAlert className="mt-0.5 h-4 w-4 text-semantic-warning" />
                      <div>
                        <div className="text-sm text-content-primary">{alert.message}</div>
                        <div className="text-xs text-content-secondary">{alert.alertType}</div>
                      </div>
                    </div>
                    <Button variant="ghost" size="sm" onClick={() => void handleAckAlert(alert.id)}>已处理</Button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </Card>

        <Card subtitle="Plan" title="明日计划" variant="data">
          <div className="space-y-3">
            <InfoRow label="建议总仓位" value={`${plan?.recommendedPositionPct ?? 0}%`} />
            <InfoRow label="交易许可" value={plan?.tradeAllowed ? '允许交易' : '空仓'} />
            <InfoRow label="主做板块" value={plan?.mainSectors.map(item => item.sectorName).join('、') || '暂无'} />
            <InfoRow label="主阶段" value={plan ? STAGE_LABEL[plan.primaryStage] : '--'} />
          </div>
          {!!plan?.blockedRules.length && (
            <div className="mt-4 space-y-2">
              {plan.blockedRules.map(rule => (
                <div key={rule} className="text-sm text-semantic-warning">{rule}</div>
              ))}
            </div>
          )}
          <div className="mt-4 rounded-xl border border-white/8 bg-surface-4 p-4 text-sm text-content-secondary">
            {plan?.disciplineNotes.join('\n')}
          </div>
        </Card>
      </div>

      <div className="grid gap-6 xl:grid-cols-2">
        <Card subtitle="Execution Journal" title="新增持仓记录" variant="data">
          <div className="grid gap-3">
            <input
              className="rounded-xl border border-white/10 bg-surface-4 px-3 py-2 text-sm text-content-primary"
              placeholder="股票代码"
              value={positionForm.code}
              onChange={e => setPositionForm(prev => ({ ...prev, code: e.target.value }))}
            />
            <input
              className="rounded-xl border border-white/10 bg-surface-4 px-3 py-2 text-sm text-content-primary"
              placeholder="股票名称（可选）"
              value={positionForm.name}
              onChange={e => setPositionForm(prev => ({ ...prev, name: e.target.value }))}
            />
            <div className="grid gap-3 md:grid-cols-2">
              <input
                type="date"
                className="rounded-xl border border-white/10 bg-surface-4 px-3 py-2 text-sm text-content-primary"
                value={positionForm.openDate}
                onChange={e => setPositionForm(prev => ({ ...prev, openDate: e.target.value }))}
              />
              <select
                className="rounded-xl border border-white/10 bg-surface-4 px-3 py-2 text-sm text-content-primary"
                value={positionForm.openType}
                onChange={e => setPositionForm(prev => ({ ...prev, openType: e.target.value }))}
              >
                <option value="breakout">突破交易</option>
                <option value="pullback">回调交易</option>
                <option value="late_reclaim">后期转强</option>
                <option value="compensation">踏空补偿</option>
              </select>
            </div>
            <div className="grid gap-3 md:grid-cols-2">
              <input
                className="rounded-xl border border-white/10 bg-surface-4 px-3 py-2 text-sm text-content-primary"
                placeholder="开仓价格"
                value={positionForm.entryPrice}
                onChange={e => setPositionForm(prev => ({ ...prev, entryPrice: e.target.value }))}
              />
              <input
                className="rounded-xl border border-white/10 bg-surface-4 px-3 py-2 text-sm text-content-primary"
                placeholder="初始止损位"
                value={positionForm.initialStopLoss}
                onChange={e => setPositionForm(prev => ({ ...prev, initialStopLoss: e.target.value }))}
              />
            </div>
            <input
              className="rounded-xl border border-white/10 bg-surface-4 px-3 py-2 text-sm text-content-primary"
              placeholder="仓位百分比"
              value={positionForm.positionPct}
              onChange={e => setPositionForm(prev => ({ ...prev, positionPct: e.target.value }))}
            />
            <textarea
              className="min-h-[88px] rounded-xl border border-white/10 bg-surface-4 px-3 py-2 text-sm text-content-primary"
              placeholder="备注"
              value={positionForm.notes}
              onChange={e => setPositionForm(prev => ({ ...prev, notes: e.target.value }))}
            />
            <Button variant="aurora" fullWidth isLoading={positionSaving} onClick={() => void handlePositionSubmit()}>
              保存持仓记录
            </Button>
          </div>
        </Card>

        <Card subtitle="Positions" title="当前持仓台账" variant="data">
          <div className="space-y-3">
            {positions.length === 0 ? (
              <div className="text-sm text-content-secondary">暂无持仓/历史仓位记录。</div>
            ) : positions.slice(0, 10).map(item => (
              <div key={item.id} className="rounded-xl border border-white/10 bg-surface-4 p-3">
                <div className="flex items-center justify-between gap-2">
                  <div>
                    <div className="text-sm font-semibold text-content-primary">{item.name} {item.code}</div>
                    <div className="text-xs text-content-secondary">{item.openDate} / {item.openType}</div>
                  </div>
                  <Badge variant={item.status === 'open' ? 'success' : 'default'}>{item.status}</Badge>
                </div>
              </div>
            ))}
          </div>
        </Card>
      </div>
    </div>
  );
};

const MetricCard: React.FC<{ label: string; value: string; badge?: React.ReactNode }> = ({ label, value, badge }) => (
  <div className="rounded-2xl border border-white/8 bg-surface-4 p-4">
    <div className="text-sm text-content-secondary">{label}</div>
    <div className="mt-3 flex items-center justify-between gap-3">
      <div className="text-2xl font-bold text-content-primary">{value}</div>
      {badge}
    </div>
  </div>
);

const InfoRow: React.FC<{ label: string; value: string; compact?: boolean }> = ({ label, value, compact = false }) => (
  <div className={`rounded-xl border border-white/8 bg-surface-4 ${compact ? 'px-3 py-2' : 'px-4 py-3'}`}>
    <div className="text-xs uppercase tracking-wider text-content-secondary">{label}</div>
    <div className="mt-1 text-sm font-medium text-content-primary whitespace-pre-wrap">{value}</div>
  </div>
);

export default TrendSystemPage;
