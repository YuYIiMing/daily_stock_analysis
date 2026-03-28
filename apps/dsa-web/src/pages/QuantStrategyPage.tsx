import type React from 'react';
import { useDeferredValue, useEffect, useState, startTransition } from 'react';
import { Filter, RefreshCcw } from 'lucide-react';
import { quantStrategyApi } from '../api/quantStrategy';
import type { ParsedApiError } from '../api/error';
import { getParsedApiError } from '../api/error';
import type {
  QuantBacktestDetailResponse,
  QuantBacktestRunResponse,
  QuantEquityPoint,
  QuantSyncResponse,
  QuantSyncStatusResponse,
  QuantSyncStatusSummary,
  QuantTradeItem,
  QuantTradePlanItem,
  QuantTradePlanResponse,
} from '../types/quantStrategy';
import { ApiErrorAlert, Badge, Button, Card, Drawer } from '../components/common';
import {
  ChartPanel,
  FilterRail,
  MetricTile,
  PageHero,
  QuantSyncStatusCard,
  TradePlanWorkspace,
  getExitReasonLabel,
  getModuleLabel,
  getRegimeLabel,
  getStageLabel,
  getTabLabel,
} from '../components/quant';

type TabId = 'overview' | 'plan' | 'trades';
type SyncMode = 'latest' | 'full';
type SyncActionStatus = 'ok' | 'partial' | 'error' | null;

function toIsoDate(input: Date): string {
  return input.toISOString().slice(0, 10);
}

function parseNumber(value: number | undefined | null): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

function toPercent(value: number | undefined | null): string {
  if (value == null || Number.isNaN(value)) return '--';
  return `${value.toFixed(2)}%`;
}

function toSummaryErrorList(payload: Record<string, unknown> | undefined): string[] {
  const raw = payload?.errors;
  if (!Array.isArray(raw)) return [];
  return raw.filter((item): item is string => typeof item === 'string' && item.trim().length > 0);
}

function buildSyncActionMessage(response: QuantSyncResponse, mode: SyncMode): string {
  const errors = toSummaryErrorList(response.summary);
  const defaultOkMessage = mode === 'latest'
    ? '最新日同步完成，已刷新交易计划和同步状态。'
    : '全窗口重同步完成，已刷新同步状态；如需更新回测，请手动运行结构化回测。';
  if (response.status === 'partial') {
    const errorPreview = errors.slice(0, 2).join('；');
    return errorPreview
      ? `同步部分完成：${errorPreview}${errors.length > 2 ? ' 等更多失败项，请检查 summary.errors。' : ''}`
      : '同步部分完成，存在失败项，请检查 summary.errors。';
  }
  return response.message || defaultOkMessage;
}

function buildMockTradePlan(asOfDate: string): QuantTradePlanResponse {
  return {
    asOfDate,
    regime: 'Neutral',
    marketScore: 1.62,
    maxExposurePct: 30,
    diagnostics: {
      eligibleStockCount: 36,
      sameDayBoardMatchCount: 34,
      recentBoardFallbackCount: 0,
      missingBoardFeatureCount: 2,
      tradeAllowedStockCount: 8,
      candidateStockCount: 3,
      mappedStageDistribution: {
        TREND: 18,
        EMERGING: 10,
        IGNORE: 8,
      },
      tradeAllowedBoards: [
        { boardName: '消费复苏', stockCount: 3, stage: 'TREND', themeScore: 3, featureTradeDate: asOfDate },
        { boardName: '智能驾驶', stockCount: 3, stage: 'EMERGING', themeScore: 3, featureTradeDate: asOfDate },
      ],
      topMissingBoards: [{ boardName: '半导体材料', stockCount: 2 }],
      primaryBlocker: 'candidates_ready',
      summary: '当前已有候选股通过市场、板块和个股条件，可继续按计划执行。',
    },
    items: [
      {
        code: '600519',
        boardCode: 'BK0987',
        boardName: '消费复苏',
        stage: 'TREND',
        entryModule: 'PULLBACK',
        signalScore: 86.2,
        plannedEntryPrice: 1688.5,
        initialStopPrice: 1626.2,
        plannedPositionPct: 11.5,
        blockedReason: null,
      },
      {
        code: '000625',
        boardCode: 'BK1015',
        boardName: '智能驾驶',
        stage: 'EMERGING',
        entryModule: 'BREAKOUT',
        signalScore: 82.4,
        plannedEntryPrice: 18.24,
        initialStopPrice: 17.52,
        plannedPositionPct: 9.2,
        blockedReason: null,
      },
      {
        code: '600460',
        boardCode: 'BK1030',
        boardName: '半导体材料',
        stage: 'CLIMAX',
        entryModule: 'CLIMAX_WEAK_TO_STRONG',
        signalScore: 77.1,
        plannedEntryPrice: 57.8,
        initialStopPrice: 55.6,
        plannedPositionPct: 0,
        blockedReason: 'capacity_exhausted',
      },
    ],
  };
}

function buildMockBacktestDetail(startDate: string, endDate: string): QuantBacktestDetailResponse {
  return {
    runId: 901,
    strategyName: 'concept_trend_v1',
    marketScope: 'cn_main_board',
    boardSource: 'ths_concept',
    startDate,
    endDate,
    initialCapital: 1000000,
    status: 'completed',
    summary: {
      totalReturnPct: 18.4,
      finalEquity: 1184000,
      maxDrawdownPct: 6.3,
      tradeCount: 27,
      winCount: 16,
      lossCount: 11,
      winRatePct: 59.3,
    },
  };
}

function buildMockEquity(startDate: string): QuantEquityPoint[] {
  const seed = parseFloat(startDate.replace(/-/g, '').slice(-4)) || 1000;
  const points: QuantEquityPoint[] = [];
  let equity = 1000000;
  let peak = equity;
  for (let i = 0; i < 45; i += 1) {
    const day = new Date(Date.now() - (44 - i) * 24 * 3600 * 1000);
    const drift = Math.sin((seed + i) / 3.1) * 1300 + (i % 7 - 3) * 380;
    equity += drift;
    peak = Math.max(peak, equity);
    const drawdownPct = ((equity - peak) / peak) * 100;
    const exposurePct = 20 + Math.max(0, Math.sin(i / 4) * 35);
    points.push({
      tradeDate: toIsoDate(day),
      cash: Math.max(200000, 1000000 - i * 7000),
      marketValue: Math.max(0, equity - 350000),
      equity,
      drawdownPct,
      exposurePct,
    });
  }
  return points;
}

function buildMockTrades(): QuantTradeItem[] {
  return [
    {
      code: '600519',
      stockName: '贵州茅台',
      boardCode: 'BK0987',
      boardName: '消费复苏',
      entryDate: '2026-02-21',
      exitDate: '2026-03-05',
      entryPrice: 1622.4,
      exitPrice: 1728.8,
      entryAmount: 648960,
      exitAmount: 691520,
      shares: 400,
      entryModule: 'PULLBACK',
      stage: 'TREND',
      status: 'closed',
      pnlPct: 6.56,
      pnlAmount: 42560,
      exitReason: 'take_profit_10pct',
      blockedExit: false,
    },
    {
      code: '000625',
      stockName: '长安汽车',
      boardCode: 'BK1015',
      boardName: '智能驾驶',
      entryDate: '2026-03-01',
      exitDate: '2026-03-11',
      entryPrice: 17.81,
      exitPrice: 18.62,
      entryAmount: 441688,
      exitAmount: 461776,
      shares: 24800,
      entryModule: 'BREAKOUT',
      stage: 'EMERGING',
      status: 'closed',
      pnlPct: 4.55,
      pnlAmount: 20088,
      exitReason: 'trend_exit',
      blockedExit: false,
    },
    {
      code: '600460',
      stockName: '士兰微',
      boardCode: 'BK1030',
      boardName: '半导体材料',
      entryDate: '2026-03-08',
      exitDate: '2026-03-13',
      entryPrice: 59.1,
      exitPrice: 55.4,
      entryAmount: 555540,
      exitAmount: 520760,
      shares: 9400,
      entryModule: 'CLIMAX_WEAK_TO_STRONG',
      stage: 'CLIMAX',
      status: 'closed',
      pnlPct: -6.26,
      pnlAmount: -34780,
      exitReason: 'hard_stop',
      blockedExit: false,
    },
  ];
}

function toRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  return value as Record<string, unknown>;
}

function toMetricNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function toMetricString(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function pickMetricNumber(
  sources: Array<Record<string, unknown> | null>,
  keys: string[],
): number | null {
  for (const source of sources) {
    if (!source) continue;
    for (const key of keys) {
      const value = toMetricNumber(source[key]);
      if (value != null) return value;
    }
  }
  return null;
}

function pickMetricString(
  sources: Array<Record<string, unknown> | null>,
  keys: string[],
): string | null {
  for (const source of sources) {
    if (!source) continue;
    for (const key of keys) {
      const value = toMetricString(source[key]);
      if (value) return value;
    }
  }
  return null;
}

function normalizeSyncStatus(payload: QuantSyncStatusResponse | null): QuantSyncStatusSummary | null {
  if (!payload) return null;

  const root = toRecord(payload);
  const summary = toRecord(payload.summary);
  const boardSnapshot = toRecord(root?.latestBoardSnapshot ?? root?.boardSnapshot ?? root?.conceptBoardSnapshot);
  const stockSnapshot = toRecord(root?.latestStockFeatureSnapshot ?? root?.stockFeatureSnapshot);
  const indexSnapshot = toRecord(root?.latestIndexFeatureSnapshot ?? root?.indexFeatureSnapshot);
  const conceptBoard = toRecord(root?.conceptBoard);
  const stockFeature = toRecord(root?.stockFeature);
  const indexFeature = toRecord(root?.indexFeature);
  const sources = [root, summary];

  return {
    membershipDistinctCodes: pickMetricNumber(
      sources,
      ['membershipDistinctCodes', 'membershipCoverageCount', 'membershipCount', 'coveredStockCount'],
    ),
    latestMembershipDate: pickMetricString(
      sources,
      ['latestMembershipDate', 'membershipLatestDate', 'latestMembershipTradeDate'],
    ),
    latestMembershipCount: pickMetricNumber(
      sources,
      ['latestMembershipCount', 'dailyMembershipCount', 'membershipDailyCount'],
    ),
    conceptBoardCoverageCount: pickMetricNumber(
      [...sources, conceptBoard],
      ['conceptBoardCoverageCount', 'boardCoverageCount', 'conceptBoardCoverage', 'coveredBoardCount', 'boardCount'],
    ),
    latestBoardDate: pickMetricString(
      [...sources, boardSnapshot, conceptBoard],
      ['latestBoardDate', 'boardLatestDate', 'latestBoardTradeDate', 'tradeDate', 'date'],
    ),
    latestBoardCount: pickMetricNumber(
      [...sources, boardSnapshot, conceptBoard],
      ['latestBoardCount', 'latestBoardFeatureCount', 'dailyBoardCount', 'boardCount', 'count'],
    ),
    stockPoolSize: pickMetricNumber(
      sources,
      ['stockPoolSize', 'stockUniverseSize', 'stockPoolCount', 'mainBoardStockPoolSize'],
    ),
    stockDailyDistinctCodes: pickMetricNumber(
      sources,
      ['stockDailyDistinctCodes', 'stockDailyCount', 'dailyStockCoverageCount', 'loadedStockDailyCount'],
    ),
    latestStockFeatureDate: pickMetricString(
      [...sources, stockSnapshot, stockFeature],
      ['latestStockFeatureDate', 'stockFeatureLatestDate', 'latestFeatureDate', 'tradeDate', 'date'],
    ),
    latestStockFeatureCount: pickMetricNumber(
      [...sources, stockSnapshot, stockFeature],
      ['latestStockFeatureCount', 'dailyStockFeatureCount', 'stockFeatureCount', 'stockCount', 'count'],
    ),
    latestIndexFeatureDate: pickMetricString(
      [...sources, indexSnapshot, indexFeature],
      ['latestIndexFeatureDate', 'indexFeatureLatestDate', 'latestIndexDate', 'tradeDate', 'date'],
    ),
  };
}

const QuantStrategyPage: React.FC = () => {
  const today = toIsoDate(new Date());
  const [filters, setFilters] = useState({
    asOfDate: today,
    module: 'ALL',
    stage: 'ALL',
    code: '',
  });
  const [activeTab, setActiveTab] = useState<TabId>('overview');
  const [isFilterOpen, setIsFilterOpen] = useState(false);
  const [isRunning, setIsRunning] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [runId, setRunId] = useState<number | null>(null);
  const [detail, setDetail] = useState<QuantBacktestDetailResponse | null>(null);
  const [plan, setPlan] = useState<QuantTradePlanResponse | null>(null);
  const [trades, setTrades] = useState<QuantTradeItem[]>([]);
  const [equity, setEquity] = useState<QuantEquityPoint[]>([]);
  const [selectedTrade, setSelectedTrade] = useState<QuantTradeItem | null>(null);
  const [pageError, setPageError] = useState<ParsedApiError | null>(null);
  const [planHint, setPlanHint] = useState<string | null>(null);
  const [usingDemo, setUsingDemo] = useState(false);
  const [syncStatus, setSyncStatus] = useState<QuantSyncStatusSummary | null>(null);
  const [syncStatusMessage, setSyncStatusMessage] = useState<string | null>(null);
  const [syncStatusError, setSyncStatusError] = useState<string | null>(null);
  const [isSyncStatusLoading, setIsSyncStatusLoading] = useState(false);
  const [isSyncing, setIsSyncing] = useState(false);
  const [syncingMode, setSyncingMode] = useState<SyncMode | null>(null);
  const [syncActionMessage, setSyncActionMessage] = useState<string | null>(null);
  const [syncActionStatus, setSyncActionStatus] = useState<SyncActionStatus>(null);
  const deferredCode = useDeferredValue(filters.code.trim());

  const applyDemoData = (asOfDate: string) => {
    const end = asOfDate;
    const start = toIsoDate(new Date(Date.now() - 60 * 24 * 3600 * 1000));
    startTransition(() => {
      setPlan(buildMockTradePlan(asOfDate));
      setDetail(buildMockBacktestDetail(start, end));
      setEquity(buildMockEquity(start));
      setTrades(buildMockTrades());
      setRunId(901);
      setPlanHint(null);
      setUsingDemo(true);
    });
  };

  const shouldUseDemoFallback = (error: ParsedApiError | null | undefined): boolean => {
    if (!error) return false;
    if (error.category === 'local_connection_failed') return true;
    return typeof error.status === 'number' && error.status >= 500;
  };

  const fetchSyncStatus = async () => {
    setIsSyncStatusLoading(true);
    try {
      const response = await quantStrategyApi.getSyncStatus();
      const summaryMessage = toMetricString(toRecord(response.summary)?.message);
      startTransition(() => {
        setSyncStatus(normalizeSyncStatus(response));
        setSyncStatusMessage(response.message ?? summaryMessage ?? null);
        setSyncStatusError(null);
      });
    } catch (error) {
      const parsed = getParsedApiError(error);
      startTransition(() => {
        setSyncStatusError(parsed.message);
        setSyncStatusMessage(null);
      });
    } finally {
      setIsSyncStatusLoading(false);
    }
  };

  const runSync = async (mode: SyncMode) => {
    setIsSyncing(true);
    setSyncingMode(mode);
    setSyncActionMessage(null);
    setSyncActionStatus(null);
    try {
      const response = await quantStrategyApi.runSync({
        asOfDate: filters.asOfDate,
        historyDays: 130,
        includeRankedBoards: true,
        latestFeatureOnly: mode === 'latest',
      });
      const summary = response.summary as Record<string, unknown> | undefined;
      const resolvedAsOfDate = typeof summary?.asOfDate === 'string'
        ? summary.asOfDate
        : typeof summary?.as_of_date === 'string'
          ? summary.as_of_date
          : filters.asOfDate;
      const refreshResults = await Promise.allSettled([
        fetchSyncStatus(),
        fetchTradePlan(resolvedAsOfDate, resolvedAsOfDate !== filters.asOfDate),
      ]);
      const refreshFailed = refreshResults.some((result) => result.status === 'rejected');
      const nextActionStatus: SyncActionStatus = refreshFailed
        ? 'partial'
        : response.status === 'partial'
          ? 'partial'
          : 'ok';
      const nextActionMessage = refreshFailed
        ? `${buildSyncActionMessage(response, mode)} 页面局部刷新失败，请手动点“刷新状态”确认最新覆盖结果。`
        : buildSyncActionMessage(response, mode);
      startTransition(() => {
        setSyncActionMessage(nextActionMessage);
        setSyncActionStatus(nextActionStatus);
      });
    } catch (error) {
      const parsed = getParsedApiError(error);
      startTransition(() => {
        setSyncActionStatus('error');
        setSyncActionMessage(`同步失败：${parsed.message}`);
      });
    } finally {
      setIsSyncing(false);
      setSyncingMode(null);
    }
  };

  const fetchTradePlan = async (asOfDate?: string, syncFilterDate = false) => {
    try {
      const response = await quantStrategyApi.getTradePlan(asOfDate);
      startTransition(() => {
        setPlan(response);
        setPlanHint(response.message ?? null);
        setPageError(null);
        setUsingDemo(false);
        if (syncFilterDate && response.asOfDate) {
          setFilters((prev) => ({ ...prev, asOfDate: response.asOfDate }));
        }
      });
    } catch (error) {
      const parsed = getParsedApiError(error);
      if (parsed.status === 404) {
        startTransition(() => {
          setPlan(null);
          setPlanHint('当前还没有量化特征数据，交易计划暂不可用。请先同步指数、概念板块和个股特征快照。');
        });
        return;
      }
      setPageError(parsed);
      if (shouldUseDemoFallback(parsed)) {
        applyDemoData(asOfDate ?? filters.asOfDate);
      }
    }
  };

  const fetchBacktestArtifacts = async (nextRunId: number, detailResponse?: QuantBacktestDetailResponse) => {
    try {
      const [detailPayload, tradeResponse, equityResponse] = await Promise.all([
        detailResponse ? Promise.resolve(detailResponse) : quantStrategyApi.getBacktestDetail(nextRunId),
        quantStrategyApi.getTrades(nextRunId),
        quantStrategyApi.getEquityCurve(nextRunId),
      ]);
      startTransition(() => {
        setDetail(detailPayload);
        setTrades(tradeResponse);
        setEquity(equityResponse);
        setRunId(nextRunId);
        setPageError(null);
      });
    } catch (error) {
      const parsed = getParsedApiError(error);
      if (parsed.status === 404) {
        return;
      }
      setPageError(parsed);
      if (shouldUseDemoFallback(parsed)) {
        applyDemoData(filters.asOfDate);
      }
    }
  };

  useEffect(() => {
    const loadInitialData = async () => {
      setIsLoading(true);
      try {
        await Promise.allSettled([
          fetchTradePlan(undefined, true),
          fetchSyncStatus(),
          (async () => {
            try {
              const latestDetail = await quantStrategyApi.getLatestBacktestDetail();
              await fetchBacktestArtifacts(latestDetail.runId, latestDetail);
            } catch (error) {
              const parsed = getParsedApiError(error);
              if (parsed.status !== 404) {
                setPageError(parsed);
              }
            }
          })(),
        ]);
      } finally {
        setIsLoading(false);
      }
    };
    void loadInitialData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleApplyFilters = () => {
    setIsFilterOpen(false);
    setIsLoading(true);
    void fetchTradePlan(filters.asOfDate).finally(() => setIsLoading(false));
  };

  const handleRunBacktest = async () => {
    setIsRunning(true);
    const endDate = filters.asOfDate;
    const startDate = toIsoDate(new Date(new Date(endDate).getTime() - 120 * 24 * 3600 * 1000));
    try {
      const response: QuantBacktestRunResponse = await quantStrategyApi.runBacktest({
        startDate,
        endDate,
        initialCapital: 1000000,
        strategyName: 'concept_trend_v1',
      });
      await fetchBacktestArtifacts(response.runId);
      await fetchTradePlan(filters.asOfDate);
      await fetchSyncStatus();
      setUsingDemo(false);
    } catch (error) {
      setPageError(getParsedApiError(error));
      applyDemoData(filters.asOfDate);
    } finally {
      setIsRunning(false);
    }
  };

  const filteredPlanItems: QuantTradePlanItem[] = (plan?.items ?? []).filter((item) => {
    if (filters.module !== 'ALL' && item.entryModule !== filters.module) return false;
    if (filters.stage !== 'ALL' && item.stage !== filters.stage) return false;
    if (deferredCode && !item.code.includes(deferredCode)) return false;
    return true;
  });

  const filteredTrades: QuantTradeItem[] = trades.filter((trade) => {
    if (filters.module !== 'ALL' && trade.entryModule !== filters.module) return false;
    if (filters.stage !== 'ALL' && trade.stage !== filters.stage) return false;
    if (deferredCode && !trade.code.includes(deferredCode)) return false;
    return true;
  });

  const summary = detail?.summary;
  const totalReturn = parseNumber(summary?.totalReturnPct);
  const winRate = parseNumber(summary?.winRatePct);
  const drawdown = parseNumber(summary?.maxDrawdownPct);
  const tradeCount = parseNumber(summary?.tradeCount);
  const equityPoints = equity.map((point) => ({ x: point.tradeDate, y: point.equity }));
  const drawdownPoints = equity.map((point) => ({ x: point.tradeDate, y: point.drawdownPct }));
  const regime = plan?.regime ?? '未知';
  const marketScore = plan?.marketScore;
  const maxExposurePct = plan?.maxExposurePct;
  const planHintTitle = plan ? '当前交易提示' : '交易计划暂不可用';

  return (
    <div className="min-h-screen md:ml-20 px-4 pb-8 pt-5 md:px-6">
      <div className="mx-auto w-full max-w-[1460px] space-y-5">
        <PageHero
          regime={regime}
          marketScore={marketScore}
          maxExposurePct={maxExposurePct}
          asOfDate={filters.asOfDate}
          isRunning={isRunning}
          onRunBacktest={handleRunBacktest}
        />

        <QuantSyncStatusCard
          status={syncStatus}
          isLoading={isSyncStatusLoading}
          isSyncing={isSyncing}
          syncingMode={syncingMode}
          errorMessage={syncStatusError}
          apiMessage={syncStatusMessage}
          actionMessage={syncActionMessage}
          actionStatus={syncActionStatus}
          onRetry={() => void fetchSyncStatus()}
          onSyncLatest={() => void runSync('latest')}
          onSyncFull={() => void runSync('full')}
        />

        {pageError ? <ApiErrorAlert error={pageError} /> : null}
        {planHint ? (
          <Card variant="bordered" padding="sm" className="border-semantic-warning/35">
            <div>
              <p className="text-sm text-content-primary font-semibold">{planHintTitle}</p>
              <p className="text-xs text-content-tertiary mt-1">{planHint}</p>
            </div>
          </Card>
        ) : null}
        {usingDemo ? (
          <Card variant="bordered" padding="sm" className="border-brand-secondary/30">
            <div className="flex items-center justify-between gap-3">
              <div>
                <p className="text-sm text-content-primary font-semibold">演示数据模式</p>
                <p className="text-xs text-content-tertiary mt-1">
                  后端量化接口尚未就绪，当前页面展示演示数据用于验证 UI 流程。
                </p>
              </div>
              <Button variant="ghost" size="sm" onClick={() => void fetchTradePlan(filters.asOfDate)}>
                <RefreshCcw className="w-3.5 h-3.5" />
                重试接口
              </Button>
            </div>
          </Card>
        ) : null}

        <div className="flex items-center justify-between gap-3 xl:hidden">
          <div className="flex items-center gap-2">
            <Button variant="ghost" size="sm" onClick={() => setIsFilterOpen(true)}>
              <Filter className="w-4 h-4" />
              过滤器
            </Button>
            <Badge variant="default">{filteredPlanItems.length} 个候选</Badge>
          </div>
          <span className="text-xs text-content-tertiary">{isLoading ? '加载中...' : `回测批次 #${runId ?? '--'}`}</span>
        </div>

        <div className="grid gap-5 xl:grid-cols-[300px_minmax(0,1fr)]">
          <aside className="hidden xl:block">
            <FilterRail filters={filters} onChange={setFilters} onApply={handleApplyFilters} />
          </aside>

          <section className="space-y-4">
            <div className="rounded-xl border border-white/8 bg-black/15 p-1.5">
              <div className="grid grid-cols-3 gap-1">
                {(['overview', 'plan', 'trades'] as TabId[]).map((tab) => (
                  <button
                    key={tab}
                    type="button"
                    onClick={() => setActiveTab(tab)}
                    className={`
                      rounded-lg px-3 py-2 text-xs font-medium transition-all duration-200
                      ${activeTab === tab
                        ? 'bg-white/10 text-content-primary shadow-[inset_0_1px_0_rgba(255,255,255,0.15)]'
                        : 'text-content-tertiary hover:text-content-secondary'}
                    `}
                  >
                    {getTabLabel(tab)}
                  </button>
                ))}
              </div>
            </div>

            {(activeTab === 'overview' || activeTab === 'plan') ? (
              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                <MetricTile label="总收益率" value={toPercent(totalReturn)} hint="当前回测区间结果" tone={totalReturn >= 0 ? 'success' : 'danger'} />
                <MetricTile label="胜率" value={toPercent(winRate)} hint={`盈利 ${summary?.winCount ?? 0} 笔 / 亏损 ${summary?.lossCount ?? 0} 笔`} tone={winRate >= 50 ? 'success' : 'warning'} />
                <MetricTile label="最大回撤" value={toPercent(drawdown)} hint="越低越稳健" tone={drawdown >= 10 ? 'danger' : 'warning'} />
                <MetricTile label="交易次数" value={tradeCount ? String(tradeCount) : '--'} hint={`回测批次：${runId ?? '--'}`} />
              </div>
            ) : null}

            {(activeTab === 'overview' || activeTab === 'trades') ? (
              <div className="grid gap-4 lg:grid-cols-2">
                <ChartPanel
                  title="组合净值曲线"
                  subtitle="展示当前回测区间内的资金变化"
                  points={equityPoints}
                  tone="equity"
                />
                <ChartPanel
                  title="回撤曲线"
                  subtitle="展示从阶段高点回落的资金压力"
                  points={drawdownPoints}
                  tone="drawdown"
                />
              </div>
            ) : null}

            {(activeTab === 'overview' || activeTab === 'plan') ? (
              <TradePlanWorkspace
                planItems={filteredPlanItems}
                planDiagnostics={plan?.diagnostics}
                trades={filteredTrades}
                onOpenTradeDetail={setSelectedTrade}
              />
            ) : null}

            {activeTab === 'trades' ? (
              <TradePlanWorkspace
                planItems={[]}
                planDiagnostics={plan?.diagnostics}
                trades={filteredTrades}
                onOpenTradeDetail={setSelectedTrade}
              />
            ) : null}
          </section>
        </div>
      </div>

      <Drawer isOpen={isFilterOpen} onClose={() => setIsFilterOpen(false)} title="策略过滤器" width="max-w-md">
        <FilterRail filters={filters} onChange={setFilters} onApply={handleApplyFilters} compact />
      </Drawer>

      <Drawer
        isOpen={selectedTrade != null}
        onClose={() => setSelectedTrade(null)}
        title={selectedTrade ? `${selectedTrade.stockName ?? selectedTrade.code} · ${getModuleLabel(selectedTrade.entryModule)}` : '交易详情'}
        width="max-w-xl"
      >
        {selectedTrade ? (
          <div className="space-y-3">
            <Card variant="bordered" padding="md">
              <p className="text-sm font-semibold text-content-primary">执行快照</p>
              <div className="mt-3 grid grid-cols-2 gap-2 text-xs">
                <div className="rounded-lg bg-white/5 p-2">
                  <span className="text-content-quaternary">股票名称</span>
                  <p className="mt-1 text-content-primary">{selectedTrade.stockName ?? selectedTrade.code}</p>
                </div>
                <div className="rounded-lg bg-white/5 p-2">
                  <span className="text-content-quaternary">成交股数</span>
                  <p className="mt-1 font-mono text-content-primary">{selectedTrade.shares?.toLocaleString() ?? '--'}</p>
                </div>
                <div className="rounded-lg bg-white/5 p-2">
                  <span className="text-content-quaternary">买入日期</span>
                  <p className="mt-1 font-mono text-content-primary">{selectedTrade.entryDate}</p>
                </div>
                <div className="rounded-lg bg-white/5 p-2">
                  <span className="text-content-quaternary">卖出日期</span>
                  <p className="mt-1 font-mono text-content-primary">{selectedTrade.exitDate ?? '--'}</p>
                </div>
                <div className="rounded-lg bg-white/5 p-2">
                  <span className="text-content-quaternary">买入价格</span>
                  <p className="mt-1 font-mono text-content-primary">{selectedTrade.entryPrice?.toFixed(2) ?? '--'}</p>
                </div>
                <div className="rounded-lg bg-white/5 p-2">
                  <span className="text-content-quaternary">卖出价格</span>
                  <p className="mt-1 font-mono text-content-primary">{selectedTrade.exitPrice?.toFixed(2) ?? '--'}</p>
                </div>
                <div className="rounded-lg bg-white/5 p-2">
                  <span className="text-content-quaternary">买入金额</span>
                  <p className="mt-1 font-mono text-content-primary">{selectedTrade.entryAmount?.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) ?? '--'}</p>
                </div>
                <div className="rounded-lg bg-white/5 p-2">
                  <span className="text-content-quaternary">卖出金额</span>
                  <p className="mt-1 font-mono text-content-primary">{selectedTrade.exitAmount?.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) ?? '--'}</p>
                </div>
              </div>
            </Card>
            <Card variant="bordered" padding="md">
              <p className="text-sm font-semibold text-content-primary">风险与结果</p>
              <div className="mt-3 flex flex-wrap gap-2">
                <Badge variant={Number(selectedTrade.pnlPct ?? 0) >= 0 ? 'success' : 'danger'}>
                  收益率 {selectedTrade.pnlPct?.toFixed(2) ?? '--'}%
                </Badge>
                <Badge variant={Number(selectedTrade.pnlAmount ?? 0) >= 0 ? 'success' : 'danger'}>
                  盈亏金额 {selectedTrade.pnlAmount?.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) ?? '--'}
                </Badge>
                <Badge variant="default">出场原因：{getExitReasonLabel(selectedTrade.exitReason)}</Badge>
                <Badge variant={selectedTrade.blockedExit ? 'warning' : 'success'}>
                  {selectedTrade.blockedExit ? '卖出受阻' : '正常卖出'}
                </Badge>
                <Badge variant="default">阶段：{getStageLabel(selectedTrade.stage)}</Badge>
                <Badge variant="default">市场状态：{getRegimeLabel(plan?.regime)}</Badge>
              </div>
            </Card>
          </div>
        ) : null}
      </Drawer>
    </div>
  );
};

export default QuantStrategyPage;
