import type React from 'react';
import { useState, useEffect, useCallback } from 'react';
import { backtestApi } from '../api/backtest';
import type { ParsedApiError } from '../api/error';
import { getParsedApiError } from '../api/error';
import { ApiErrorAlert, Card, Badge, Pagination, Button } from '../components/common';
import type {
  BacktestResultItem,
  BacktestRunResponse,
  PerformanceMetrics,
} from '../types/backtest';

// ============ Helpers ============

function pct(value?: number | null): string {
  if (value == null) return '--';
  return `${value.toFixed(1)}%`;
}

function outcomeBadge(outcome?: string) {
  if (!outcome) return <Badge variant="default">--</Badge>;
  switch (outcome) {
    case 'win':
      return <Badge variant="success" glow>盈利</Badge>;
    case 'loss':
      return <Badge variant="danger" glow>亏损</Badge>;
    case 'neutral':
      return <Badge variant="warning">中性</Badge>;
    default:
      return <Badge variant="default">{outcome}</Badge>;
  }
}

function statusBadge(status: string) {
  switch (status) {
    case 'completed':
      return <Badge variant="success">已完成</Badge>;
    case 'insufficient':
      return <Badge variant="warning">数据不足</Badge>;
    case 'error':
      return <Badge variant="danger">错误</Badge>;
    default:
      return <Badge variant="default">{status}</Badge>;
  }
}

function boolIcon(value?: boolean | null) {
  if (value === true) return <span className="text-semantic-success">&#10003;</span>;
  if (value === false) return <span className="text-semantic-danger">&#10007;</span>;
  return <span className="text-content-quaternary">--</span>;
}

// ============ Metric Row ============

const MetricRow: React.FC<{ label: string; value: string; accent?: boolean }> = ({ label, value, accent }) => (
  <div className="flex items-center justify-between py-1.5 border-b border-white/5 last:border-0">
    <span className="text-xs text-content-secondary">{label}</span>
    <span className={`text-sm font-mono font-semibold ${accent ? 'text-brand-primary' : 'text-content-primary'}`}>{value}</span>
  </div>
);

// ============ Performance Card ============

const PerformanceCard: React.FC<{ metrics: PerformanceMetrics; title: string }> = ({ metrics, title }) => (
  <Card variant="gradient" padding="md" className="animate-fade-in">
    <div className="mb-3">
      <span className="text-xs font-semibold tracking-wider uppercase text-brand-secondary">{title}</span>
    </div>
    <MetricRow label="方向准确率" value={pct(metrics.directionAccuracyPct)} accent />
    <MetricRow label="胜率" value={pct(metrics.winRatePct)} accent />
    <MetricRow label="平均模拟收益" value={pct(metrics.avgSimulatedReturnPct)} />
    <MetricRow label="平均股票收益" value={pct(metrics.avgStockReturnPct)} />
    <MetricRow label="止损触发率" value={pct(metrics.stopLossTriggerRate)} />
    <MetricRow label="止盈触发率" value={pct(metrics.takeProfitTriggerRate)} />
    <MetricRow label="平均触发天数" value={metrics.avgDaysToFirstHit != null ? metrics.avgDaysToFirstHit.toFixed(1) : '--'} />
    <div className="mt-3 pt-2 border-t border-white/5 flex items-center justify-between">
      <span className="text-xs text-content-quaternary">评估数</span>
      <span className="text-xs text-content-secondary font-mono">
        {Number(metrics.completedCount)} / {Number(metrics.totalEvaluations)}
      </span>
    </div>
    <div className="flex items-center justify-between">
      <span className="text-xs text-content-quaternary">盈/亏/平</span>
      <span className="text-xs font-mono">
        <span className="text-semantic-success">{metrics.winCount}</span>
        {' / '}
        <span className="text-semantic-danger">{metrics.lossCount}</span>
        {' / '}
        <span className="text-semantic-warning">{metrics.neutralCount}</span>
      </span>
    </div>
  </Card>
);

// ============ Run Summary ============

const RunSummary: React.FC<{ data: BacktestRunResponse }> = ({ data }) => (
  <div className="flex items-center gap-4 px-3 py-2 rounded-lg bg-surface-4 border border-white/5 text-xs font-mono animate-fade-in">
    <span className="text-content-secondary">已处理: <span className="text-content-primary">{data.processed}</span></span>
    <span className="text-content-secondary">已保存: <span className="text-brand-primary">{data.saved}</span></span>
    <span className="text-content-secondary">已完成: <span className="text-semantic-success">{data.completed}</span></span>
    <span className="text-content-secondary">数据不足: <span className="text-semantic-warning">{data.insufficient}</span></span>
    {data.errors > 0 && (
      <span className="text-content-secondary">错误: <span className="text-semantic-danger">{data.errors}</span></span>
    )}
  </div>
);

// ============ Main Page ============

const BacktestPage: React.FC = () => {
  // Input state
  const [codeFilter, setCodeFilter] = useState('');
  const [evalDays, setEvalDays] = useState('');
  const [forceRerun, setForceRerun] = useState(false);
  const [isRunning, setIsRunning] = useState(false);
  const [runResult, setRunResult] = useState<BacktestRunResponse | null>(null);
  const [runError, setRunError] = useState<ParsedApiError | null>(null);
  const [pageError, setPageError] = useState<ParsedApiError | null>(null);

  // Results state
  const [results, setResults] = useState<BacktestResultItem[]>([]);
  const [totalResults, setTotalResults] = useState(0);
  const [currentPage, setCurrentPage] = useState(1);
  const [isLoadingResults, setIsLoadingResults] = useState(false);
  const pageSize = 20;

  // Performance state
  const [overallPerf, setOverallPerf] = useState<PerformanceMetrics | null>(null);
  const [stockPerf, setStockPerf] = useState<PerformanceMetrics | null>(null);
  const [isLoadingPerf, setIsLoadingPerf] = useState(false);

  // Fetch results
  const fetchResults = useCallback(async (page = 1, code?: string, windowDays?: number) => {
    setIsLoadingResults(true);
    try {
      const response = await backtestApi.getResults({ code: code || undefined, evalWindowDays: windowDays, page, limit: pageSize });
      setResults(response.items);
      setTotalResults(response.total);
      setCurrentPage(response.page);
      setPageError(null);
    } catch (err) {
      console.error('Failed to fetch backtest results:', err);
      setPageError(getParsedApiError(err));
    } finally {
      setIsLoadingResults(false);
    }
  }, []);

  // Fetch performance
  const fetchPerformance = useCallback(async (code?: string, windowDays?: number) => {
    setIsLoadingPerf(true);
    try {
      const overall = await backtestApi.getOverallPerformance(windowDays);
      setOverallPerf(overall);

      if (code) {
        const stock = await backtestApi.getStockPerformance(code, windowDays);
        setStockPerf(stock);
      } else {
        setStockPerf(null);
      }
      setPageError(null);
    } catch (err) {
      console.error('Failed to fetch performance:', err);
      setPageError(getParsedApiError(err));
    } finally {
      setIsLoadingPerf(false);
    }
  }, []);

  // Initial load
  useEffect(() => {
    const init = async () => {
      const overall = await backtestApi.getOverallPerformance();
      setOverallPerf(overall);
      const windowDays = overall?.evalWindowDays;
      if (windowDays && !evalDays) {
        setEvalDays(String(windowDays));
      }
      fetchResults(1, undefined, windowDays);
    };
    init();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Run backtest
  const handleRun = async () => {
    setIsRunning(true);
    setRunResult(null);
    setRunError(null);
    try {
      const code = codeFilter.trim() || undefined;
      const evalWindowDays = evalDays ? parseInt(evalDays, 10) : undefined;
      const response = await backtestApi.run({
        code,
        force: forceRerun || undefined,
        minAgeDays: forceRerun ? 0 : undefined,
        evalWindowDays,
      });
      setRunResult(response);
      fetchResults(1, codeFilter.trim() || undefined, evalWindowDays);
      fetchPerformance(codeFilter.trim() || undefined, evalWindowDays);
    } catch (err) {
      setRunError(getParsedApiError(err));
    } finally {
      setIsRunning(false);
    }
  };

  // Filter by code
  const handleFilter = () => {
    const code = codeFilter.trim() || undefined;
    const windowDays = evalDays ? parseInt(evalDays, 10) : undefined;
    setCurrentPage(1);
    fetchResults(1, code, windowDays);
    fetchPerformance(code, windowDays);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleFilter();
    }
  };

  // Pagination
  const totalPages = Math.ceil(totalResults / pageSize);
  const handlePageChange = (page: number) => {
    const windowDays = evalDays ? parseInt(evalDays, 10) : undefined;
    fetchResults(page, codeFilter.trim() || undefined, windowDays);
  };

  return (
    <div className="min-h-screen flex flex-col md:ml-20">
      {/* Header */}
      <header className="flex-shrink-0 px-4 py-3 border-b border-white/5">
        <div className="flex items-center gap-2 max-w-4xl">
          <div className="flex-1 relative">
            <input
              type="text"
              value={codeFilter}
              onChange={(e) => setCodeFilter(e.target.value.toUpperCase())}
              onKeyDown={handleKeyDown}
              placeholder="按股票代码筛选（留空显示全部）"
              disabled={isRunning}
              className="input-modern w-full"
            />
          </div>
          <Button
            variant="ghost"
            size="sm"
            onClick={handleFilter}
            disabled={isLoadingResults}
          >
            筛选
          </Button>
          <div className="flex items-center gap-1 whitespace-nowrap">
            <span className="text-xs text-content-tertiary">窗口期</span>
            <input
              type="number"
              min={1}
              max={120}
              value={evalDays}
              onChange={(e) => setEvalDays(e.target.value)}
              placeholder="10"
              disabled={isRunning}
              className="input-modern w-14 text-center text-xs py-2"
            />
          </div>
          <button
            type="button"
            onClick={() => setForceRerun(!forceRerun)}
            disabled={isRunning}
            className={`
              flex items-center gap-1.5 px-3 py-2 rounded-lg text-xs font-medium
              transition-all duration-200 whitespace-nowrap border cursor-pointer
              ${forceRerun
                ? 'border-brand-primary/40 bg-brand-primary/10 text-brand-primary shadow-glow-primary'
                : 'border-white/10 bg-transparent text-content-tertiary hover:border-white/20 hover:text-content-secondary'
              }
              disabled:opacity-50 disabled:cursor-not-allowed
            `}
          >
            <span className={`
              inline-block w-1.5 h-1.5 rounded-full transition-colors duration-200
              ${forceRerun ? 'bg-brand-primary shadow-glow-primary' : 'bg-white/20'}
            `} />
            强制重跑
          </button>
          <Button
            variant="aurora"
            size="sm"
            onClick={handleRun}
            disabled={isRunning}
          >
            {isRunning ? (
              <>
                <svg className="w-3.5 h-3.5 animate-spin" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                </svg>
                运行中...
              </>
            ) : (
              '运行回测'
            )}
          </Button>
        </div>
        {runResult && (
          <div className="mt-2 max-w-4xl">
            <RunSummary data={runResult} />
          </div>
        )}
        {runError && (
          <ApiErrorAlert error={runError} className="mt-2 max-w-4xl" />
        )}
      </header>

      {/* Main content */}
      <main className="flex-1 flex overflow-hidden p-3 gap-3">
        {/* Left sidebar - Performance */}
        <div className="flex flex-col gap-3 w-64 flex-shrink-0 overflow-y-auto">
          {isLoadingPerf ? (
            <div className="flex items-center justify-center py-8">
              <div className="relative">
                <div className="absolute inset-0 rounded-full animate-pulse-glow bg-brand-primary/20 blur-sm" />
                <div className="w-8 h-8 border-2 border-brand-primary/20 border-t-brand-primary rounded-full animate-spin relative" />
              </div>
            </div>
          ) : overallPerf ? (
            <PerformanceCard metrics={overallPerf} title="整体表现" />
          ) : (
            <Card padding="md">
              <p className="text-xs text-content-secondary text-center py-4">
                暂无回测数据。运行回测以查看性能指标。
              </p>
            </Card>
          )}

          {stockPerf && (
            <PerformanceCard metrics={stockPerf} title={`${stockPerf.code || codeFilter}`} />
          )}
        </div>

        {/* Right content - Results table */}
        <section className="flex-1 overflow-y-auto">
          {pageError ? (
            <ApiErrorAlert error={pageError} className="mb-3" />
          ) : null}
          {isLoadingResults ? (
            <div className="flex flex-col items-center justify-center h-64">
              <div className="relative">
                <div className="absolute inset-0 rounded-full animate-pulse-glow bg-brand-primary/20 blur-lg" />
                <div className="w-10 h-10 border-3 border-brand-primary/20 border-t-brand-primary rounded-full animate-spin relative" />
              </div>
              <p className="mt-3 text-content-secondary text-sm">加载结果中...</p>
            </div>
          ) : results.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-64 text-center">
              <div className="w-12 h-12 mb-3 rounded-xl bg-surface-4 flex items-center justify-center">
                <svg className="w-6 h-6 text-content-tertiary" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
                </svg>
              </div>
              <h3 className="text-base font-medium text-content-primary mb-1.5">暂无结果</h3>
              <p className="text-xs text-content-secondary max-w-xs">
                运行回测以评估历史分析准确性
              </p>
            </div>
          ) : (
            <div className="animate-fade-in">
              <div className="overflow-x-auto rounded-xl border border-white/5">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-surface-4 text-left">
                      <th className="px-3 py-2.5 text-xs font-medium text-content-secondary uppercase tracking-wider">代码</th>
                      <th className="px-3 py-2.5 text-xs font-medium text-content-secondary uppercase tracking-wider">日期</th>
                      <th className="px-3 py-2.5 text-xs font-medium text-content-secondary uppercase tracking-wider">建议</th>
                      <th className="px-3 py-2.5 text-xs font-medium text-content-secondary uppercase tracking-wider">方向</th>
                      <th className="px-3 py-2.5 text-xs font-medium text-content-secondary uppercase tracking-wider">结果</th>
                      <th className="px-3 py-2.5 text-xs font-medium text-content-secondary uppercase tracking-wider text-right">收益%</th>
                      <th className="px-3 py-2.5 text-xs font-medium text-content-secondary uppercase tracking-wider text-center">止损</th>
                      <th className="px-3 py-2.5 text-xs font-medium text-content-secondary uppercase tracking-wider text-center">止盈</th>
                      <th className="px-3 py-2.5 text-xs font-medium text-content-secondary uppercase tracking-wider">状态</th>
                    </tr>
                  </thead>
                  <tbody>
                    {results.map((row) => (
                      <tr
                        key={row.analysisHistoryId}
                        className="border-t border-white/5 hover:bg-surface-5 transition-colors"
                      >
                        <td className="px-3 py-2 font-mono text-brand-primary text-xs">{row.code}</td>
                        <td className="px-3 py-2 text-xs text-content-secondary">{row.analysisDate || '--'}</td>
                        <td className="px-3 py-2 text-xs text-content-primary truncate max-w-[140px]" title={row.operationAdvice || ''}>
                          {row.operationAdvice || '--'}
                        </td>
                        <td className="px-3 py-2 text-xs">
                          <span className="flex items-center gap-1">
                            {boolIcon(row.directionCorrect)}
                            <span className="text-content-tertiary">{row.directionExpected || ''}</span>
                          </span>
                        </td>
                        <td className="px-3 py-2">{outcomeBadge(row.outcome)}</td>
                        <td className="px-3 py-2 text-xs font-mono text-right">
                          <span className={
                            row.simulatedReturnPct != null
                              ? row.simulatedReturnPct > 0 ? 'text-semantic-success' : row.simulatedReturnPct < 0 ? 'text-semantic-danger' : 'text-content-secondary'
                              : 'text-content-tertiary'
                          }>
                            {pct(row.simulatedReturnPct)}
                          </span>
                        </td>
                        <td className="px-3 py-2 text-center">{boolIcon(row.hitStopLoss)}</td>
                        <td className="px-3 py-2 text-center">{boolIcon(row.hitTakeProfit)}</td>
                        <td className="px-3 py-2">{statusBadge(row.evalStatus)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {/* Pagination */}
              <div className="mt-4">
                <Pagination
                  currentPage={currentPage}
                  totalPages={totalPages}
                  onPageChange={handlePageChange}
                />
              </div>

              <p className="text-xs text-content-tertiary text-center mt-2">
                共 {totalResults} 条结果
              </p>
            </div>
          )}
        </section>
      </main>
    </div>
  );
};

export default BacktestPage;
