import type React from 'react';
import { useState, useEffect, useCallback, useRef } from 'react';
import { 
  Search, 
  Menu,
  Sparkles,
} from 'lucide-react';
import { ApiErrorAlert, Button } from '../components/common';
import { getParsedApiError } from '../api/error';
import type { HistoryItem, AnalysisReport, TaskInfo } from '../types/analysis';
import { historyApi } from '../api/history';
import { analysisApi, DuplicateTaskError } from '../api/analysis';
import { validateStockCode } from '../utils/validation';
import { getRecentStartDate, getTodayInShanghai } from '../utils/format';
import { useAnalysisStore } from '../stores/analysisStore';
import { ReportSummary, ReportMarkdown } from '../components/report';
import { HistoryList } from '../components/history';
import { TaskPanel } from '../components/tasks';
import { useTaskStream } from '../hooks';

/**
 * 粒子背景组件 - 增强空间感
 */
const ParticleBackground: React.FC = () => {
  return (
    <>
      {/* 粒子层 */}
      <div className="particle-container">
        {Array.from({ length: 10 }).map((_, i) => (
          <div key={i} className="particle" />
        ))}
      </div>
      {/* 极光渐变叠加层 */}
      <div className="aurora-overlay" />
    </>
  );
};

/**
 * HomePage - Bento Glassmorphism 主页面
 * 左侧历史 + 中间报告 + 粒子背景
 */
const HomePage: React.FC = () => {
  const {
    error: analysisError,
    setLoading,
    setError: setStoreError,
  } = useAnalysisStore();

  // Input states
  const [stockCode, setStockCode] = useState('');
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [inputError, setInputError] = useState<string>();

  // History list states
  const [historyItems, setHistoryItems] = useState<HistoryItem[]>([]);
  const [isLoadingHistory, setIsLoadingHistory] = useState(false);
  const [isLoadingMore, setIsLoadingMore] = useState(false);
  const [hasMore, setHasMore] = useState(true);
  const [currentPage, setCurrentPage] = useState(1);
  const pageSize = 20;

  // Report detail states
  const [selectedReport, setSelectedReport] = useState<AnalysisReport | null>(null);
  const [isLoadingReport, setIsLoadingReport] = useState(false);

  // Task queue states
  const [activeTasks, setActiveTasks] = useState<TaskInfo[]>([]);
  const [duplicateError, setDuplicateError] = useState<string | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);

  // Markdown report drawer state
  const [showMarkdownDrawer, setShowMarkdownDrawer] = useState(false);

  // Batch analysis states
  const [isGeneratingReport, setIsGeneratingReport] = useState(false);
  const [showBatchConfirm, setShowBatchConfirm] = useState(false);

  // Track current analysis request
  const analysisRequestIdRef = useRef<number>(0);

  // Update task in list
  const updateTask = useCallback((updatedTask: TaskInfo) => {
    setActiveTasks((prev) => {
      const index = prev.findIndex((t) => t.taskId === updatedTask.taskId);
      if (index >= 0) {
        const newTasks = [...prev];
        newTasks[index] = updatedTask;
        return newTasks;
      }
      return prev;
    });
  }, []);

  // Remove completed/failed tasks
  const removeTask = useCallback((taskId: string) => {
    setActiveTasks((prev) => prev.filter((t) => t.taskId !== taskId));
  }, []);

  // SSE task stream
  useTaskStream({
    onTaskCreated: (task: TaskInfo) => {
      setActiveTasks((prev) => {
        if (prev.some((t) => t.taskId === task.taskId)) return prev;
        return [...prev, task];
      });
    },
    onTaskStarted: updateTask,
    onTaskCompleted: (task: TaskInfo) => {
      fetchHistory();
      setTimeout(() => removeTask(task.taskId), 2000);
    },
    onTaskFailed: (task: TaskInfo) => {
      updateTask(task);
      setStoreError(getParsedApiError(task.error || '分析失败'));
      setTimeout(() => removeTask(task.taskId), 5000);
    },
    onError: () => {
      console.warn('SSE connection lost, reconnecting...');
    },
    enabled: true,
  });

  const currentPageRef = useRef(currentPage);
  currentPageRef.current = currentPage;
  const historyItemsRef = useRef(historyItems);
  historyItemsRef.current = historyItems;
  const selectedReportRef = useRef(selectedReport);
  selectedReportRef.current = selectedReport;

  // Load history list
  const fetchHistory = useCallback(async (autoSelectFirst = false, reset = true, silent = false) => {
    if (!silent) {
      if (reset) {
        setIsLoadingHistory(true);
        setCurrentPage(1);
      } else {
        setIsLoadingMore(true);
      }
    }

    const page = reset ? 1 : currentPageRef.current + 1;

    try {
      const response = await historyApi.getList({
        startDate: getRecentStartDate(30),
        endDate: getTodayInShanghai(),
        page,
        limit: pageSize,
      });

      if (silent && reset) {
        setHistoryItems((prev: HistoryItem[]) => {
          const existingIds = new Set(prev.map((item: HistoryItem) => item.id));
          const newItems = response.items.filter((item: HistoryItem) => !existingIds.has(item.id));
          return newItems.length > 0 ? [...newItems, ...prev] : prev;
        });
      } else if (reset) {
        setHistoryItems(response.items);
        setCurrentPage(1);
      } else {
        setHistoryItems(prev => [...prev, ...response.items]);
        setCurrentPage(page);
      }

      if (!silent) {
        const totalLoaded = reset ? response.items.length : historyItemsRef.current.length + response.items.length;
        setHasMore(totalLoaded < response.total);
      }

      if (autoSelectFirst && response.items.length > 0 && !selectedReportRef.current) {
        const firstItem = response.items[0];
        setIsLoadingReport(true);
        try {
          const report = await historyApi.getDetail(firstItem.id);
          setStoreError(null);
          setSelectedReport(report);
        } catch (err) {
          console.error('Failed to fetch first report:', err);
          setStoreError(getParsedApiError(err));
        } finally {
          setIsLoadingReport(false);
        }
      }
    } catch (err) {
      console.error('Failed to fetch history:', err);
      setStoreError(getParsedApiError(err));
    } finally {
      setIsLoadingHistory(false);
      setIsLoadingMore(false);
    }
  }, [pageSize, setStoreError]);

  // Load more history
  const handleLoadMore = useCallback(() => {
    if (!isLoadingMore && hasMore) {
      fetchHistory(false, false);
    }
  }, [fetchHistory, isLoadingMore, hasMore]);

  // Handle history item deletion
  const handleDeleteHistory = useCallback((deletedId: number) => {
    setHistoryItems(prev => prev.filter(item => item.id !== deletedId));
    if (selectedReport?.meta.id === deletedId) {
      setSelectedReport(null);
    }
  }, [selectedReport?.meta.id]);

  // Initial load
  useEffect(() => {
    fetchHistory(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Background polling every 30s
  useEffect(() => {
    const interval = setInterval(() => {
      fetchHistory(false, true, true);
    }, 30_000);
    return () => clearInterval(interval);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Refresh when tab becomes visible
  useEffect(() => {
    const handleVisibilityChange = () => {
      if (document.visibilityState === 'visible') {
        fetchHistory(false, true, true);
      }
    };
    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => document.removeEventListener('visibilitychange', handleVisibilityChange);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Click history item to load report
  const handleHistoryClick = async (recordId: number) => {
    const requestId = ++analysisRequestIdRef.current;

    try {
      const report = await historyApi.getDetail(recordId);
      if (requestId === analysisRequestIdRef.current) {
        setStoreError(null);
        setSelectedReport(report);
      }
    } catch (err) {
      console.error('Failed to fetch report:', err);
      setStoreError(getParsedApiError(err));
    }
  };

  // Analyze stock
  const handleAnalyze = async () => {
    const { valid, message, normalized } = validateStockCode(stockCode);
    if (!valid) {
      setInputError(message);
      return;
    }

    setInputError(undefined);
    setDuplicateError(null);
    setIsAnalyzing(true);
    setLoading(true);
    setStoreError(null);

    const currentRequestId = ++analysisRequestIdRef.current;

    try {
      const response = await analysisApi.analyzeAsync({
        stockCode: normalized,
        reportType: 'detailed',
      });

      if (currentRequestId === analysisRequestIdRef.current) {
        setStockCode('');
      }

      console.log('Task submitted:', response.taskId);
    } catch (err) {
      console.error('Analysis failed:', err);
      if (currentRequestId === analysisRequestIdRef.current) {
        if (err instanceof DuplicateTaskError) {
          setDuplicateError(`股票 ${err.stockCode} 正在分析中，请稍候`);
        } else {
          setStoreError(getParsedApiError(err as Error));
        }
      }
    } finally {
      setIsAnalyzing(false);
      setLoading(false);
    }
  };

  // Submit on Enter
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && stockCode && !isAnalyzing) {
      handleAnalyze();
    }
  };

  // Batch analyze
  const handleGenerateReport = async (mode: string) => {
    setShowBatchConfirm(false);
    setIsGeneratingReport(true);
    setStoreError(null);

    try {
      const result = await analysisApi.triggerBatch(mode);
      console.log(`Batch task (${mode}) submitted:`, result);
    } catch (err) {
      console.error('Batch analysis failed:', err);
      setStoreError(getParsedApiError(err instanceof Error ? err.message : '启动批量分析失败'));
    } finally {
      setIsGeneratingReport(false);
    }
  };

  const sidebarContent = (
    <div className="flex flex-col gap-3 overflow-hidden min-h-0 h-full">
      <TaskPanel tasks={activeTasks} />
      <HistoryList
        items={historyItems}
        isLoading={isLoadingHistory}
        isLoadingMore={isLoadingMore}
        hasMore={hasMore}
        selectedId={selectedReport?.meta.id}
        onItemClick={(id: number) => { handleHistoryClick(id); setSidebarOpen(false); }}
        onLoadMore={handleLoadMore}
        onDelete={handleDeleteHistory}
        className="max-h-[62vh] md:max-h-[62vh] flex-1 overflow-hidden"
      />
    </div>
  );

  return (
    <>
      {/* 粒子背景 */}
      <ParticleBackground />
      
      {/* 背景层次增强 - 左右模块分隔 */}
      <div 
        className="fixed inset-0 pointer-events-none z-0 hidden md:block"
        style={{
          background: 'linear-gradient(90deg, rgba(5, 12, 22, 0.3) 0%, transparent 20%, transparent 80%, rgba(5, 12, 22, 0.3) 100%)',
        }}
      />
      
      <div
        className="min-h-screen flex flex-col md:grid overflow-hidden w-full relative z-10"
        style={{ 
          gridTemplateColumns: 'minmax(12px, 1fr) 280px 24px minmax(auto, 896px) minmax(12px, 1fr)', 
          gridTemplateRows: 'auto 1fr' 
        }}
      >
        {/* Top input bar - Bento style */}
        <header
          className="md:col-start-2 md:col-end-5 md:row-start-1 py-4 px-4 md:px-0 flex-shrink-0 flex items-center min-w-0 overflow-hidden"
          style={{ 
            background: 'rgba(5, 12, 22, 0.5)',
            backdropFilter: 'blur(10px)',
            borderBottom: '1px solid rgba(255, 255, 255, 0.05)',
          }}
        >
          <div className="flex items-center gap-3 w-full min-w-0 flex-1" style={{ maxWidth: 'min(100%, 1184px)' }}>
            {/* Mobile hamburger */}
            <button
              onClick={() => setSidebarOpen(true)}
              className="md:hidden p-2 -ml-2 rounded-xl transition-all duration-200 flex-shrink-0 text-[rgba(255,255,255,0.4)] hover:text-white"
              title="历史"
              aria-label="Toggle history sidebar"
            >
              <Menu className="w-5 h-5" />
            </button>
            
            <div className="flex-1 relative min-w-0">
              <div className="relative">
                <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-[rgba(255,255,255,0.3)]" />
                <input
                  type="text"
                  value={stockCode}
                  onChange={(e) => {
                    setStockCode(e.target.value.toUpperCase());
                    setInputError(undefined);
                  }}
                  onKeyDown={handleKeyDown}
                  placeholder="输入股票代码，如 600519、00700、AAPL"
                  disabled={isAnalyzing}
                  className="input-bento pl-12"
                  style={inputError ? { borderColor: '#FF3D00' } : undefined}
                  aria-label="Stock code input"
                  aria-invalid={!!inputError}
                />
              </div>
              {inputError && (
                <p className="absolute -bottom-5 left-0 text-xs text-[#FF3D00]">{inputError}</p>
              )}
              {duplicateError && (
                <p className="absolute -bottom-5 left-0 text-xs text-[#00F2FE]">{duplicateError}</p>
              )}
            </div>
            
            <Button
              variant="aurora"
              onClick={handleAnalyze}
              disabled={!stockCode || isAnalyzing}
            >
              {isAnalyzing ? (
                <>
                  <div className="w-4 h-4 border-2 border-[#050C16]/30 border-t-[#050C16] rounded-full animate-spin" />
                  分析中
                </>
              ) : (
                <>
                  <Search className="w-4 h-4" />
                  分析
                </>
              )}
            </Button>
            
            <Button
              variant="nebula"
              onClick={() => setShowBatchConfirm(true)}
              disabled={isGeneratingReport}
            >
              {isGeneratingReport ? (
                <>
                  <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                  生成中
                </>
              ) : (
                <>
                  <Sparkles className="w-4 h-4" />
                  日报
                </>
              )}
            </Button>
          </div>
        </header>

        {/* Desktop sidebar */}
        <div className="hidden md:flex col-start-2 row-start-2 flex-col gap-3 overflow-hidden min-h-0 py-4">
          {sidebarContent}
        </div>

        {/* Mobile sidebar overlay */}
        {sidebarOpen && (
          <div className="fixed inset-0 z-40 md:hidden" onClick={() => setSidebarOpen(false)}>
            <div 
              className="absolute inset-0" 
              style={{ backgroundColor: 'rgba(5, 12, 22, 0.8)', backdropFilter: 'blur(10px)' }} 
            />
            <div
              className="absolute left-0 top-0 bottom-0 w-80 flex flex-col overflow-hidden p-4"
              style={{ 
                background: 'rgba(16, 24, 36, 0.9)',
                backdropFilter: 'blur(20px)',
                borderRight: '1px solid rgba(255, 255, 255, 0.05)',
              }}
              onClick={(e) => e.stopPropagation()}
              role="dialog"
              aria-modal="true"
              aria-label="History sidebar"
            >
              {sidebarContent}
            </div>
          </div>
        )}

        {/* Right report panel */}
        <section className="md:col-start-4 md:row-start-2 flex-1 overflow-y-auto overflow-x-auto px-4 md:px-0 md:pl-1 min-w-0 min-h-0 py-4 custom-scrollbar">
          {analysisError ? (
            <ApiErrorAlert error={analysisError} className="mb-4" />
          ) : null}
          
          {isLoadingReport ? (
            <div className="flex flex-col items-center justify-center h-full">
              <div className="relative">
                <div className="absolute inset-0 rounded-full animate-pulse-subtle bg-[rgba(0,242,254,0.15)] blur-xl" />
                <div className="w-12 h-12 border-3 rounded-full animate-spin relative" style={{ borderColor: 'rgba(0, 242, 254, 0.2)', borderTopColor: '#00F2FE' }} />
              </div>
              <p className="mt-4 text-sm text-[rgba(255,255,255,0.6)]">加载报告中...</p>
            </div>
          ) : selectedReport ? (
            <div className="max-w-4xl">
              <ReportSummary 
                data={selectedReport} 
                isHistory={true}
                onViewDetails={() => setShowMarkdownDrawer(true)}
              />
            </div>
          ) : (
            /* Empty state */
            <div className="flex flex-col items-center justify-center h-full text-center px-4">
              <div 
                className="w-20 h-20 rounded-[20px] flex items-center justify-center mb-6"
                style={{
                  background: 'rgba(16, 24, 36, 0.6)',
                  backdropFilter: 'blur(10px)',
                  boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.08)',
                }}
              >
                <svg className="w-10 h-10 text-[rgba(0,242,254,0.5)]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                </svg>
              </div>
              <h3 className="text-lg font-medium text-[rgba(255,255,255,0.8)] mb-2">选择股票查看分析</h3>
              <p className="text-sm text-[rgba(255,255,255,0.4)] max-w-sm">
                在左侧选择历史分析记录，或在上方输入股票代码开始新的分析
              </p>
            </div>
          )}
        </section>

        {/* Batch confirm modal */}
            {showBatchConfirm && (
          <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
            <div 
              className="absolute inset-0" 
              style={{ backgroundColor: 'rgba(5, 12, 22, 0.8)', backdropFilter: 'blur(10px)' }}
              onClick={() => setShowBatchConfirm(false)}
            />
            <div
              className="relative rounded-[20px] p-6 max-w-sm w-full"
              style={{
                background: 'rgba(16, 24, 36, 0.9)',
                backdropFilter: 'blur(20px)',
                boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.1), 0 20px 40px rgba(0, 0, 0, 0.5)',
              }}
            >
              <h3 className="text-lg font-semibold text-white mb-4">批量分析</h3>
              <p className="text-sm text-[rgba(255,255,255,0.6)] mb-6">
                选择分析模式，将为选定的股票范围生成分析报告
              </p>
              <div className="flex flex-col gap-3">
                <button
                  onClick={() => handleGenerateReport('stocks_only')}
                  className="w-full btn-aurora"
                  disabled={isGeneratingReport}
                >
                  自选股分析
                </button>
                <button
                  onClick={() => handleGenerateReport('market_only')}
                  className="w-full btn-ghost"
                  disabled={isGeneratingReport}
                >
                  大盘分析
                </button>
                <button
                  onClick={() => handleGenerateReport('full')}
                  className="w-full btn-ghost"
                  disabled={isGeneratingReport}
                >
                  全部（自选股+大盘）
                </button>
              </div>
              <button
                onClick={() => setShowBatchConfirm(false)}
                className="absolute top-4 right-4 text-[rgba(255,255,255,0.4)] hover:text-white"
              >
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
          </div>
        )}

        {/* Markdown drawer */}
        {showMarkdownDrawer && selectedReport && selectedReport.meta.id && (
          <ReportMarkdown
            recordId={selectedReport.meta.id}
            stockName={selectedReport.meta.stockName}
            stockCode={selectedReport.meta.stockCode}
            onClose={() => setShowMarkdownDrawer(false)}
          />
        )}
      </div>
    </>
  );
};

export default HomePage;