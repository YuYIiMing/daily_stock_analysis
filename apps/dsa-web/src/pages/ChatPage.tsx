import React, { useState, useRef, useEffect, useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';
import Markdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import {
  Menu,
  Plus,
  Trash2,
  Sparkles,
  Download,
  Send,
  ChevronRight,
  Brain,
  CheckCircle2,
  XCircle,
  PenLine,
} from 'lucide-react';
import { agentApi } from '../api/agent';
import { ApiErrorAlert, Button } from '../components/common';
import { getParsedApiError } from '../api/error';
import { historyApi } from '../api/history';
import {
  useAgentChatStore,
  type Message,
  type ProgressStep,
} from '../stores/agentChatStore';
import { downloadSession, formatSessionAsMarkdown } from '../utils/chatExport';
import StrategySelector from '../components/chat/StrategySelector';
import { getStoredStrategy, setStoredStrategy, getStrategyById } from '../config/strategies';

interface FollowUpContext {
  stock_code: string;
  stock_name: string | null;
  previous_analysis_summary?: unknown;
  previous_strategy?: unknown;
  previous_price?: number;
  previous_change_pct?: number;
}

// 快速问题示例
const QUICK_QUESTIONS = [
  { label: '用缠论分析茅台', strategy: 'chan_theory' },
  { label: '波浪理论看宁德时代', strategy: 'wave_theory' },
  { label: '分析比亚迪趋势', strategy: 'bull_trend' },
  { label: '箱体震荡策略看中芯国际', strategy: 'box_oscillation' },
  { label: '分析腾讯 hk00700', strategy: 'bull_trend' },
  { label: '用情绪周期分析东方财富', strategy: 'emotion_cycle' },
];

const ChatPage: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [input, setInput] = useState('');
  const [selectedStrategy, setSelectedStrategy] = useState<string>(() => {
    const stored = getStoredStrategy();
    return stored !== null ? stored : '';
  });
  const [expandedThinking, setExpandedThinking] = useState<Set<string>>(new Set());
  const [deleteConfirmId, setDeleteConfirmId] = useState<string | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [sending, setSending] = useState(false);
  const [sendToast, setSendToast] = useState<{
    type: 'success' | 'error';
    message: string;
  } | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const initialFollowUpHandled = useRef(false);
  const followUpContextRef = useRef<FollowUpContext | null>(null);

  const {
    messages,
    loading,
    progressSteps,
    sessionId,
    sessions,
    sessionsLoading,
    chatError,
    loadSessions,
    loadInitialSession,
    switchSession,
    startStream,
    clearCompletionBadge,
  } = useAgentChatStore();

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages, progressSteps]);

  useEffect(() => {
    clearCompletionBadge();
  }, [clearCompletionBadge]);

  useEffect(() => {
    loadInitialSession();
  }, [loadInitialSession]);

  const handleStartNewChat = useCallback(() => {
    followUpContextRef.current = null;
    useAgentChatStore.getState().startNewChat();
    setSidebarOpen(false);
  }, []);

  const handleSwitchSession = useCallback((targetSessionId: string) => {
    switchSession(targetSessionId);
    setSidebarOpen(false);
  }, [switchSession]);

  const confirmDelete = useCallback(() => {
    if (!deleteConfirmId) return;
    agentApi.deleteChatSession(deleteConfirmId).then(() => {
      loadSessions();
      if (deleteConfirmId === sessionId) {
        handleStartNewChat();
      }
    }).catch(() => {});
    setDeleteConfirmId(null);
  }, [deleteConfirmId, sessionId, loadSessions, handleStartNewChat]);

  // Handle follow-up from report page: ?stock=600519&name=贵州茅台&recordId=xxx
  useEffect(() => {
    if (initialFollowUpHandled.current) return;
    const stock = searchParams.get('stock');
    const name = searchParams.get('name');
    const recordId = searchParams.get('recordId');
    if (stock) {
      initialFollowUpHandled.current = true;
      const displayName = name ? `${name}(${stock})` : stock;
      setInput(`请深入分析 ${displayName}`);
      if (recordId) {
        historyApi.getDetail(Number(recordId)).then((report) => {
          const ctx: FollowUpContext = { stock_code: stock, stock_name: name };
          if (report.summary) ctx.previous_analysis_summary = report.summary;
          if (report.strategy) ctx.previous_strategy = report.strategy;
          if (report.meta) {
            ctx.previous_price = report.meta.currentPrice;
            ctx.previous_change_pct = report.meta.changePct;
          }
          followUpContextRef.current = ctx;
        }).catch(() => {});
      }
      setSearchParams({}, { replace: true });
    }
  }, [searchParams, setSearchParams]);

  const handleSend = useCallback(
    async (overrideMessage?: string, overrideStrategy?: string) => {
      const msgText = overrideMessage || input.trim();
      if (!msgText || loading) return;
      const usedStrategy = overrideStrategy || selectedStrategy;
      const strategyInfo = getStrategyById(usedStrategy);
      const usedStrategyName = strategyInfo?.name || '通用';

      const payload = {
        message: msgText,
        session_id: sessionId,
        skills: usedStrategy ? [usedStrategy] : undefined,
        context: followUpContextRef.current ?? undefined,
      };
      followUpContextRef.current = null;

      setInput('');
      await startStream(payload, { strategyName: usedStrategyName });
    },
    [input, loading, selectedStrategy, sessionId, startStream],
  );

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const handleQuickQuestion = (q: (typeof QUICK_QUESTIONS)[0]) => {
    setSelectedStrategy(q.strategy);
    setStoredStrategy(q.strategy);
    handleSend(q.label, q.strategy);
  };

  const toggleThinking = (msgId: string) => {
    setExpandedThinking((prev) => {
      const next = new Set(prev);
      if (next.has(msgId)) next.delete(msgId);
      else next.add(msgId);
      return next;
    });
  };

  const getCurrentStage = (steps: ProgressStep[]): string => {
    if (steps.length === 0) return '正在连接...';
    const last = steps[steps.length - 1];
    if (last.type === 'thinking') return last.message || 'AI 正在思考...';
    if (last.type === 'tool_start')
      return `${last.display_name || last.tool}...`;
    if (last.type === 'tool_done')
      return `${last.display_name || last.tool} 完成`;
    if (last.type === 'generating')
      return last.message || '正在生成最终分析...';
    return '处理中...';
  };

  const renderThinkingBlock = (msg: Message) => {
    if (!msg.thinkingSteps || msg.thinkingSteps.length === 0) return null;
    const isExpanded = expandedThinking.has(msg.id);
    const toolSteps = msg.thinkingSteps.filter((s) => s.type === 'tool_done');
    const totalDuration = toolSteps.reduce(
      (sum, s) => sum + (s.duration || 0),
      0,
    );
    const summary = `${toolSteps.length} 个工具调用 · ${totalDuration.toFixed(1)}s`;

    return (
      <button
        onClick={() => toggleThinking(msg.id)}
        className="flex items-center gap-2 text-xs text-[rgba(255,255,255,0.5)] hover:text-[#00F2FE] transition-colors mb-2 w-full text-left"
      >
        <ChevronRight
          className={`w-3 h-3 transition-transform flex-shrink-0 ${isExpanded ? 'rotate-90' : ''}`}
        />
        <span className="flex items-center gap-1.5">
          <span className="opacity-60">思考过程</span>
          <span className="text-[rgba(255,255,255,0.3)]">·</span>
          <span className="opacity-50">{summary}</span>
        </span>
      </button>
    );
  };

  const renderThinkingDetails = (steps: ProgressStep[]) => (
    <div className="mb-3 pl-4 border-l-2 border-[rgba(0,242,254,0.2)] space-y-1 animate-fade-in">
      {steps.map((step, idx) => {
        let icon: React.ReactNode = <span>⋯</span>;
        let text = '';
        let colorClass = 'text-[rgba(255,255,255,0.5)]';
        if (step.type === 'thinking') {
          icon = <Brain className="w-3 h-3" />;
          text = step.message || `第 ${step.step} 步：思考`;
          colorClass = 'text-[#00F2FE]';
        } else if (step.type === 'tool_start') {
          icon = <Sparkles className="w-3 h-3" />;
          text = `${step.display_name || step.tool}...`;
          colorClass = 'text-[#00F2FE]';
        } else if (step.type === 'tool_done') {
          icon = step.success ? (
            <CheckCircle2 className="w-3 h-3 text-[#00E676]" />
          ) : (
            <XCircle className="w-3 h-3 text-[#FF3D00]" />
          );
          text = `${step.display_name || step.tool} (${step.duration}s)`;
          colorClass = step.success ? 'text-[#00E676]' : 'text-[#FF3D00]';
        } else if (step.type === 'generating') {
          icon = <PenLine className="w-3 h-3" />;
          text = step.message || '生成分析';
          colorClass = 'text-[#8A2BE2]';
        }
        return (
          <div
            key={idx}
            className={`flex items-center gap-2 text-xs py-0.5 ${colorClass}`}
          >
            <span className="w-4 flex-shrink-0 flex items-center justify-center">
              {icon}
            </span>
            <span className="leading-relaxed">{text}</span>
          </div>
        );
      })}
    </div>
  );

  const sidebarContent = (
    <>
      <div className="p-4 border-b border-[rgba(255,255,255,0.05)] flex items-center justify-between">
        <span className="text-sm font-medium text-[rgba(255,255,255,0.9)]">对话历史</span>
        <button
          onClick={handleStartNewChat}
          className="p-2 rounded-xl hover:bg-[rgba(255,255,255,0.05)] transition-colors text-[rgba(255,255,255,0.5)] hover:text-white"
          title="新建对话"
          aria-label="开始新对话"
        >
          <Plus className="w-4 h-4" />
        </button>
      </div>
      <div className="flex-1 overflow-y-auto custom-scrollbar p-2">
        {sessionsLoading ? (
          <div className="p-4 text-center text-xs text-[rgba(255,255,255,0.4)]">加载中...</div>
        ) : sessions.length === 0 ? (
          <div className="p-4 text-center text-xs text-[rgba(255,255,255,0.4)]">暂无对话历史</div>
        ) : (
          sessions.map((s) => (
            <button
              key={s.session_id}
              onClick={() => handleSwitchSession(s.session_id)}
              className={`w-full text-left p-3 rounded-xl mb-1 transition-all duration-200 group ${
                s.session_id === sessionId 
                  ? 'bg-[rgba(0,242,254,0.1)] border border-[rgba(0,242,254,0.2)]' 
                  : 'hover:bg-[rgba(255,255,255,0.03)]'
              }`}
            >
              <div className="flex items-center justify-between gap-2">
                <span className={`text-sm truncate flex-1 ${
                  s.session_id === sessionId ? 'text-[#00F2FE]' : 'text-[rgba(255,255,255,0.7)]'
                }`}>
                  {s.title}
                </span>
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    setDeleteConfirmId(s.session_id);
                  }}
                  className="opacity-0 group-hover:opacity-100 p-1 rounded-lg hover:bg-[rgba(255,61,0,0.1)] text-[rgba(255,255,255,0.4)] hover:text-[#FF3D00] transition-all flex-shrink-0"
                  title="删除"
                  aria-label="删除对话"
                >
                  <Trash2 className="w-3.5 h-3.5" />
                </button>
              </div>
              <div className="text-xs text-[rgba(255,255,255,0.4)] mt-1">
                {s.message_count} 条消息
                {s.last_active &&
                  ` · ${new Date(s.last_active).toLocaleDateString('zh-CN', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })}`}
              </div>
            </button>
          ))
        )}
      </div>
    </>
  );

  return (
    <div className="h-screen flex max-w-6xl mx-auto w-full p-4 md:p-6 gap-4">
      {/* Desktop sidebar - Gemini风格 */}
      <div 
        className="hidden md:flex flex-col w-64 flex-shrink-0 rounded-[20px] overflow-hidden"
        style={{
          background: 'rgba(16, 24, 36, 0.6)',
          backdropFilter: 'blur(10px) saturate(0.7)',
          boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.08)',
        }}
      >
        {sidebarContent}
      </div>

      {/* Mobile sidebar overlay */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 z-40 md:hidden"
          onClick={() => setSidebarOpen(false)}
          role="dialog"
          aria-modal="true"
          aria-label="Chat history sidebar"
        >
          <div className="absolute inset-0 bg-black/60" />
          <div
            className="absolute left-0 top-0 bottom-0 w-72 flex flex-col overflow-hidden border-r border-[rgba(255,255,255,0.05)] shadow-2xl"
            style={{
              background: 'rgba(16, 24, 36, 0.9)',
              backdropFilter: 'blur(20px)',
            }}
            onClick={(e) => e.stopPropagation()}
          >
            {sidebarContent}
          </div>
        </div>
      )}

      {/* Delete confirmation dialog */}
      {deleteConfirmId && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center"
          style={{ backgroundColor: 'rgba(5, 12, 22, 0.8)', backdropFilter: 'blur(10px)' }}
          onClick={() => setDeleteConfirmId(null)}
          role="dialog"
          aria-modal="true"
          aria-label="Delete confirmation"
        >
          <div
            className="rounded-[20px] p-6 max-w-sm mx-4 shadow-2xl"
            style={{
              background: 'rgba(16, 24, 36, 0.9)',
              backdropFilter: 'blur(20px)',
              boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.1), 0 20px 40px rgba(0, 0, 0, 0.5)',
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <h3 className="text-[rgba(255,255,255,0.9)] font-medium mb-2">删除对话</h3>
            <p className="text-sm text-[rgba(255,255,255,0.5)] mb-5">
              此操作无法撤销，确定要删除吗？
            </p>
            <div className="flex justify-end gap-3">
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setDeleteConfirmId(null)}
              >
                取消
              </Button>
              <Button
                variant="danger"
                size="sm"
                onClick={confirmDelete}
              >
                删除
              </Button>
            </div>
          </div>
        </div>
      )}

      {/* Main chat area - Gemini风格 */}
      <div className="flex-1 flex flex-col min-w-0">
        <header className="mb-4 flex-shrink-0">
          <h1 className="text-2xl font-bold text-[rgba(255,255,255,0.95)] mb-2 flex items-center gap-2">
            <button
              onClick={() => setSidebarOpen(true)}
              className="md:hidden p-1.5 -ml-1 rounded-xl hover:bg-[rgba(255,255,255,0.05)] transition-colors text-[rgba(255,255,255,0.5)] hover:text-white"
              title="对话历史"
              aria-label="切换对话历史"
            >
              <Menu className="w-5 h-5" />
            </button>
            <div 
              className="w-8 h-8 rounded-xl flex items-center justify-center"
              style={{
                background: 'linear-gradient(135deg, #00F2FE, #8A2BE2)',
              }}
            >
              <Sparkles className="w-5 h-5 text-white" />
            </div>
            问股
          </h1>
          <p className="text-[rgba(255,255,255,0.5)] text-sm">
            向AI询问股票分析，获取基于策略的交易建议
          </p>
          {messages.length > 0 && (
            <div className="mt-2 flex gap-2 items-center">
              <Button
                variant="ghost"
                size="sm"
                onClick={() => downloadSession(messages)}
              >
                <Download className="w-4 h-4" />
                导出
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={async () => {
                  if (sending) return;
                  setSending(true);
                  setSendToast(null);
                  try {
                    const content = formatSessionAsMarkdown(messages);
                    await agentApi.sendChat(content);
                    setSendToast({ type: 'success', message: '已发送到通知渠道' });
                    setTimeout(() => setSendToast(null), 3000);
                  } catch (err) {
                    const parsed = getParsedApiError(err);
                    setSendToast({
                      type: 'error',
                      message: parsed.message || '发送失败',
                    });
                    setTimeout(() => setSendToast(null), 5000);
                  } finally {
                    setSending(false);
                  }
                }}
                disabled={sending}
              >
                {sending ? (
                  <span className="w-4 h-4 border-2 border-brand-primary border-t-transparent rounded-full animate-spin" />
                ) : (
                  <Send className="w-4 h-4" />
                )}
                发送
              </Button>
              {sendToast && (
                <span
                  className={`text-sm ${sendToast.type === 'success' ? 'text-[#00E676]' : 'text-[#FF3D00]'}`}
                >
                  {sendToast.message}
                </span>
              )}
            </div>
          )}
        </header>

        <div 
          className="flex-1 flex flex-col overflow-hidden min-h-0 relative z-10 rounded-[20px]"
          style={{
            background: 'rgba(16, 24, 36, 0.5)',
            backdropFilter: 'blur(10px) saturate(0.7)',
            boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.08)',
          }}
        >
          {/* Messages - Gemini风格 */}
          <div className="flex-1 overflow-y-auto p-4 md:p-6 space-y-6 custom-scrollbar relative z-10">
            {messages.length === 0 && !loading ? (
              <div className="h-full flex flex-col items-center justify-center text-center">
                <div 
                  className="w-16 h-16 mb-4 rounded-2xl flex items-center justify-center"
                  style={{
                    background: 'linear-gradient(135deg, rgba(0,242,254,0.2), rgba(138,43,226,0.2))',
                  }}
                >
                  <Brain className="w-8 h-8 text-[#00F2FE]" />
                </div>
                <h3 className="text-lg font-medium text-[rgba(255,255,255,0.9)] mb-2">
                  开始对话
                </h3>
                <p className="text-sm text-[rgba(255,255,255,0.5)] max-w-sm mb-6">
                  输入"分析600519"或"茅台现在适合买入吗"，AI将使用实时数据生成决策报告
                </p>
                <div className="flex flex-wrap gap-2 justify-center max-w-lg">
                  {QUICK_QUESTIONS.map((q, i) => (
                    <button
                      key={i}
                      onClick={() => handleQuickQuestion(q)}
                      className="px-4 py-2 rounded-xl text-sm transition-all duration-200 hover:scale-105"
                      style={{
                        background: 'rgba(255, 255, 255, 0.05)',
                        border: '1px solid rgba(255, 255, 255, 0.1)',
                        color: 'rgba(255, 255, 255, 0.7)',
                      }}
                      onMouseEnter={(e) => {
                        e.currentTarget.style.borderColor = 'rgba(0, 242, 254, 0.4)';
                        e.currentTarget.style.background = 'rgba(0, 242, 254, 0.1)';
                      }}
                      onMouseLeave={(e) => {
                        e.currentTarget.style.borderColor = 'rgba(255, 255, 255, 0.1)';
                        e.currentTarget.style.background = 'rgba(255, 255, 255, 0.05)';
                      }}
                    >
                      {q.label}
                    </button>
                  ))}
                </div>
              </div>
            ) : (
              messages.map((msg) => (
                <div
                  key={msg.id}
                  className={`flex gap-4 ${msg.role === 'user' ? 'flex-row-reverse' : ''}`}
                >
                  {/* Avatar - Gemini风格 */}
                  <div
                    className={`w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0 text-sm font-medium shadow-lg ${
                      msg.role === 'user'
                        ? ''
                        : ''
                    }`}
                    style={{
                      background: msg.role === 'user' 
                        ? 'linear-gradient(135deg, #00F2FE, #8A2BE2)'
                        : 'rgba(255, 255, 255, 0.1)',
                      boxShadow: msg.role === 'user'
                        ? '0 4px 20px rgba(0, 242, 254, 0.3)'
                        : '0 4px 20px rgba(0, 0, 0, 0.2)',
                    }}
                  >
                    {msg.role === 'user' ? (
                      <span className="text-white">我</span>
                    ) : (
                      <Sparkles className="w-4 h-4 text-[#00F2FE]" />
                    )}
                  </div>
                  
                  {/* Message Bubble - Gemini风格 */}
                  <div
                    className={`max-w-[80%] rounded-2xl px-5 py-3.5 shadow-lg ${
                      msg.role === 'user'
                        ? 'rounded-tr-sm'
                        : 'rounded-tl-sm'
                    }`}
                    style={{
                      background: msg.role === 'user'
                        ? 'linear-gradient(135deg, rgba(0, 242, 254, 0.15), rgba(138, 43, 226, 0.15))'
                        : 'rgba(255, 255, 255, 0.05)',
                      border: msg.role === 'user'
                        ? '1px solid rgba(0, 242, 254, 0.2)'
                        : '1px solid rgba(255, 255, 255, 0.08)',
                      boxShadow: msg.role === 'user'
                        ? '0 4px 20px rgba(0, 242, 254, 0.1)'
                        : '0 4px 20px rgba(0, 0, 0, 0.2)',
                    }}
                  >
                    {msg.role === 'assistant' && msg.strategyName && (
                      <div className="mb-2">
                        <span 
                          className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-xs"
                          style={{
                            background: 'rgba(0, 242, 254, 0.1)',
                            border: '1px solid rgba(0, 242, 254, 0.2)',
                            color: '#00F2FE',
                          }}
                        >
                          <Sparkles className="w-3 h-3" />
                          {msg.strategyName}
                        </span>
                      </div>
                    )}
                    {msg.role === 'assistant' && renderThinkingBlock(msg)}
                    {msg.role === 'assistant' &&
                      expandedThinking.has(msg.id) &&
                      msg.thinkingSteps &&
                      renderThinkingDetails(msg.thinkingSteps)}
                    {msg.role === 'assistant' ? (
                      <div
                        className="prose prose-invert prose-sm max-w-none
                        prose-headings:text-[rgba(255,255,255,0.9)] prose-headings:font-semibold prose-headings:mt-3 prose-headings:mb-1.5
                        prose-h1:text-lg prose-h2:text-base prose-h3:text-sm
                        prose-p:leading-relaxed prose-p:mb-2 prose-p:last:mb-0 prose-p:text-[rgba(255,255,255,0.8)]
                        prose-strong:text-[rgba(255,255,255,0.95)] prose-strong:font-semibold
                        prose-ul:my-1.5 prose-ol:my-1.5 prose-li:my-0.5
                        prose-code:text-[#00F2FE] prose-code:bg-[rgba(0,242,254,0.1)] prose-code:px-1 prose-code:py-0.5 prose-code:rounded prose-code:text-xs
                        prose-pre:bg-[rgba(0,0,0,0.3)] prose-pre:border prose-pre:border-[rgba(255,255,255,0.1)] prose-pre:rounded-xl prose-pre:p-3
                        prose-table:w-full prose-table:text-sm
                        prose-th:text-[rgba(255,255,255,0.9)] prose-th:font-medium prose-th:border-[rgba(255,255,255,0.1)] prose-th:px-3 prose-th:py-1.5 prose-th:bg-[rgba(255,255,255,0.05)]
                        prose-td:border-[rgba(255,255,255,0.1)] prose-td:px-3 prose-td:py-1.5 prose-td:text-[rgba(255,255,255,0.7)]
                        prose-hr:border-[rgba(255,255,255,0.1)] prose-hr:my-3
                        prose-a:text-[#00F2FE] prose-a:no-underline hover:prose-a:underline
                        prose-blockquote:border-l-2 prose-blockquote:border-[#00F2FE] prose-blockquote:text-[rgba(255,255,255,0.6)] prose-blockquote:pl-3
                      "
                      >
                        <Markdown remarkPlugins={[remarkGfm]}>
                          {msg.content}
                        </Markdown>
                      </div>
                    ) : (
                      <p className="text-[rgba(255,255,255,0.9)] leading-relaxed">
                        {msg.content}
                      </p>
                    )}
                  </div>
                </div>
              ))
            )}

            {loading && (
              <div className="flex gap-4">
                <div 
                  className="w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0"
                  style={{
                    background: 'rgba(255, 255, 255, 0.1)',
                    boxShadow: '0 4px 20px rgba(0, 0, 0, 0.2)',
                  }}
                >
                  <Sparkles className="w-4 h-4 text-[#00F2FE]" />
                </div>
                <div 
                  className="rounded-2xl rounded-tl-sm px-5 py-4 min-w-[200px] max-w-[80%]"
                  style={{
                    background: 'rgba(255, 255, 255, 0.05)',
                    border: '1px solid rgba(255, 255, 255, 0.08)',
                  }}
                >
                  <div className="flex items-center gap-3 text-sm text-[rgba(255,255,255,0.6)]">
                    <div className="relative w-5 h-5 flex-shrink-0">
                      <div className="absolute inset-0 rounded-full border-2 border-[rgba(0,242,254,0.2)]" />
                      <div className="absolute inset-0 rounded-full border-2 border-[#00F2FE] border-t-transparent animate-spin" />
                    </div>
                    <span>{getCurrentStage(progressSteps)}</span>
                  </div>
                </div>
              </div>
            )}

            <div ref={messagesEndRef} />
          </div>

          {/* Input area - Gemini风格 */}
          <div className="p-4 md:p-5 relative z-20">
            {chatError ? (
              <ApiErrorAlert error={chatError} className="mb-3" />
            ) : null}
            
            {/* Strategy selection */}
            <div className="mb-3">
              <StrategySelector
                selectedStrategy={selectedStrategy}
                onStrategyChange={(id) => {
                  setSelectedStrategy(id);
                  setStoredStrategy(id);
                }}
                disabled={loading}
              />
            </div>

            {/* Input box */}
            <div className="flex gap-3 items-end">
              <div className="flex-1 relative">
                <textarea
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  onKeyDown={handleKeyDown}
                  placeholder="输入消息..."
                  disabled={loading}
                  rows={1}
                  className="w-full rounded-2xl py-3 px-4 pr-12 resize-none text-sm outline-none transition-all"
                  style={{
                    background: 'rgba(255, 255, 255, 0.05)',
                    border: '1px solid rgba(255, 255, 255, 0.1)',
                    color: 'rgba(255, 255, 255, 0.9)',
                    minHeight: '48px',
                    maxHeight: '200px',
                  }}
                  onFocus={(e) => {
                    e.currentTarget.style.borderColor = 'rgba(0, 242, 254, 0.3)';
                    e.currentTarget.style.background = 'rgba(255, 255, 255, 0.08)';
                  }}
                  onBlur={(e) => {
                    e.currentTarget.style.borderColor = 'rgba(255, 255, 255, 0.1)';
                    e.currentTarget.style.background = 'rgba(255, 255, 255, 0.05)';
                  }}
                  onInput={(e) => {
                    const t = e.target as HTMLTextAreaElement;
                    t.style.height = 'auto';
                    t.style.height = `${Math.min(t.scrollHeight, 200)}px`;
                  }}
                />
                <button
                  onClick={() => handleSend()}
                  disabled={!input.trim() || loading}
                  className="absolute right-2 bottom-2 w-9 h-9 rounded-xl flex items-center justify-center transition-all disabled:opacity-30 disabled:cursor-not-allowed hover:scale-105"
                  style={{
                    background: input.trim() && !loading
                      ? 'linear-gradient(135deg, #00F2FE, #8A2BE2)'
                      : 'rgba(255, 255, 255, 0.1)',
                    boxShadow: input.trim() && !loading
                      ? '0 4px 15px rgba(0, 242, 254, 0.3)'
                      : 'none',
                  }}
                >
                  {loading ? (
                    <span className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                  ) : (
                    <Send className="w-4 h-4 text-white" />
                  )}
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ChatPage;