import type React from 'react';
import type { ReportMeta, ReportSummary as ReportSummaryType } from '../../types/analysis';
import { ScoreGauge } from '../common';
import { formatDateTime } from '../../utils/format';

interface ReportOverviewProps {
  meta: ReportMeta;
  summary: ReportSummaryType;
  isHistory?: boolean;
  onViewDetails?: () => void;
}

/**
 * 报告概览 - 三层便当盒布局
 * Layer 1: 股票信息 (70% opacity)
 * Layer 2: 核心观点 (60% opacity)  
 * Layer 3: 操作建议 + 趋势预测 (50% opacity)
 */
export const ReportOverview: React.FC<ReportOverviewProps> = ({
  meta,
  summary,
  onViewDetails,
}) => {
  // 涨跌色霓虹发光
  const getPriceChangeColor = (changePct: number | undefined): { color: string; shadow: string } => {
    if (changePct === undefined || changePct === null) {
      return { color: 'rgba(255,255,255,0.4)', shadow: 'none' };
    }
    if (changePct > 0) {
      return { 
        color: '#00E676', 
        shadow: '0 0 8px rgba(0, 230, 118, 0.25)' 
      };
    }
    if (changePct < 0) {
      return { 
        color: '#FF3D00', 
        shadow: '0 0 8px rgba(255, 61, 0, 0.25)' 
      };
    }
    return { color: 'rgba(255,255,255,0.4)', shadow: 'none' };
  };

  const formatChangePct = (changePct: number | undefined): string => {
    if (changePct === undefined || changePct === null) return '--';
    const sign = changePct > 0 ? '+' : '';
    return `${sign}${changePct.toFixed(2)}%`;
  };

  const priceStyle = getPriceChangeColor(meta.changePct);

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 items-stretch">
        {/* 左侧：三层便当盒 */}
        <div className="lg:col-span-2 space-y-4">
          
          {/* Layer 1: 股票信息 - 70% opacity */}
          <div 
            className="rounded-[20px] p-5 transition-all duration-300 hover:shadow-[0_8px_32px_rgba(0,242,254,0.08)]"
            style={{
              background: 'rgba(16, 24, 36, 0.7)',
              backdropFilter: 'blur(10px) saturate(0.7)',
              boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.08)',
            }}
          >
            <div className="flex items-start justify-between">
              <div className="flex-1">
                <div className="flex items-center gap-3">
                  <h2 className="text-2xl font-bold text-[rgba(255,255,255,0.95)]">
                    {meta.stockName || meta.stockCode}
                  </h2>
                  {/* 价格与涨跌 - 霓虹发光 */}
                  {meta.currentPrice != null && (
                    <div className="flex items-baseline gap-2">
                      <span 
                        className="text-xl font-bold font-mono"
                        style={{ color: priceStyle.color, textShadow: priceStyle.shadow }}
                      >
                        {meta.currentPrice.toFixed(2)}
                      </span>
                      <span 
                        className="text-sm font-semibold font-mono"
                        style={{ color: priceStyle.color, textShadow: priceStyle.shadow }}
                      >
                        {formatChangePct(meta.changePct)}
                      </span>
                    </div>
                  )}
                </div>
                <div className="flex items-center gap-2 mt-2">
                  <span 
                    className="font-mono text-xs px-2 py-0.5 rounded-lg"
                    style={{
                      color: '#00F2FE',
                      background: 'rgba(0, 242, 254, 0.1)',
                    }}
                  >
                    {meta.stockCode}
                  </span>
                  <span className="text-xs text-[rgba(255,255,255,0.4)] flex items-center gap-1">
                    <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
                    </svg>
                    {formatDateTime(meta.createdAt)}
                  </span>
                </div>
              </div>
            </div>
          </div>

          {/* Layer 2: 核心观点 - 60% opacity */}
          <div 
            className="rounded-[20px] p-5 transition-all duration-300 hover:shadow-[0_8px_32px_rgba(138,43,226,0.06)]"
            style={{
              background: 'rgba(16, 24, 36, 0.6)',
              backdropFilter: 'blur(10px) saturate(0.7)',
              boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.08)',
            }}
          >
            <span 
              className="text-xs font-medium tracking-wider mb-3 block"
              style={{
                background: 'linear-gradient(135deg, #00F2FE, #8A2BE2)',
                WebkitBackgroundClip: 'text',
                WebkitTextFillColor: 'transparent',
                backgroundClip: 'text',
              }}
            >
              核心观点
            </span>
            <p className="text-[rgba(255,255,255,0.85)] text-sm leading-relaxed whitespace-pre-wrap text-left">
              {summary.analysisSummary || '暂无核心观点分析'}
            </p>
          </div>

          {/* Layer 3: 操作建议 + 趋势预测 - 50% opacity */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {/* 操作建议 */}
            <div 
              className="rounded-[20px] p-4 cursor-pointer transition-all duration-300 hover:bg-[rgba(16,24,36,0.55)]"
              style={{
                background: 'rgba(16, 24, 36, 0.5)',
                backdropFilter: 'blur(10px) saturate(0.7)',
                boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.08)',
              }}
            >
              <div className="flex items-start gap-3">
                <div 
                  className="w-8 h-8 rounded-xl flex items-center justify-center flex-shrink-0"
                  style={{
                    background: 'rgba(0, 230, 118, 0.1)',
                  }}
                >
                  <svg className="w-4 h-4" style={{ color: '#00E676' }} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4" />
                  </svg>
                </div>
                <div>
                  <h4 className="text-xs font-medium mb-1" style={{ color: '#00E676' }}>操作建议</h4>
                  <p className="text-[rgba(255,255,255,0.9)] text-sm font-medium">
                    {summary.operationAdvice || '暂无建议'}
                  </p>
                </div>
              </div>
            </div>

            {/* 趋势预测 */}
            <div 
              className="rounded-[20px] p-4 cursor-pointer transition-all duration-300 hover:bg-[rgba(16,24,36,0.55)]"
              style={{
                background: 'rgba(16, 24, 36, 0.5)',
                backdropFilter: 'blur(10px) saturate(0.7)',
                boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.08)',
              }}
            >
              <div className="flex items-start gap-3">
                <div 
                  className="w-8 h-8 rounded-xl flex items-center justify-center flex-shrink-0"
                  style={{
                    background: 'rgba(0, 242, 254, 0.1)',
                  }}
                >
                  <svg className="w-4 h-4" style={{ color: '#00F2FE' }} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
                  </svg>
                </div>
                <div>
                  <h4 className="text-xs font-medium mb-1" style={{ color: '#00F2FE' }}>趋势预测</h4>
                  <p className="text-[rgba(255,255,255,0.9)] text-sm font-medium">
                    {summary.trendPrediction || '暂无预测'}
                  </p>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* 右侧：情绪仪表盘 */}
        <div className="flex flex-col self-stretch min-h-full gap-3">
          <div 
            className="relative flex-1 flex flex-col min-h-0 rounded-[20px] p-5 overflow-visible"
            style={{
              background: 'rgba(16, 24, 36, 0.7)',
              backdropFilter: 'blur(10px) saturate(0.7)',
              boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.08)',
            }}
          >
            <div className="text-center flex-1 flex flex-col justify-center items-center">
              <h3 className="text-sm font-medium text-[rgba(255,255,255,0.8)] mb-4">市场情绪</h3>
              <ScoreGauge score={summary.sentimentScore} size="lg" />
            </div>
            {/* 装饰性光晕 */}
            <div 
              className="absolute -bottom-4 -right-4 w-24 h-24 rounded-full blur-3xl pointer-events-none opacity-20"
              style={{ background: 'rgba(0, 242, 254, 0.3)' }}
            />
          </div>
          
          {/* 查看详情按钮 */}
          {onViewDetails && (
            <button
              onClick={onViewDetails}
              className="w-full rounded-[16px] p-4 flex items-center justify-center gap-2 transition-all duration-300 hover:scale-[1.02]"
              style={{
                background: 'rgba(16, 24, 36, 0.5)',
                backdropFilter: 'blur(10px) saturate(0.7)',
                boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.08)',
                border: '1px solid rgba(0, 242, 254, 0.2)',
              }}
            >
              <svg className="w-4 h-4" style={{ color: '#00F2FE' }} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
              </svg>
              <span className="text-sm font-medium" style={{ color: '#00F2FE' }}>查看完整报告</span>
            </button>
          )}
        </div>
      </div>
    </div>
  );
};

export default ReportOverview;