import type React from 'react';
import { useState } from 'react';
import type { ReportDetails as ReportDetailsType } from '../../types/analysis';

interface ReportDetailsProps {
  details?: ReportDetailsType;
  recordId?: number;
}

/**
 * 数据追溯 - Bento Glassmorphism 设计
 * 可折叠的原始数据展示
 */
export const ReportDetails: React.FC<ReportDetailsProps> = ({
  details,
  recordId,
}) => {
  const [showRaw, setShowRaw] = useState(false);
  const [showSnapshot, setShowSnapshot] = useState(false);
  const [copied, setCopied] = useState(false);

  if (!details?.rawResult && !details?.contextSnapshot && !recordId) {
    return null;
  }

  const copyToClipboard = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error('Copy failed:', err);
    }
  };

  const renderJson = (data: unknown) => {
    const jsonStr = JSON.stringify(data, null, 2);
    return (
      <div className="relative overflow-hidden mt-3">
        <button
          type="button"
          onClick={() => copyToClipboard(jsonStr)}
          className="absolute top-2 right-2 text-xs text-[rgba(255,255,255,0.4)] hover:text-[#00F2FE] transition-colors z-10"
        >
          {copied ? '已复制' : '复制'}
        </button>
        <pre 
          className="text-xs text-[rgba(255,255,255,0.6)] font-mono overflow-x-auto p-3 rounded-xl text-left w-0 min-w-full max-h-80 overflow-y-auto custom-scrollbar"
          style={{
            background: 'rgba(5, 12, 22, 0.8)',
          }}
        >
          {jsonStr}
        </pre>
      </div>
    );
  };

  return (
    <div 
      className="rounded-[20px] p-5 text-left"
      style={{
        background: 'rgba(16, 24, 36, 0.5)',
        backdropFilter: 'blur(10px) saturate(0.7)',
        boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.08)',
      }}
    >
      <h3 className="text-sm font-medium text-[rgba(255,255,255,0.6)] mb-4">数据追溯</h3>

      {/* Record ID */}
      {recordId && (
        <div className="flex items-center gap-2 text-xs text-[rgba(255,255,255,0.4)] mb-4 pb-4" style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.05)' }}>
          <span>记录 ID:</span>
          <code 
            className="font-mono text-xs px-2 py-0.5 rounded"
            style={{
              color: '#00F2FE',
              background: 'rgba(0, 242, 254, 0.1)',
            }}
          >
            {recordId}
          </code>
        </div>
      )}

      {/* 折叠区域 */}
      <div className="space-y-2">
        {/* 原始分析结果 */}
        {details?.rawResult && (
          <div>
            <button
              type="button"
              onClick={() => setShowRaw(!showRaw)}
              className="w-full flex items-center justify-between p-3 rounded-xl transition-all duration-200 hover:bg-[rgba(255,255,255,0.05)]"
              style={{
                background: showRaw ? 'rgba(255, 255, 255, 0.03)' : 'transparent',
              }}
            >
              <span className="text-xs text-[rgba(255,255,255,0.7)]">原始分析结果</span>
              <svg
                className={`w-4 h-4 text-[rgba(255,255,255,0.4)] transition-transform ${showRaw ? 'rotate-180' : ''}`}
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
              </svg>
            </button>
            {showRaw && (
              <div className="animate-fade-in">
                {renderJson(details.rawResult)}
              </div>
            )}
          </div>
        )}

        {/* 分析快照 */}
        {details?.contextSnapshot && (
          <div>
            <button
              type="button"
              onClick={() => setShowSnapshot(!showSnapshot)}
              className="w-full flex items-center justify-between p-3 rounded-xl transition-all duration-200 hover:bg-[rgba(255,255,255,0.05)]"
              style={{
                background: showSnapshot ? 'rgba(255, 255, 255, 0.03)' : 'transparent',
              }}
            >
              <span className="text-xs text-[rgba(255,255,255,0.7)]">分析快照</span>
              <svg
                className={`w-4 h-4 text-[rgba(255,255,255,0.4)] transition-transform ${showSnapshot ? 'rotate-180' : ''}`}
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
              </svg>
            </button>
            {showSnapshot && (
              <div className="animate-fade-in">
                {renderJson(details.contextSnapshot)}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportDetails;