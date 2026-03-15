import type React from 'react';
import { useEffect, useState, useCallback } from 'react';
import Markdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { historyApi } from '../../api/history';

interface ReportMarkdownProps {
  recordId: number;
  stockName: string;
  stockCode: string;
  onClose: () => void;
}

// Custom Markdown components with Bento Glassmorphism styling
const MarkdownComponents: Record<string, React.FC<{ children?: React.ReactNode; className?: string; href?: string }>> = {
  // Main title - Glass card with accent
  h1: ({ children }) => (
    <div 
      className="rounded-[20px] p-6 mb-6"
      style={{
        background: 'linear-gradient(135deg, rgba(0, 242, 254, 0.08), rgba(138, 43, 226, 0.08))',
        backdropFilter: 'blur(10px)',
        boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.1), 0 8px 32px rgba(0, 242, 254, 0.05)',
        border: '1px solid rgba(0, 242, 254, 0.15)',
      }}
    >
      <h1 className="text-xl font-bold text-[rgba(255,255,255,0.95)] m-0 flex items-center gap-3">
        <span 
          className="w-8 h-8 rounded-xl flex items-center justify-center"
          style={{ background: 'rgba(0, 242, 254, 0.2)' }}
        >
          <svg className="w-4 h-4" style={{ color: '#00F2FE' }} fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
          </svg>
        </span>
        <span>{children}</span>
      </h1>
    </div>
  ),
  
  // Section headers - Gradient underline style
  h2: ({ children }) => (
    <div className="mt-8 mb-4">
      <h2 
        className="text-sm font-semibold tracking-wide mb-2 inline-block pb-2"
        style={{
          background: 'linear-gradient(135deg, #00F2FE, #8A2BE2)',
          WebkitBackgroundClip: 'text',
          WebkitTextFillColor: 'transparent',
          backgroundClip: 'text',
        }}
      >
        {children}
      </h2>
      <div 
        className="h-px w-full"
        style={{
          background: 'linear-gradient(90deg, rgba(0, 242, 254, 0.3), rgba(138, 43, 226, 0.1), transparent)',
        }}
      />
    </div>
  ),
  
  // Sub-section headers
  h3: ({ children }) => (
    <h3 className="text-[15px] font-semibold text-[rgba(255,255,255,0.9)] mt-6 mb-3 flex items-center gap-2">
      <span 
        className="w-1 h-4 rounded-full"
        style={{ background: 'linear-gradient(180deg, #00F2FE, #8A2BE2)' }}
      />
      {children}
    </h3>
  ),
  
  // Paragraphs with better spacing
  p: ({ children }) => (
    <p className="text-[rgba(255,255,255,0.8)] leading-[1.8] mb-4 text-[14px]">
      {children}
    </p>
  ),
  
  // Important info blocks - Neon border card
  blockquote: ({ children }) => (
    <div 
      className="rounded-[16px] p-5 my-5 relative overflow-hidden"
      style={{
        background: 'linear-gradient(135deg, rgba(0, 242, 254, 0.05), rgba(0, 242, 254, 0.02))',
        boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.05)',
        border: '1px solid rgba(0, 242, 254, 0.15)',
      }}
    >
      <div 
        className="absolute left-0 top-0 bottom-0 w-1 rounded-l-[16px]"
        style={{
          background: 'linear-gradient(180deg, #00F2FE, #8A2BE2)',
          boxShadow: '0 0 12px rgba(0, 242, 254, 0.5)',
        }}
      />
      <div className="text-[rgba(255,255,255,0.85)] text-[14px] leading-relaxed">
        {children}
      </div>
    </div>
  ),
  
  // Unordered lists with neon dots
  ul: ({ children }) => (
    <ul className="list-none pl-0 my-5 space-y-3">
      {children}
    </ul>
  ),
  
  ol: ({ children }) => (
    <ol className="list-none pl-0 my-5 space-y-3 counter-reset-[decimal]">
      {children}
    </ol>
  ),
  
  li: ({ children }) => (
    <li className="flex items-start gap-3 text-[rgba(255,255,255,0.8)] text-[14px] leading-[1.7]">
      <span 
        className="w-2 h-2 rounded-full mt-[7px] flex-shrink-0"
        style={{ 
          background: '#00F2FE',
          boxShadow: '0 0 8px rgba(0, 242, 254, 0.6)',
        }} 
      />
      <span className="flex-1">{children}</span>
    </li>
  ),
  
  // Strong text with glow
  strong: ({ children }) => (
    <strong 
      className="font-semibold"
      style={{
        color: '#00F2FE',
        textShadow: '0 0 12px rgba(0, 242, 254, 0.4)',
      }}
    >
      {children}
    </strong>
  ),
  
  // Emphasized text
  em: ({ children }) => (
    <em className="text-[rgba(255,255,255,0.7)]">
      {children}
    </em>
  ),
  
  // Horizontal rule - Gradient divider
  hr: () => (
    <div 
      className="my-6 h-px"
      style={{
        background: 'linear-gradient(90deg, transparent 0%, rgba(255,255,255,0.15) 50%, transparent 100%)',
      }}
    />
  ),
  
  // Code - Glass effect
  code: ({ children, className }) => {
    const isInline = !className;
    if (isInline) {
      return (
        <code 
          className="px-2 py-1 rounded-lg text-[12px] font-mono"
          style={{
            background: 'rgba(0, 242, 254, 0.1)',
            color: '#00F2FE',
            boxShadow: '0 0 0 1px rgba(0, 242, 254, 0.1)',
          }}
        >
          {children}
        </code>
      );
    }
    return (
      <pre 
        className="rounded-[16px] p-5 my-5 overflow-x-auto"
        style={{
          background: 'rgba(16, 24, 36, 0.8)',
          backdropFilter: 'blur(10px)',
          boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.05)',
          border: '1px solid rgba(255, 255, 255, 0.08)',
        }}
      >
        <code className="text-[13px] font-mono text-[rgba(255,255,255,0.85)] block whitespace-pre">
          {children}
        </code>
      </pre>
    );
  },
  
  // Tables with glass styling
  table: ({ children }) => (
    <div 
      className="overflow-x-auto my-5 rounded-[16px]"
      style={{
        background: 'rgba(16, 24, 36, 0.6)',
        backdropFilter: 'blur(10px)',
        boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.05)',
        border: '1px solid rgba(255, 255, 255, 0.08)',
      }}
    >
      <table className="w-full text-[14px]">
        {children}
      </table>
    </div>
  ),
  
  thead: ({ children }) => (
    <thead style={{ background: 'rgba(0, 242, 254, 0.08)' }}>
      {children}
    </thead>
  ),
  
  th: ({ children }) => (
    <th className="text-left px-5 py-3 text-[rgba(255,255,255,0.9)] font-semibold border-b border-[rgba(255,255,255,0.1)]">
      {children}
    </th>
  ),
  
  tbody: ({ children }) => (
    <tbody>{children}</tbody>
  ),
  
  td: ({ children }) => (
    <td className="px-5 py-3 text-[rgba(255,255,255,0.75)] border-b border-[rgba(255,255,255,0.05)]">
      {children}
    </td>
  ),
  
  tr: ({ children }) => (
    <tr className="last:border-b-0">{children}</tr>
  ),
  
  // Links with hover effect
  a: ({ children, href }) => (
    <a 
      href={href}
      className="transition-all duration-200 hover:underline"
      style={{ 
        color: '#00F2FE',
        textShadow: '0 0 8px rgba(0, 242, 254, 0.3)',
      }}
      target="_blank"
      rel="noopener noreferrer"
    >
      {children}
    </a>
  ),
};

export const ReportMarkdown: React.FC<ReportMarkdownProps> = ({
  recordId,
  stockName,
  stockCode,
  onClose,
}) => {
  const [content, setContent] = useState<string>('');
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isOpen, setIsOpen] = useState(false);

  useEffect(() => {
    setIsOpen(true);
  }, []);

  const handleClose = useCallback(() => {
    setIsOpen(false);
    setTimeout(onClose, 300);
  }, [onClose]);

  useEffect(() => {
    let isMounted = true;

    const fetchMarkdown = async () => {
      setIsLoading(true);
      setError(null);
      try {
        const markdownContent = await historyApi.getMarkdown(recordId);
        if (isMounted) {
          setContent(markdownContent);
        }
      } catch (err) {
        if (isMounted) {
          setError(err instanceof Error ? err.message : '加载报告失败');
        }
      } finally {
        if (isMounted) {
          setIsLoading(false);
        }
      }
    };

    fetchMarkdown();

    return () => {
      isMounted = false;
    };
  }, [recordId]);

  // Handle ESC key
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        handleClose();
      }
    };
    if (isOpen) {
      document.addEventListener('keydown', handleKeyDown);
      document.body.style.overflow = 'hidden';
    }
    return () => {
      document.removeEventListener('keydown', handleKeyDown);
      document.body.style.overflow = '';
    };
  }, [isOpen, handleClose]);

  if (!isOpen && !isLoading) return null;

  return (
    <div 
      className="fixed inset-0 overflow-hidden"
      style={{ zIndex: 100 }}
    >
      {/* Backdrop */}
      <div
        className="absolute inset-0 transition-opacity duration-300"
        style={{
          background: 'rgba(5, 12, 22, 0.85)',
          backdropFilter: 'blur(10px)',
        }}
        onClick={handleClose}
      />
      
      {/* Drawer Panel */}
      <div 
        className={`absolute inset-y-0 right-0 w-full max-w-3xl flex flex-col transform transition-transform duration-300 ease-out ${isOpen ? 'translate-x-0' : 'translate-x-full'}`}
        style={{
          background: 'linear-gradient(180deg, rgba(16, 24, 36, 0.98), rgba(10, 20, 35, 0.99))',
          boxShadow: '-8px 0 32px rgba(0, 0, 0, 0.5)',
          borderLeft: '1px solid rgba(255, 255, 255, 0.08)',
        }}
      >
        {/* Header */}
        <div 
          className="flex items-center justify-between px-6 py-5"
          style={{
            borderBottom: '1px solid rgba(255, 255, 255, 0.08)',
            background: 'rgba(16, 24, 36, 0.5)',
          }}
        >
          <div className="flex items-center gap-4">
            <div 
              className="w-10 h-10 rounded-xl flex items-center justify-center"
              style={{
                background: 'linear-gradient(135deg, rgba(0, 242, 254, 0.15), rgba(138, 43, 226, 0.15))',
                boxShadow: '0 4px 16px rgba(0, 242, 254, 0.1)',
              }}
            >
              <svg 
                className="w-5 h-5" 
                style={{ color: '#00F2FE' }} 
                fill="none" 
                stroke="currentColor" 
                viewBox="0 0 24 24"
              >
                <path 
                  strokeLinecap="round" 
                  strokeLinejoin="round" 
                  strokeWidth={2} 
                  d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" 
                />
              </svg>
            </div>
            <div>
              <h2 className="text-lg font-semibold text-[rgba(255,255,255,0.95)]">
                {stockName || stockCode}
              </h2>
              <p className="text-xs text-[rgba(255,255,255,0.5)] mt-0.5">完整分析报告</p>
            </div>
          </div>
          <button
            type="button"
            onClick={handleClose}
            className="p-2 rounded-xl transition-all duration-200"
            style={{
              background: 'rgba(255, 255, 255, 0.05)',
            }}
          >
            <svg 
              className="w-5 h-5 text-[rgba(255,255,255,0.5)]" 
              fill="none" 
              stroke="currentColor" 
              viewBox="0 0 24 24"
            >
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Content */}
        <div 
          className="flex-1 overflow-y-auto px-6 py-5"
          style={{
            scrollbarWidth: 'thin',
            scrollbarColor: 'rgba(255, 255, 255, 0.2) transparent',
          }}
        >
          {isLoading ? (
            <div className="flex flex-col items-center justify-center h-64">
              <div 
                className="w-12 h-12 border-2 rounded-full animate-spin"
                style={{
                  borderColor: 'rgba(0, 242, 254, 0.2)',
                  borderTopColor: '#00F2FE',
                }}
              />
              <p className="mt-4 text-[rgba(255,255,255,0.5)] text-sm">加载报告中...</p>
            </div>
          ) : error ? (
            <div className="flex flex-col items-center justify-center h-64">
              <div 
                className="w-14 h-14 rounded-2xl flex items-center justify-center mb-4"
                style={{ 
                  background: 'rgba(255, 61, 0, 0.1)',
                  boxShadow: '0 0 24px rgba(255, 61, 0, 0.2)',
                }}
              >
                <svg 
                  className="w-7 h-7" 
                  style={{ color: '#FF3D00' }} 
                  fill="none" 
                  stroke="currentColor" 
                  viewBox="0 0 24 24"
                >
                  <path 
                    strokeLinecap="round" 
                    strokeLinejoin="round" 
                    strokeWidth={2} 
                    d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" 
                  />
                </svg>
              </div>
              <p className="text-[#FF3D00] text-sm mb-4">{error}</p>
              <button
                type="button"
                onClick={handleClose}
                className="px-5 py-2.5 rounded-xl text-sm transition-all duration-200"
                style={{
                  background: 'rgba(255, 255, 255, 0.08)',
                  color: 'rgba(255, 255, 255, 0.8)',
                }}
              >
                关闭
              </button>
            </div>
          ) : (
            <div className="space-y-3">
              <Markdown remarkPlugins={[remarkGfm]} components={MarkdownComponents}>
                {content}
              </Markdown>
            </div>
          )}
        </div>

        {/* Footer */}
        <div 
          className="flex justify-end px-6 py-4"
          style={{
            borderTop: '1px solid rgba(255, 255, 255, 0.08)',
            background: 'rgba(16, 24, 36, 0.5)',
          }}
        >
          <button
            type="button"
            onClick={handleClose}
            className="px-6 py-2.5 rounded-xl text-sm font-medium transition-all duration-200"
            style={{
              background: 'linear-gradient(135deg, rgba(0, 242, 254, 0.15), rgba(138, 43, 226, 0.15))',
              color: 'rgba(255, 255, 255, 0.9)',
              boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.1), 0 4px 16px rgba(0, 242, 254, 0.15)',
              border: '1px solid rgba(0, 242, 254, 0.2)',
            }}
          >
            关闭
          </button>
        </div>
      </div>
    </div>
  );
};

export default ReportMarkdown;