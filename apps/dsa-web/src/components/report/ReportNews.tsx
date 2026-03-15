import type React from 'react';
import { useState, useEffect, useCallback } from 'react';
import type { ParsedApiError } from '../../api/error';
import { getParsedApiError } from '../../api/error';
import { historyApi } from '../../api/history';
import type { NewsIntelItem } from '../../types/analysis';

interface ReportNewsProps {
  recordId?: number;
  limit?: number;
}

/**
 * 相关资讯 - Bento Glassmorphism 设计
 * 半透明毛玻璃卡片，隐形边框
 */
export const ReportNews: React.FC<ReportNewsProps> = ({ recordId, limit = 20 }) => {
  const [isLoading, setIsLoading] = useState(false);
  const [items, setItems] = useState<NewsIntelItem[]>([]);
  const [error, setError] = useState<ParsedApiError | null>(null);

  const fetchNews = useCallback(async () => {
    if (!recordId) return;
    setIsLoading(true);
    setError(null);

    try {
      const response = await historyApi.getNews(recordId, limit);
      setItems(response.items || []);
    } catch (err) {
      setError(getParsedApiError(err));
    } finally {
      setIsLoading(false);
    }
  }, [recordId, limit]);

  useEffect(() => {
    setItems([]);
    setError(null);

    if (recordId) {
      fetchNews();
    }
  }, [recordId, fetchNews]);

  if (!recordId) {
    return null;
  }

  return (
    <div 
      className="rounded-[20px] p-5"
      style={{
        background: 'rgba(16, 24, 36, 0.6)',
        backdropFilter: 'blur(10px) saturate(0.7)',
        boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.08)',
      }}
    >
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-medium text-[rgba(255,255,255,0.6)]">相关资讯</h3>
        <div className="flex items-center gap-2">
          {isLoading && (
            <div className="w-4 h-4 border-2 border-[rgba(0,242,254,0.2)] border-t-[#00F2FE] rounded-full animate-spin" />
          )}
          <button
            type="button"
            onClick={fetchNews}
            className="text-xs text-[#00F2FE] hover:text-white transition-colors"
          >
            刷新
          </button>
        </div>
      </div>

      {/* Error State */}
      {error && !isLoading && (
        <div className="text-xs text-[#FF3D00] mb-3">加载失败，点击刷新重试</div>
      )}

      {/* Loading State */}
      {isLoading && items.length === 0 && (
        <div className="flex items-center gap-2 text-xs text-[rgba(255,255,255,0.4)]">
          <div className="w-4 h-4 border-2 border-[rgba(0,242,254,0.2)] border-t-[#00F2FE] rounded-full animate-spin" />
          加载资讯中...
        </div>
      )}

      {/* Empty State */}
      {!isLoading && !error && items.length === 0 && (
        <div className="text-xs text-[rgba(255,255,255,0.3)]">暂无相关资讯</div>
      )}

      {/* News Items */}
      {!isLoading && !error && items.length > 0 && (
        <div className="space-y-2 max-h-[300px] overflow-y-auto custom-scrollbar">
          {items.map((item, index) => (
            <div
              key={`${item.title}-${index}`}
              className="group p-3 rounded-xl transition-all duration-200 hover:bg-[rgba(255,255,255,0.05)] cursor-pointer"
            >
              <div className="flex items-start justify-between gap-3">
                <div className="flex-1 min-w-0">
                  <p className="text-sm text-[rgba(255,255,255,0.85)] font-medium leading-snug text-left group-hover:text-[#00F2FE] transition-colors">
                    {item.title}
                  </p>
                  {item.snippet && (
                    <p className="text-xs text-[rgba(255,255,255,0.4)] mt-1 text-left line-clamp-2">
                      {item.snippet}
                    </p>
                  )}
                </div>
                {item.url && (
                  <a
                    href={item.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-xs text-[rgba(255,255,255,0.3)] hover:text-[#00F2FE] transition-colors inline-flex items-center gap-1 whitespace-nowrap flex-shrink-0"
                    onClick={(e) => e.stopPropagation()}
                  >
                    查看
                    <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                    </svg>
                  </a>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

export default ReportNews;