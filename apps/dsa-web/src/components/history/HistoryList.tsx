import type React from 'react';
import { useRef, useCallback, useEffect, useState } from 'react';
import { Trash2 } from 'lucide-react';
import type { HistoryItem } from '../../types/analysis';
import { formatDateTime } from '../../utils/format';
import { historyApi } from '../../api/history';

interface HistoryListProps {
  items: HistoryItem[];
  isLoading: boolean;
  isLoadingMore: boolean;
  hasMore: boolean;
  selectedId?: number;
  onItemClick: (recordId: number) => void;
  onLoadMore: () => void;
  onDelete?: (recordId: number) => void;
  className?: string;
}

// Get sentiment color for the neon indicator
const getSentimentColor = (score: number): string => {
  if (score >= 70) return '#00E676'; // Rise green
  if (score >= 40) return '#00F2FE'; // Aurora blue
  return '#FF3D00'; // Fall red
};

/**
 * 分析历史列表 - Bento Glassmorphism 设计
 * 隐形滚动条，悬停显示毛玻璃背景
 */
export const HistoryList: React.FC<HistoryListProps> = ({
  items,
  isLoading,
  isLoadingMore,
  hasMore,
  selectedId,
  onItemClick,
  onLoadMore,
  onDelete,
  className = '',
}) => {
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const loadMoreTriggerRef = useRef<HTMLDivElement>(null);
  const [deleteConfirmId, setDeleteConfirmId] = useState<number | null>(null);
  const [isDeleting, setIsDeleting] = useState(false);

  const handleObserver = useCallback(
    (entries: IntersectionObserverEntry[]) => {
      const target = entries[0];
      if (target.isIntersecting && hasMore && !isLoading && !isLoadingMore) {
        const container = scrollContainerRef.current;
        if (container && container.scrollHeight > container.clientHeight) {
          onLoadMore();
        }
      }
    },
    [hasMore, isLoading, isLoadingMore, onLoadMore]
  );

  useEffect(() => {
    const trigger = loadMoreTriggerRef.current;
    const container = scrollContainerRef.current;
    if (!trigger || !container) return;

    const observer = new IntersectionObserver(handleObserver, {
      root: container,
      rootMargin: '20px',
      threshold: 0.1,
    });

    observer.observe(trigger);

    return () => {
      observer.disconnect();
    };
  }, [handleObserver]);

  // 处理删除确认
  const handleConfirmDelete = async () => {
    if (!deleteConfirmId) return;

    setIsDeleting(true);
    try {
      const result = await historyApi.deleteRecord(deleteConfirmId);
      if (result.success) {
        setDeleteConfirmId(null);
        if (onDelete) {
          onDelete(deleteConfirmId);
        }
      }
    } catch (error) {
      console.error('Failed to delete history record:', error);
    } finally {
      setIsDeleting(false);
    }
  };

  return (
    <aside className={`flex flex-col h-full ${className}`}>
      {/* Header - 极简风格 */}
      <div className="px-4 py-3 mb-2">
        <h2 className="text-xs font-medium text-[rgba(255,255,255,0.4)] tracking-wider flex items-center gap-2">
          <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          分析历史
        </h2>
      </div>

      {/* Scrollable List - 隐形滚动条 */}
      <div 
        ref={scrollContainerRef} 
        className="flex-1 overflow-y-auto px-3 pb-4"
        style={{ 
          scrollbarWidth: 'none',
          msOverflowStyle: 'none',
        }}
      >
        {/* Loading State */}
        {isLoading ? (
          <div className="flex justify-center py-8">
            <div className="w-5 h-5 border-2 border-[rgba(0,242,254,0.2)] border-t-[#00F2FE] rounded-full animate-spin" />
          </div>
        ) : items.length === 0 ? (
          /* Empty State - 中文 */
          <div className="text-center py-8 text-[rgba(255,255,255,0.35)] text-xs">
            暂无分析记录
          </div>
        ) : (
          /* History Items - 悬停显形 */
          <div className="space-y-1">
            {items.map((item) => {
              const sentimentColor = item.sentimentScore !== undefined 
                ? getSentimentColor(item.sentimentScore) 
                : '#00F2FE';
              const isSelected = selectedId === item.id;

              return (
                <div
                  key={item.id}
                  className={`
                    group relative w-full text-left rounded-xl p-3 transition-all duration-200
                    ${isSelected 
                      ? 'bg-[rgba(16,24,36,0.8)] backdrop-blur-[10px] border-l-2 border-[#00F2FE]' 
                      : 'bg-transparent hover:bg-[rgba(16,24,36,0.7)] hover:backdrop-blur-[10px] border-l-2 border-transparent hover:border-[rgba(0,242,254,0.5)]'
                    }
                  `}
                  style={{
                    boxShadow: isSelected 
                      ? 'inset 0 1px 1px rgba(255, 255, 255, 0.1)' 
                      : 'none'
                  }}
                >
                  {/* 主内容区域 - 点击选择 */}
                  <button
                    type="button"
                    onClick={() => onItemClick(item.id)}
                    className="w-full text-left"
                    aria-selected={isSelected}
                    role="option"
                  >
                    <div className="flex items-center gap-3">
                      {/* Sentiment Neon Indicator */}
                      {item.sentimentScore !== undefined && (
                        <div className="flex flex-col items-center gap-0.5">
                          <span
                            className="w-1 h-8 rounded-full"
                            style={{
                              background: sentimentColor,
                              boxShadow: `0 0 8px ${sentimentColor}40`
                            }}
                          />
                        </div>
                      )}
                      
                      {/* Item Content */}
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center justify-between gap-2">
                          <span className="font-medium text-[rgba(255,255,255,0.9)] truncate text-sm">
                            {item.stockName || item.stockCode}
                          </span>
                          {item.sentimentScore !== undefined && (
                            <span
                              className="text-xs font-mono font-semibold px-2 py-0.5 rounded-lg"
                              style={{
                                color: sentimentColor,
                                background: `${sentimentColor}15`,
                                textShadow: `0 0 8px ${sentimentColor}30`
                              }}
                            >
                              {item.sentimentScore}
                            </span>
                          )}
                        </div>
                        <div className="flex items-center gap-2 mt-1">
                          <span className="text-xs text-[rgba(255,255,255,0.5)] font-mono">
                            {item.stockCode}
                          </span>
                          <span className="text-[rgba(255,255,255,0.2)]">·</span>
                          <span className="text-xs text-[rgba(255,255,255,0.35)]">
                            {formatDateTime(item.createdAt)}
                          </span>
                        </div>
                      </div>
                    </div>
                  </button>

                  {/* 删除按钮 - 悬停显示 */}
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      setDeleteConfirmId(item.id);
                    }}
                    className="absolute right-2 top-1/2 -translate-y-1/2 opacity-0 group-hover:opacity-100 p-1.5 rounded-lg transition-all duration-200 hover:bg-[rgba(255,61,0,0.15)] text-[rgba(255,255,255,0.4)] hover:text-[#FF3D00] disabled:opacity-50 disabled:cursor-not-allowed"
                    title="删除记录"
                    disabled={isDeleting}
                  >
                    <Trash2 className="w-3.5 h-3.5" />
                  </button>
                </div>
              );
            })}

            {/* Load More Trigger */}
            <div ref={loadMoreTriggerRef} className="h-4" />

            {/* Loading More */}
            {isLoadingMore && (
              <div className="flex justify-center py-3">
                <div className="w-4 h-4 border-2 border-[rgba(0,242,254,0.2)] border-t-[#00F2FE] rounded-full animate-spin" />
              </div>
            )}

            {/* End of List - 中文 */}
            {!hasMore && items.length > 0 && (
              <div className="text-center py-3 text-[rgba(255,255,255,0.25)] text-xs">
                已加载全部
              </div>
            )}
          </div>
        )}
      </div>

      {/* 删除确认弹窗 */}
      {deleteConfirmId && (
        <div 
          className="fixed inset-0 z-50 flex items-center justify-center"
          style={{ backgroundColor: 'rgba(5, 12, 22, 0.8)', backdropFilter: 'blur(10px)' }}
          onClick={() => setDeleteConfirmId(null)}
        >
          <div 
            className="rounded-[20px] p-5 max-w-xs w-full mx-4"
            style={{
              background: 'rgba(16, 24, 36, 0.95)',
              backdropFilter: 'blur(20px)',
              boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.1), 0 20px 40px rgba(0, 0, 0, 0.5)',
              border: '1px solid rgba(255, 61, 0, 0.2)',
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <h3 className="text-[rgba(255,255,255,0.9)] font-medium mb-2">删除分析记录</h3>
            <p className="text-sm text-[rgba(255,255,255,0.5)] mb-4">
              确定要删除这条分析记录吗？此操作无法撤销。
            </p>
            <div className="flex justify-end gap-2">
              <button
                onClick={() => setDeleteConfirmId(null)}
                className="px-4 py-2 rounded-xl text-sm text-[rgba(255,255,255,0.6)] hover:text-white hover:bg-[rgba(255,255,255,0.05)] transition-colors"
              >
                取消
              </button>
              <button
                onClick={handleConfirmDelete}
                disabled={isDeleting}
                className="px-4 py-2 rounded-xl text-sm font-medium text-white transition-all disabled:opacity-50 disabled:cursor-not-allowed"
                style={{
                  background: 'linear-gradient(135deg, #FF3D00, #FF6B35)',
                  boxShadow: '0 4px 15px rgba(255, 61, 0, 0.3)',
                }}
              >
                {isDeleting ? '删除中...' : '删除'}
              </button>
            </div>
          </div>
        </div>
      )}
    </aside>
  );
};

export default HistoryList;