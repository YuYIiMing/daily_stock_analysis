import type React from 'react';
import type { TaskInfo } from '../../types/analysis';

/**
 * Task Item Component - Bento Glassmorphism
 */
interface TaskItemProps {
  task: TaskInfo;
}

const TaskItem: React.FC<TaskItemProps> = ({ task }) => {
  const isPending = task.status === 'pending';
  const isProcessing = task.status === 'processing';

  return (
    <div 
      className="flex items-center gap-3 px-3 py-2.5 rounded-xl"
      style={{
        background: 'rgba(16, 24, 36, 0.6)',
        backdropFilter: 'blur(10px)',
        boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.06)',
      }}
    >
      {/* Status Icon */}
      <div className="shrink-0">
        {isProcessing ? (
          // Processing - spinning icon with aurora glow
          <div className="relative">
            <div 
              className="absolute inset-0 rounded-full blur-sm animate-pulse"
              style={{ background: 'rgba(0, 242, 254, 0.2)' }}
            />
            <svg 
              className="w-4 h-4 animate-spin relative" 
              style={{ color: '#00F2FE' }}
              fill="none" 
              viewBox="0 0 24 24"
            >
              <circle
                className="opacity-25"
                cx="12"
                cy="12"
                r="10"
                stroke="currentColor"
                strokeWidth="4"
              />
              <path
                className="opacity-75"
                fill="currentColor"
                d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
              />
            </svg>
          </div>
        ) : isPending ? (
          // Pending - clock icon
          <svg 
            className="w-4 h-4 text-[rgba(255,255,255,0.3)]" 
            fill="none" 
            stroke="currentColor" 
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"
            />
          </svg>
        ) : null}
      </div>

      {/* Task Info */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-sm font-medium text-[rgba(255,255,255,0.9)] truncate">
            {task.stockName || task.stockCode}
          </span>
          <span className="text-xs text-[rgba(255,255,255,0.4)]">
            {task.stockCode}
          </span>
        </div>
        {task.message && (
          <p className="text-xs text-[rgba(255,255,255,0.5)] truncate mt-0.5">
            {task.message}
          </p>
        )}
      </div>

      {/* Status Badge */}
      <div className="flex-shrink-0">
        <span
          className={`
            text-xs px-2 py-0.5 rounded-full font-medium
            ${isProcessing
              ? 'text-[#00F2FE] border border-[rgba(0,242,254,0.3)]'
              : 'text-[rgba(255,255,255,0.4)] border border-[rgba(255,255,255,0.1)]'
            }
          `}
          style={{
            background: isProcessing ? 'rgba(0, 242, 254, 0.1)' : 'rgba(255, 255, 255, 0.05)',
          }}
        >
          {isProcessing ? '分析中' : '等待中'}
        </span>
      </div>
    </div>
  );
};

/**
 * Task Panel Props
 */
interface TaskPanelProps {
  tasks: TaskInfo[];
  visible?: boolean;
  title?: string;
  className?: string;
}

/**
 * 分析任务面板 - Bento Glassmorphism 设计
 */
export const TaskPanel: React.FC<TaskPanelProps> = ({
  tasks,
  visible = true,
  title = '分析任务',
  className = '',
}) => {
  // Filter active tasks
  const activeTasks = tasks.filter(
    (t) => t.status === 'pending' || t.status === 'processing'
  );

  if (!visible || activeTasks.length === 0) {
    return null;
  }

  const pendingCount = activeTasks.filter((t) => t.status === 'pending').length;
  const processingCount = activeTasks.filter((t) => t.status === 'processing').length;

  return (
    <div 
      className={`rounded-[20px] overflow-hidden ${className}`}
      style={{
        background: 'rgba(16, 24, 36, 0.7)',
        backdropFilter: 'blur(10px) saturate(0.7)',
        boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.08)',
      }}
    >
      {/* Header */}
      <div 
        className="flex items-center justify-between px-4 py-3"
        style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.05)' }}
      >
        <div className="flex items-center gap-2">
          <svg 
            className="w-4 h-4" 
            style={{ color: '#00F2FE' }}
            fill="none" 
            stroke="currentColor" 
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
            />
          </svg>
          <span className="text-sm font-medium text-[rgba(255,255,255,0.8)]">{title}</span>
        </div>
        <div className="flex items-center gap-2 text-xs text-[rgba(255,255,255,0.4)]">
          {processingCount > 0 && (
            <span className="flex items-center gap-1">
              <span 
                className="w-1.5 h-1.5 rounded-full animate-pulse"
                style={{ background: '#00F2FE', boxShadow: '0 0 6px rgba(0, 242, 254, 0.5)' }}
              />
              {processingCount} 进行中
            </span>
          )}
          {pendingCount > 0 && (
            <span>{pendingCount} 等待中</span>
          )}
        </div>
      </div>

      {/* Task List */}
      <div className="p-3 space-y-2 max-h-64 overflow-y-auto custom-scrollbar">
        {activeTasks.map((task) => (
          <TaskItem key={task.taskId} task={task} />
        ))}
      </div>
    </div>
  );
};

export default TaskPanel;