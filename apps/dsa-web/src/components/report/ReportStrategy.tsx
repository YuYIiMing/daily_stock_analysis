import type React from 'react';
import type { ReportStrategy as ReportStrategyType } from '../../types/analysis';

interface ReportStrategyProps {
  strategy?: ReportStrategyType;
}

interface StrategyItemProps {
  label: string;
  value?: string;
  color: string;
  glowColor: string;
}

const StrategyItem: React.FC<StrategyItemProps> = ({
  label,
  value,
  color,
  glowColor,
}) => (
  <div 
    className="relative overflow-hidden rounded-[16px] p-4 transition-all duration-300 hover:bg-[rgba(255,255,255,0.03)]"
    style={{
      background: 'rgba(16, 24, 36, 0.5)',
      backdropFilter: 'blur(10px) saturate(0.7)',
      boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.06)',
    }}
  >
    <div className="flex flex-col">
      <span className="text-xs text-[rgba(255,255,255,0.4)] mb-1">{label}</span>
      <span
        className="text-lg font-bold font-mono"
        style={{ 
          color: value ? color : 'rgba(255,255,255,0.3)',
          textShadow: value ? `0 0 8px ${glowColor}` : 'none',
        }}
      >
        {value || '—'}
      </span>
    </div>
    {/* 底部霓虹指示条 */}
    {value && (
      <div
        className="absolute bottom-0 left-0 right-0 h-[2px]"
        style={{ 
          background: `linear-gradient(90deg, transparent, ${color}, transparent)`,
          opacity: 0.6,
        }}
      />
    )}
  </div>
);

/**
 * 狙击点位 - Bento Glassmorphism 设计
 * 四个便当盒展示买入/止损/止盈点位
 */
export const ReportStrategy: React.FC<ReportStrategyProps> = ({ strategy }) => {
  if (!strategy) {
    return null;
  }

  const strategyItems = [
    {
      label: '理想买入',
      value: strategy.idealBuy,
      color: '#00E676',
      glowColor: 'rgba(0, 230, 118, 0.25)',
    },
    {
      label: '二次买入',
      value: strategy.secondaryBuy,
      color: '#00F2FE',
      glowColor: 'rgba(0, 242, 254, 0.25)',
    },
    {
      label: '止损价位',
      value: strategy.stopLoss,
      color: '#FF3D00',
      glowColor: 'rgba(255, 61, 0, 0.25)',
    },
    {
      label: '止盈目标',
      value: strategy.takeProfit,
      color: '#8A2BE2',
      glowColor: 'rgba(138, 43, 226, 0.25)',
    },
  ];

  return (
    <div 
      className="rounded-[20px] p-5"
      style={{
        background: 'rgba(16, 24, 36, 0.6)',
        backdropFilter: 'blur(10px) saturate(0.7)',
        boxShadow: 'inset 0 1px 1px rgba(255, 255, 255, 0.08)',
      }}
    >
      <h3 className="text-sm font-medium text-[rgba(255,255,255,0.6)] mb-4">狙击点位</h3>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {strategyItems.map((item) => (
          <StrategyItem key={item.label} {...item} />
        ))}
      </div>
    </div>
  );
};

export default ReportStrategy;