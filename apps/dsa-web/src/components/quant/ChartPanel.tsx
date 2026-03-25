import type React from 'react';
import { Card } from '../common';

interface ChartPoint {
  x: string;
  y: number;
}

interface ChartPanelProps {
  title: string;
  subtitle: string;
  points: ChartPoint[];
  tone?: 'equity' | 'drawdown';
}

function toPath(points: ChartPoint[], width: number, height: number): string {
  if (points.length === 0) return '';
  const ys = points.map((point) => point.y);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const range = maxY - minY || 1;
  return points
    .map((point, index) => {
      const x = (index / Math.max(points.length - 1, 1)) * width;
      const y = height - ((point.y - minY) / range) * height;
      return `${index === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(' ');
}

function areaPath(points: ChartPoint[], width: number, height: number): string {
  if (points.length === 0) return '';
  const line = toPath(points, width, height);
  return `${line} L ${width} ${height} L 0 ${height} Z`;
}

export const ChartPanel: React.FC<ChartPanelProps> = ({
  title,
  subtitle,
  points,
  tone = 'equity',
}) => {
  const width = 560;
  const height = 220;
  const stroke = tone === 'drawdown' ? 'var(--fall)' : 'var(--aurora)';
  const gradientId = tone === 'drawdown' ? 'quant-drawdown-grad' : 'quant-equity-grad';
  const line = toPath(points, width, height);
  const area = areaPath(points, width, height);
  const latest = points.length > 0 ? points[points.length - 1].y : null;

  return (
    <Card variant="bordered" className="h-full" padding="md">
      <div className="mb-4 flex items-start justify-between gap-4">
        <div>
          <p className="text-sm font-semibold text-content-primary">{title}</p>
          <p className="text-xs text-content-tertiary mt-1">{subtitle}</p>
        </div>
        <div className="text-right">
          <span className="text-[10px] uppercase tracking-wider text-content-quaternary">最新值</span>
          <p className={`text-lg font-semibold tabular-nums ${tone === 'drawdown' ? 'text-semantic-danger' : 'text-semantic-success'}`}>
            {latest == null ? '--' : latest.toFixed(2)}
          </p>
        </div>
      </div>
      <div className="relative h-[220px] rounded-xl border border-white/8 bg-black/20 p-2">
        {points.length === 0 ? (
          <div className="flex h-full items-center justify-center text-xs text-content-tertiary">
            暂无图表数据
          </div>
        ) : (
          <svg viewBox={`0 0 ${width} ${height}`} className="h-full w-full">
            <defs>
              <linearGradient id={gradientId} x1="0" x2="0" y1="0" y2="1">
                <stop offset="0%" stopColor={stroke} stopOpacity={0.42} />
                <stop offset="100%" stopColor={stroke} stopOpacity={0.03} />
              </linearGradient>
            </defs>
            <g opacity={0.2}>
              <line x1="0" y1={height * 0.25} x2={width} y2={height * 0.25} stroke="white" strokeDasharray="6 6" />
              <line x1="0" y1={height * 0.5} x2={width} y2={height * 0.5} stroke="white" strokeDasharray="6 6" />
              <line x1="0" y1={height * 0.75} x2={width} y2={height * 0.75} stroke="white" strokeDasharray="6 6" />
            </g>
            <path d={area} fill={`url(#${gradientId})`} />
            <path d={line} fill="none" stroke={stroke} strokeWidth="2.6" strokeLinecap="round" />
          </svg>
        )}
      </div>
    </Card>
  );
};
