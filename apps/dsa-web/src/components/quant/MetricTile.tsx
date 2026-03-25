import type React from 'react';
import { Card } from '../common';

interface MetricTileProps {
  label: string;
  value: string;
  hint?: string;
  tone?: 'default' | 'success' | 'danger' | 'warning';
}

function valueClass(tone: MetricTileProps['tone']): string {
  switch (tone) {
    case 'success':
      return 'text-semantic-success';
    case 'danger':
      return 'text-semantic-danger';
    case 'warning':
      return 'text-semantic-warning';
    default:
      return 'text-content-primary';
  }
}

export const MetricTile: React.FC<MetricTileProps> = ({ label, value, hint, tone = 'default' }) => (
  <Card variant="data" padding="md" className="min-h-[116px]">
    <div className="flex h-full flex-col justify-between">
      <span className="text-[11px] uppercase tracking-wider text-content-quaternary">{label}</span>
      <span className={`text-2xl font-semibold tabular-nums ${valueClass(tone)}`}>{value}</span>
      <span className="text-xs text-content-tertiary">{hint ?? ' '}</span>
    </div>
  </Card>
);

