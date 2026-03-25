import type React from 'react';
import { Badge, Button, Card } from '../common';
import { getModuleLabel, getStageLabel } from './labels';

export interface QuantFilterState {
  asOfDate: string;
  module: string;
  stage: string;
  code: string;
}

interface FilterRailProps {
  filters: QuantFilterState;
  onChange: (next: QuantFilterState) => void;
  onApply: () => void;
  compact?: boolean;
}

export const FilterRail: React.FC<FilterRailProps> = ({ filters, onChange, onApply, compact = false }) => (
  <Card variant="bordered" padding="md" className={compact ? '' : 'sticky top-5'}>
    <div className="mb-4 flex items-center justify-between gap-3">
      <div>
        <p className="text-sm font-semibold text-content-primary">策略过滤器</p>
        <p className="text-xs text-content-tertiary mt-1">主板 + 概念板块 + 三模块</p>
      </div>
      <Badge variant="default">V1</Badge>
    </div>

    <div className="space-y-3">
      <label className="block">
        <span className="text-[11px] uppercase tracking-wider text-content-quaternary">信号日期</span>
        <input
          type="date"
          value={filters.asOfDate}
          onChange={(event) => onChange({ ...filters, asOfDate: event.target.value })}
          className="input-modern mt-1.5 w-full"
        />
      </label>

      <label className="block">
        <span className="text-[11px] uppercase tracking-wider text-content-quaternary">入场模块</span>
        <select
          value={filters.module}
          onChange={(event) => onChange({ ...filters, module: event.target.value })}
          className="input-modern mt-1.5 w-full"
        >
          <option value="ALL">{getModuleLabel('ALL')}</option>
          <option value="BREAKOUT">{getModuleLabel('BREAKOUT')}</option>
          <option value="PULLBACK">{getModuleLabel('PULLBACK')}</option>
          <option value="LATE_WEAK_TO_STRONG">{getModuleLabel('LATE_WEAK_TO_STRONG')}</option>
        </select>
      </label>

      <label className="block">
        <span className="text-[11px] uppercase tracking-wider text-content-quaternary">板块阶段</span>
        <select
          value={filters.stage}
          onChange={(event) => onChange({ ...filters, stage: event.target.value })}
          className="input-modern mt-1.5 w-full"
        >
          <option value="ALL">{getStageLabel('ALL')}</option>
          <option value="EMERGING">{getStageLabel('EMERGING')}</option>
          <option value="TREND">{getStageLabel('TREND')}</option>
          <option value="CLIMAX">{getStageLabel('CLIMAX')}</option>
          <option value="IGNORE">{getStageLabel('IGNORE')}</option>
        </select>
      </label>

      <label className="block">
        <span className="text-[11px] uppercase tracking-wider text-content-quaternary">股票代码</span>
        <input
          type="text"
          value={filters.code}
          onChange={(event) => onChange({ ...filters, code: event.target.value.toUpperCase() })}
          placeholder="例如 600519"
          className="input-modern mt-1.5 w-full"
        />
      </label>
    </div>

    <div className="mt-4 pt-4 border-t border-white/8">
      <Button variant="nebula" fullWidth onClick={onApply}>
        应用过滤条件
      </Button>
    </div>
  </Card>
);
