import type React from 'react';
import { Activity, ShieldAlert, Target } from 'lucide-react';
import { Badge, Button, Card } from '../common';
import { getRegimeLabel } from './labels';

interface PageHeroProps {
  regime: string;
  marketScore?: number;
  maxExposurePct?: number;
  asOfDate: string;
  isRunning: boolean;
  onRunBacktest: () => void;
}

function regimeVariant(regime: string): 'success' | 'warning' | 'danger' | 'default' {
  if (regime === 'RiskOn') return 'success';
  if (regime === 'Neutral') return 'warning';
  if (regime === 'RiskOff') return 'danger';
  return 'default';
}

export const PageHero: React.FC<PageHeroProps> = ({
  regime,
  marketScore,
  maxExposurePct,
  asOfDate,
  isRunning,
  onRunBacktest,
}) => (
  <Card className="quant-hero-card" padding="lg">
    <div className="quant-hero-bg" />
    <div className="relative z-10 grid gap-5 lg:grid-cols-[1.35fr_0.65fr]">
      <div className="space-y-3">
        <span className="label-uppercase text-brand-secondary">概念趋势工作台</span>
        <h1 className="text-2xl md:text-3xl font-semibold text-content-primary tracking-tight">
          概念板块趋势系统
        </h1>
        <p className="text-sm text-content-secondary leading-relaxed max-w-2xl">
          这里集中回答三件事：当前市场状态、历史策略表现、以及下一交易日的可执行交易清单。
        </p>
        <div className="flex flex-wrap items-center gap-2 pt-1">
          <Badge variant={regimeVariant(regime)} glow>{getRegimeLabel(regime)}</Badge>
          <Badge variant="default">信号日期 {asOfDate}</Badge>
          <Badge variant="aurora">仓位上限 {maxExposurePct ?? '--'}%</Badge>
        </div>
      </div>

      <div className="grid grid-cols-3 gap-3">
        <div className="quant-hero-stat">
          <Activity className="w-4 h-4 text-brand-primary" />
          <span className="text-[10px] text-content-quaternary uppercase tracking-wider">市场分数</span>
          <span className="text-xl font-semibold text-content-primary">{marketScore?.toFixed(2) ?? '--'}</span>
        </div>
        <div className="quant-hero-stat">
          <ShieldAlert className="w-4 h-4 text-semantic-warning" />
          <span className="text-[10px] text-content-quaternary uppercase tracking-wider">风险预算</span>
          <span className="text-xl font-semibold text-content-primary">{maxExposurePct ?? '--'}%</span>
        </div>
        <div className="quant-hero-stat">
          <Target className="w-4 h-4 text-semantic-success" />
          <span className="text-[10px] text-content-quaternary uppercase tracking-wider">执行模式</span>
          <span className="text-sm font-semibold text-content-primary">收盘出信号 / 次日开盘执行</span>
        </div>
      </div>
    </div>
    <div className="relative z-10 mt-5 flex flex-wrap items-center gap-3">
      <Button variant="aurora" onClick={onRunBacktest} isLoading={isRunning}>
        运行结构化回测
      </Button>
      <span className="text-xs text-content-tertiary">
        主板 + 概念板块主判定 + 四模块入场
      </span>
    </div>
  </Card>
);
