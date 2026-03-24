import apiClient from './index';
import { toCamelCase } from './utils';
import type {
  CandidateItem,
  PortfolioResponse,
  PositionRecord,
  RiskStateResponse,
  SectorDecisionItem,
  Stage,
  StageOverrideInfo,
  TradeRecord,
  TrendAlert,
  TrendDiagnosticsResponse,
  TrendPlanResponse,
  TrendStatusResponse,
  TrendSystemOverviewResponse,
  SectorView,
} from '../types/trendSystem';

export const trendSystemApi = {
  getOverview: async (): Promise<TrendSystemOverviewResponse> => {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/trend-system/overview');
    return toCamelCase<TrendSystemOverviewResponse>(response.data);
  },

  getPosition: async () => {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/trend-system/position');
    return toCamelCase(response.data);
  },

  getStatus: async (): Promise<TrendStatusResponse> => {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/trend-system/status');
    return toCamelCase<TrendStatusResponse>(response.data);
  },

  recompute: async (snapshotType: 'daily_close' | 'preopen' | 'manual_recompute' = 'manual_recompute') => {
    const response = await apiClient.post<Record<string, unknown>>('/api/v1/trend-system/recompute', {
      snapshot_type: snapshotType,
      background: true,
    });
    return toCamelCase(response.data);
  },

  getSectors: async (view: SectorView): Promise<SectorDecisionItem[]> => {
    const response = await apiClient.get<Record<string, unknown>[]>(
      '/api/v1/trend-system/sectors',
      { params: { view } },
    );
    return (response.data || []).map(item => toCamelCase<SectorDecisionItem>(item));
  },

  getCandidates: async (): Promise<CandidateItem[]> => {
    const response = await apiClient.get<Record<string, unknown>[]>('/api/v1/trend-system/candidates');
    return (response.data || []).map(item => toCamelCase<CandidateItem>(item));
  },

  getPortfolio: async (): Promise<PortfolioResponse> => {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/trend-system/portfolio');
    return toCamelCase<PortfolioResponse>(response.data);
  },

  getRiskState: async (): Promise<RiskStateResponse> => {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/trend-system/risk-state');
    return toCamelCase<RiskStateResponse>(response.data);
  },

  getPlan: async (): Promise<TrendPlanResponse> => {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/trend-system/plan');
    return toCamelCase<TrendPlanResponse>(response.data);
  },

  getDiagnostics: async (): Promise<TrendDiagnosticsResponse> => {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/trend-system/diagnostics');
    return toCamelCase<TrendDiagnosticsResponse>(response.data);
  },

  listTrades: async (): Promise<TradeRecord[]> => {
    const response = await apiClient.get<{ items?: Record<string, unknown>[] }>('/api/v1/trend-system/trades');
    return (response.data.items || []).map(item => toCamelCase<TradeRecord>(item));
  },

  createTrade: async (payload: {
    code: string;
    name?: string;
    sectorView?: SectorView;
    sectorKey?: string;
    sectorName?: string;
    openDate: string;
    openType: string;
    entryPrice: number;
    initialStopLoss?: number;
    positionPct: number;
    isEliteStrategy?: boolean;
  }): Promise<TradeRecord> => {
    const response = await apiClient.post<Record<string, unknown>>('/api/v1/trend-system/trades', {
      code: payload.code,
      name: payload.name,
      sector_view: payload.sectorView ?? 'concept',
      sector_key: payload.sectorKey,
      sector_name: payload.sectorName,
      open_date: payload.openDate,
      open_type: payload.openType,
      entry_price: payload.entryPrice,
      initial_stop_loss: payload.initialStopLoss,
      position_pct: payload.positionPct,
      is_elite_strategy: payload.isEliteStrategy ?? false,
    });
    return toCamelCase<TradeRecord>(response.data);
  },

  updateTrade: async (
    tradeId: number,
    payload: {
      closeDate?: string;
      exitPrice?: number;
      exitReason?: string;
      isStopLoss?: boolean;
      breakoutFailed?: boolean;
    },
  ): Promise<TradeRecord> => {
    const response = await apiClient.patch<Record<string, unknown>>(`/api/v1/trend-system/trades/${tradeId}`, {
      close_date: payload.closeDate,
      exit_price: payload.exitPrice,
      exit_reason: payload.exitReason,
      is_stop_loss: payload.isStopLoss,
      breakout_failed: payload.breakoutFailed,
    });
    return toCamelCase<TradeRecord>(response.data);
  },

  listPositions: async (status?: string): Promise<PositionRecord[]> => {
    const response = await apiClient.get<{ items?: Record<string, unknown>[] }>('/api/v1/trend-system/positions', {
      params: status ? { status } : undefined,
    });
    return (response.data.items || []).map(item => toCamelCase<PositionRecord>(item));
  },

  createPosition: async (payload: {
    code: string;
    name?: string;
    sectorView?: SectorView;
    sectorKey?: string;
    sectorName?: string;
    openDate: string;
    openType: string;
    entryPrice: number;
    initialStopLoss?: number;
    currentStopLoss?: number;
    trendExitLine?: number;
    positionPct: number;
    shares?: number;
    isEliteStrategy?: boolean;
    takeProfitStage?: number;
    notes?: string;
  }): Promise<PositionRecord> => {
    const response = await apiClient.post<Record<string, unknown>>('/api/v1/trend-system/positions', {
      code: payload.code,
      name: payload.name,
      sector_view: payload.sectorView ?? 'concept',
      sector_key: payload.sectorKey,
      sector_name: payload.sectorName,
      open_date: payload.openDate,
      open_type: payload.openType,
      entry_price: payload.entryPrice,
      initial_stop_loss: payload.initialStopLoss,
      current_stop_loss: payload.currentStopLoss,
      trend_exit_line: payload.trendExitLine,
      position_pct: payload.positionPct,
      shares: payload.shares,
      is_elite_strategy: payload.isEliteStrategy ?? false,
      take_profit_stage: payload.takeProfitStage ?? 0,
      notes: payload.notes,
    });
    return toCamelCase<PositionRecord>(response.data);
  },

  updatePosition: async (
    positionId: number,
    payload: {
      currentStopLoss?: number;
      trendExitLine?: number;
      takeProfitStage?: number;
      status?: string;
      closeDate?: string;
      exitPrice?: number;
      exitReason?: string;
      notes?: string;
    },
  ): Promise<PositionRecord> => {
    const response = await apiClient.patch<Record<string, unknown>>(`/api/v1/trend-system/positions/${positionId}`, {
      current_stop_loss: payload.currentStopLoss,
      trend_exit_line: payload.trendExitLine,
      take_profit_stage: payload.takeProfitStage,
      status: payload.status,
      close_date: payload.closeDate,
      exit_price: payload.exitPrice,
      exit_reason: payload.exitReason,
      notes: payload.notes,
    });
    return toCamelCase<PositionRecord>(response.data);
  },

  listAlerts: async (): Promise<TrendAlert[]> => {
    const response = await apiClient.get<{ items?: Record<string, unknown>[] }>('/api/v1/trend-system/alerts');
    return (response.data.items || []).map(item => toCamelCase<TrendAlert>(item));
  },

  ackAlert: async (alertId: number): Promise<TrendAlert> => {
    const response = await apiClient.post<Record<string, unknown>>(`/api/v1/trend-system/alerts/${alertId}/ack`);
    return toCamelCase<TrendAlert>(response.data);
  },

  createStageOverride: async (payload: {
    sectorView: SectorView;
    sectorKey: string;
    sectorName: string;
    originalStage: Stage;
    targetStage: Stage;
    reason: string;
  }): Promise<StageOverrideInfo> => {
    const response = await apiClient.post<Record<string, unknown>>('/api/v1/trend-system/stage-override', {
      sector_view: payload.sectorView,
      sector_key: payload.sectorKey,
      sector_name: payload.sectorName,
      original_stage: payload.originalStage,
      target_stage: payload.targetStage,
      reason: payload.reason,
    });
    return toCamelCase<StageOverrideInfo>(response.data);
  },
};
