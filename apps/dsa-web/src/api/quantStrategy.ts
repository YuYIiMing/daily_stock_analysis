import apiClient from './index';
import { toCamelCase } from './utils';
import type {
  QuantBacktestDetailResponse,
  QuantBacktestRunRequest,
  QuantBacktestRunResponse,
  QuantEquityPoint,
  QuantSyncRequest,
  QuantSyncResponse,
  QuantSyncStatusResponse,
  QuantTradeItem,
  QuantTradePlanResponse,
} from '../types/quantStrategy';

export const quantStrategyApi = {
  runSync: async (payload: QuantSyncRequest): Promise<QuantSyncResponse> => {
    const response = await apiClient.post<Record<string, unknown>>('/api/v1/quant-strategy/sync', {
      history_days: payload.historyDays ?? 130,
      include_ranked_boards: payload.includeRankedBoards ?? true,
      as_of_date: payload.asOfDate ?? undefined,
      latest_feature_only: payload.latestFeatureOnly ?? false,
    }, {
      timeout: 0,
    });
    return toCamelCase<QuantSyncResponse>(response.data);
  },

  runBacktest: async (payload: QuantBacktestRunRequest): Promise<QuantBacktestRunResponse> => {
    const response = await apiClient.post<Record<string, unknown>>('/api/v1/quant-strategy/backtests/run', {
      start_date: payload.startDate,
      end_date: payload.endDate,
      initial_capital: payload.initialCapital,
      strategy_name: payload.strategyName,
    });
    return toCamelCase<QuantBacktestRunResponse>(response.data);
  },

  getBacktestDetail: async (runId: number): Promise<QuantBacktestDetailResponse> => {
    const response = await apiClient.get<Record<string, unknown>>(`/api/v1/quant-strategy/backtests/${runId}`);
    return toCamelCase<QuantBacktestDetailResponse>(response.data);
  },

  getLatestBacktestDetail: async (): Promise<QuantBacktestDetailResponse> => {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/quant-strategy/backtests/latest');
    return toCamelCase<QuantBacktestDetailResponse>(response.data);
  },

  getTrades: async (runId: number): Promise<QuantTradeItem[]> => {
    const response = await apiClient.get<Record<string, unknown>>(`/api/v1/quant-strategy/backtests/${runId}/trades`);
    const data = toCamelCase<{ items?: QuantTradeItem[] }>(response.data);
    return data.items || [];
  },

  getEquityCurve: async (runId: number): Promise<QuantEquityPoint[]> => {
    const response = await apiClient.get<Record<string, unknown>>(`/api/v1/quant-strategy/backtests/${runId}/equity`);
    const data = toCamelCase<{ items?: QuantEquityPoint[] }>(response.data);
    return data.items || [];
  },

  getTradePlan: async (asOfDate?: string): Promise<QuantTradePlanResponse> => {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/quant-strategy/trade-plan', {
      params: asOfDate ? { as_of_date: asOfDate } : undefined,
    });
    return toCamelCase<QuantTradePlanResponse>(response.data);
  },

  getSyncStatus: async (): Promise<QuantSyncStatusResponse> => {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/quant-strategy/sync-status');
    return toCamelCase<QuantSyncStatusResponse>(response.data);
  },
};
