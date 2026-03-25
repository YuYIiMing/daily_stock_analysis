/**
 * Quant strategy API type definitions
 */

export interface QuantBacktestRunRequest {
  startDate: string;
  endDate: string;
  initialCapital?: number;
  strategyName?: string;
}

export interface QuantBacktestSummary {
  totalReturnPct?: number;
  finalEquity?: number;
  maxDrawdownPct?: number;
  tradeCount?: number;
  winCount?: number;
  lossCount?: number;
  winRatePct?: number;
}

export interface QuantBacktestRunResponse {
  runId: number;
  status: string;
  summary: QuantBacktestSummary;
  tradePlanDays?: number;
  tradeCount?: number;
}

export interface QuantBacktestDetailResponse {
  runId: number;
  strategyName: string;
  marketScope: string;
  boardSource: string;
  startDate: string;
  endDate: string;
  initialCapital: number;
  status: string;
  summary: QuantBacktestSummary;
}

export interface QuantTradeItem {
  id?: number;
  code: string;
  stockName?: string;
  boardCode?: string;
  boardName?: string;
  entryDate: string;
  exitDate?: string;
  entryPrice?: number;
  exitPrice?: number;
  entryAmount?: number;
  exitAmount?: number;
  shares?: number;
  entryModule: string;
  stage: string;
  status?: string;
  pnlPct?: number;
  pnlAmount?: number;
  exitReason?: string;
  blockedExit?: boolean;
}

export interface QuantEquityPoint {
  tradeDate: string;
  cash: number;
  marketValue: number;
  equity: number;
  drawdownPct: number;
  exposurePct: number;
}

export interface QuantTradePlanItem {
  code: string;
  boardCode?: string;
  boardName?: string;
  stage: string;
  entryModule: string;
  signalScore: number;
  plannedEntryPrice?: number;
  initialStopPrice?: number;
  plannedPositionPct?: number;
  blockedReason?: string | null;
  reason?: Record<string, unknown>;
}

export interface QuantTradePlanDiagnosticBoard {
  boardName: string;
  stockCount: number;
  stage?: string | null;
  themeScore?: number | null;
  featureTradeDate?: string | null;
}

export interface QuantTradePlanDiagnostics {
  eligibleStockCount?: number;
  sameDayBoardMatchCount?: number;
  recentBoardFallbackCount?: number;
  missingBoardFeatureCount?: number;
  tradeAllowedStockCount?: number;
  stageReadyStockCount?: number;
  candidateStockCount?: number;
  mappedStageDistribution?: Record<string, number>;
  stageReadyDistribution?: Record<string, number>;
  setupBlockerCounts?: Record<string, number>;
  topMissingBoards?: QuantTradePlanDiagnosticBoard[];
  tradeAllowedBoards?: QuantTradePlanDiagnosticBoard[];
  primaryBlocker?: string | null;
  summary?: string | null;
}

export interface QuantTradePlanResponse {
  asOfDate: string;
  regime: string;
  marketScore?: number;
  maxExposurePct?: number;
  message?: string | null;
  items: QuantTradePlanItem[];
  diagnostics?: QuantTradePlanDiagnostics;
}

export interface QuantSyncRequest {
  historyDays?: number;
  includeRankedBoards?: boolean;
  asOfDate?: string | null;
  latestFeatureOnly?: boolean;
}

export interface QuantSyncResponse {
  status: string;
  message: string;
  summary?: Record<string, unknown>;
}

export interface QuantSyncStatusSummary {
  membershipDistinctCodes?: number | null;
  latestMembershipDate?: string | null;
  latestMembershipCount?: number | null;
  conceptBoardCoverageCount?: number | null;
  stockDailyDistinctCodes?: number | null;
  latestBoardDate?: string | null;
  latestBoardCount?: number | null;
  stockPoolSize?: number | null;
  latestStockFeatureDate?: string | null;
  latestStockFeatureCount?: number | null;
  latestIndexFeatureDate?: string | null;
}

export interface QuantSyncStatusResponse extends QuantSyncStatusSummary {
  status?: string;
  message?: string | null;
  summary?: QuantSyncStatusSummary;
}
