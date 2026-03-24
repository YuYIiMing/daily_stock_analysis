export type Stage = 'initial' | 'middle' | 'late' | 'choppy';
export type SignalType = 'breakout' | 'pullback' | 'late_reclaim' | 'compensation';
export type RiskMode =
  | 'normal'
  | 'reduced_risk'
  | 'elite_disabled'
  | 'breakout_paused'
  | 'cooldown'
  | 'degraded_system';
export type SectorView = 'concept' | 'industry';
export type PositionAction = 'hold' | 'reduce' | 'exit';

export interface PositionRule {
  key: string;
  label: string;
  matched: boolean;
  value: unknown;
}

export interface PositionResponse {
  recommendedPositionPct: number;
  matchedRules: number;
  rules: PositionRule[];
  indexCode: string;
}

export interface StageOverrideInfo {
  id: number;
  overrideDate: string;
  sectorView: SectorView;
  sectorKey: string;
  sectorName?: string | null;
  originalStage: Stage;
  targetStage: Stage;
  reason: string;
  operator: string;
  createdAt?: string | null;
}

export interface SectorMemberBrief {
  code: string;
  name: string;
  pctChg: number;
  amountB?: number | null;
  gain20d?: number | null;
}

export interface SectorBreadth {
  strongMemberCount: number;
  limitUpCount: number;
  top5AvgPct?: number | null;
  consistencyScore: number;
}

export interface SectorDecisionItem {
  sectorKey: string;
  sectorName: string;
  sectorView: SectorView;
  memberCount: number;
  latestAmountB: number;
  topAmountRank: number;
  matchedConditions: number;
  conditions: Record<string, boolean>;
  tradeAllowed: boolean;
  quantStage: Stage;
  finalStage: Stage;
  stageMeta: Record<string, unknown>;
  override?: StageOverrideInfo | null;
  leader?: SectorMemberBrief | null;
  leader2?: SectorMemberBrief | null;
  frontlineMembers: SectorMemberBrief[];
  sectorBreadth: SectorBreadth;
  members: Array<Record<string, unknown>>;
}

export interface RiskStateResponse {
  currentMode: RiskMode;
  flags: Record<string, boolean>;
  consecutiveStopLosses: number;
  consecutiveNonStopLosses: number;
  recentBreakoutFailures: number;
  cooldownUntil?: string | null;
  newPositionLimitPct?: number | null;
  reasons: string[];
}

export interface OverviewSector {
  sectorKey: string;
  sectorName: string;
  sectorView: SectorView;
  finalStage: Stage;
  tradeAllowed: boolean;
}

export interface TrendSystemOverviewResponse {
  asOf: string;
  generatedAt?: string | null;
  position: PositionResponse;
  tradeAllowed: boolean;
  tradeGate: 'allowed' | 'blocked';
  primaryStage: Stage;
  mainSectors: OverviewSector[];
  riskState: RiskStateResponse;
  candidateCount: number;
  snapshotStatus: string;
  emptyReason?: string | null;
}

export interface CandidateItem {
  code: string;
  name: string;
  sectorKey: string;
  sectorName: string;
  sectorView: SectorView;
  finalStage: Stage;
  signalType: SignalType;
  signalLabel: string;
  signalScore: number;
  actionable: boolean;
  actionBlockReason?: string | null;
  suggestedEntry?: number | null;
  invalidIf?: string | null;
  stopLoss?: number | null;
  positionLimitPct: number;
  recommendedPositionPct: number;
  reasonChecks: Record<string, boolean>;
  isEliteCandidate: boolean;
  latestClose?: number | null;
  gain20d?: number | null;
  gain60d?: number | null;
  filterReasons: string[];
}

export interface TrendDiagnosticsResponse {
  mode: string;
  totalMarketSymbols: number;
  dbBackedSymbols: number;
  scanLimit: number;
  scannedSymbols: number;
  etfExcluded: number;
  indexExcluded: number;
  missingHistory: number;
  sectorResolutionFailures: number;
  sectorResolved: number;
  floatCapResolved: number;
  coverageRatio?: number | null;
  alertCount: number;
  openPositionCount: number;
  openPositionPct: number;
  sourceNotes: string[];
  candidateFilters: Record<string, unknown>;
}

export interface PositionSignals {
  strongStopLoss: boolean;
  weakStopLoss: boolean;
  takeProfit10: boolean;
  takeProfit20: boolean;
  trendExitMa10: boolean;
  trendExitMa20: boolean;
  emotionExit: boolean;
}

export interface PortfolioPositionItem {
  id: number;
  code: string;
  name?: string | null;
  sectorKey?: string | null;
  sectorName?: string | null;
  openType: string;
  positionPct: number;
  entryPrice: number;
  latestClose?: number | null;
  profitPct?: number | null;
  initialStopLoss?: number | null;
  currentStopLoss?: number | null;
  trendExitLine?: number | null;
  takeProfitStage: number;
  action: PositionAction;
  actionReason: string;
  suggestedSellPct: number;
  signals: PositionSignals;
  status: string;
}

export interface PortfolioResponse {
  summary: {
    openCount: number;
    totalPositionPct: number;
    reduceCount: number;
    exitCount: number;
  };
  items: PortfolioPositionItem[];
}

export interface TrendPlanResponse {
  generatedAt: string;
  recommendedPositionPct: number;
  tradeAllowed: boolean;
  mainSectors: OverviewSector[];
  primaryStage: Stage;
  riskState: RiskStateResponse;
  candidates: CandidateItem[];
  portfolioActions: PortfolioPositionItem[];
  blockedRules: string[];
  disciplineNotes: string[];
  emptyReason?: string | null;
  diagnosticsSummary: Record<string, unknown>;
}

export interface TradeRecord {
  id: number;
  code: string;
  name?: string | null;
  sectorView: string;
  sectorKey?: string | null;
  sectorName?: string | null;
  openDate: string;
  openType: string;
  entryPrice: number;
  initialStopLoss?: number | null;
  positionPct: number;
  isEliteStrategy: boolean;
  closeDate?: string | null;
  exitPrice?: number | null;
  exitReason?: string | null;
  isStopLoss?: boolean | null;
  breakoutFailed?: boolean | null;
  createdAt?: string | null;
  updatedAt?: string | null;
}

export interface PositionRecord {
  id: number;
  code: string;
  name?: string | null;
  sectorView: string;
  sectorKey?: string | null;
  sectorName?: string | null;
  openDate: string;
  openType: string;
  entryPrice: number;
  initialStopLoss?: number | null;
  currentStopLoss?: number | null;
  trendExitLine?: number | null;
  positionPct: number;
  shares?: number | null;
  isEliteStrategy: boolean;
  takeProfitStage: number;
  status: string;
  linkedTradeId?: number | null;
  closeDate?: string | null;
  exitPrice?: number | null;
  exitReason?: string | null;
  notes?: string | null;
  createdAt?: string | null;
  updatedAt?: string | null;
}

export interface TrendAlert {
  id: number;
  alertDate: string;
  alertType: string;
  code?: string | null;
  name?: string | null;
  sectorKey?: string | null;
  message: string;
  payload: Record<string, unknown>;
  acked: boolean;
  ackedAt?: string | null;
  createdAt?: string | null;
}

export interface TrendStatusResponse {
  asOf: string;
  dailySnapshot: {
    status: string;
    snapshotDate?: string | null;
    generatedAt?: string | null;
    source?: string | null;
  };
  preopenSnapshot: {
    status: string;
    snapshotDate?: string | null;
    generatedAt?: string | null;
    source?: string | null;
  };
  recomputeState: {
    running: boolean;
    startedAt?: string | null;
    finishedAt?: string | null;
    lastError?: string | null;
  };
}
