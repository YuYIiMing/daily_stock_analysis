import { 
  LayoutGrid, 
  TrendingUp, 
  Target, 
  RotateCcw, 
  Move, 
  Flame, 
  GitBranch,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';

export interface StrategyCategory {
  key: string;
  label: string;
  color: string;
}

export interface StrategyItem {
  id: string;
  name: string;
  category: string;
  icon: LucideIcon;
  coreLogic: string;
  applicableScenarios: string[];
}

export const STRATEGY_CATEGORIES: StrategyCategory[] = [
  { key: 'comprehensive', label: '综合分析', color: '#FFFFFF' },
  { key: 'trend', label: '趋势交易', color: '#00E676' },
  { key: 'reversal', label: '反转交易', color: '#FF3D00' },
  { key: 'sentiment', label: '情绪/题材', color: '#E91E63' },
  { key: 'structure', label: '结构分析', color: '#8A2BE2' },
];

export const GENERAL_STRATEGY: StrategyItem = {
  id: '',
  name: '通用分析',
  category: 'comprehensive',
  icon: LayoutGrid,
  coreLogic: '综合技术面、资金面、基本面和市场情绪，对个股进行整体趋势与风险评估',
  applicableScenarios: ['日常选股', '复盘分析', '交易决策'],
};

export const STRATEGIES: StrategyItem[] = [
  // 趋势交易（包含原突破交易）
  {
    id: 'bull_trend',
    name: '默认多头趋势',
    category: 'trend',
    icon: TrendingUp,
    coreLogic: '识别多头排列、趋势延续与回踩低吸机会',
    applicableScenarios: ['主升浪行情', '牛市策略', '趋势追踪'],
  },
  {
    id: 'ma_golden_cross',
    name: '均线金叉',
    category: 'trend',
    icon: TrendingUp,
    coreLogic: '检测均线金叉配合量能确认信号，经典的趋势反转/延续信号',
    applicableScenarios: ['趋势启动', '趋势确认', '中期策略'],
  },
  {
    id: 'shrink_pullback',
    name: '缩量回踩',
    category: 'trend',
    icon: TrendingUp,
    coreLogic: '检测缩量回踩均线支撑信号，趋势延续的理想入场点',
    applicableScenarios: ['强势回调', '二次买点', '趋势延续'],
  },
  {
    id: 'volume_breakout',
    name: '放量突破',
    category: 'trend',
    icon: Target,
    coreLogic: '检测放量突破阻力位信号，有新增资金推动趋势形成',
    applicableScenarios: ['平台整理末期', '趋势启动点', '突破确认'],
  },
  // 反转交易（包含原区间交易）
  {
    id: 'bottom_volume',
    name: '底部放量',
    category: 'reversal',
    icon: RotateCcw,
    coreLogic: '检测长期下跌后底部放量信号，潜在趋势反转信号',
    applicableScenarios: ['止跌区域', '底部反转', '反弹介入'],
  },
  {
    id: 'one_yang_three_yin',
    name: '一阳夹三阴',
    category: 'reversal',
    icon: RotateCcw,
    coreLogic: '检测一阳夹三阴K线整理形态，趋势延续入场信号',
    applicableScenarios: ['下跌末期', '回调结束', '形态确认'],
  },
  {
    id: 'box_oscillation',
    name: '箱体震荡',
    category: 'reversal',
    icon: Move,
    coreLogic: '识别价格箱体区间，在箱底买入、箱顶减仓',
    applicableScenarios: ['震荡市场', '波段操作', '区间交易'],
  },
  // 情绪/题材交易
  {
    id: 'dragon_head',
    name: '龙头策略',
    category: 'sentiment',
    icon: Flame,
    coreLogic: '在热点题材中寻找涨幅、资金与市场关注度最强的龙头股进行跟随',
    applicableScenarios: ['短线题材', '游资主导', '板块轮动'],
  },
  {
    id: 'emotion_cycle',
    name: '情绪周期',
    category: 'sentiment',
    icon: Flame,
    coreLogic: '根据涨停数量、连板高度、炸板率等指标判断市场情绪阶段',
    applicableScenarios: ['短线交易', '打板接力', '情绪判断'],
  },
  // 结构分析
  {
    id: 'chan_theory',
    name: '缠论',
    category: 'structure',
    icon: GitBranch,
    coreLogic: '通过笔、线段和中枢结构判断趋势与买卖点',
    applicableScenarios: ['趋势分析', '买点确认', '结构清晰'],
  },
  {
    id: 'wave_theory',
    name: '波浪理论',
    category: 'structure',
    icon: GitBranch,
    coreLogic: '基于艾略特波浪理论的推动浪与调整浪结构，判断当前浪型',
    applicableScenarios: ['中长期趋势', '主升浪判断', '波浪计数'],
  },
];

export const STRATEGY_STORAGE_KEY = 'dsa_chat_selected_strategy';

export function getStoredStrategy(): string | null {
  try {
    return localStorage.getItem(STRATEGY_STORAGE_KEY);
  } catch {
    return null;
  }
}

export function setStoredStrategy(strategyId: string): void {
  try {
    localStorage.setItem(STRATEGY_STORAGE_KEY, strategyId);
  } catch {
    // Ignore storage errors
  }
}

export function getStrategiesByCategory(category: string): StrategyItem[] {
  return STRATEGIES.filter((s) => s.category === category);
}

export function getStrategyById(id: string): StrategyItem | undefined {
  if (id === '') return GENERAL_STRATEGY;
  return STRATEGIES.find((s) => s.id === id);
}

export function getCategoryByKey(key: string): StrategyCategory | undefined {
  return STRATEGY_CATEGORIES.find((c) => c.key === key);
}