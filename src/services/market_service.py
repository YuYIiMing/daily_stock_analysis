# -*- coding: utf-8 -*-
"""
Market Service - Public market/sector data service.

Provides unified market context data for both Pipeline and Agent modes.
This module centralizes:
1. Market context data (indices, breadth, regime detection)
2. Sector strength analysis
3. Bear market constraints
4. SYSTEM_PROMPT sections shared between modes

Usage:
    from src.services.market_service import MarketService, apply_bear_constraints
    
    # Get market context
    service = MarketService()
    context = service.get_market_context()
    
    # Apply bear market constraints
    result = apply_bear_constraints(analysis_result, context)
"""

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class MarketContext:
    """Market context data structure."""
    indices: List[Dict[str, Any]]
    market_breadth: Dict[str, Any]
    top_sectors: List[Dict[str, Any]]
    bottom_sectors: List[Dict[str, Any]]
    regime: str  # 'bull' | 'bear' | 'range'
    strength_score: int  # 0-100
    sh_index: Optional[Dict[str, Any]] = None


@dataclass
class SectorStrength:
    """Sector strength data structure."""
    name: str
    strength_score: int
    change_5d: Optional[float] = None
    is_leader: bool = False
    is_laggard: bool = False


# ============================================================
# Market Service
# ============================================================

class MarketService:
    """
    Unified service for market context and sector data.
    
    Used by both Pipeline and Agent modes.
    Implements singleton pattern with cache TTL.
    """
    
    _instance = None
    _cache: Optional[Dict[str, Any]] = None
    _cache_time: float = 0
    _cache_ttl: int = 3600  # 1 hour default for market context
    
    # Sector data cache (24h TTL)
    _sector_cache: Dict[str, Any] = {}
    _sector_cache_time: Dict[str, float] = {}
    _SECTOR_CACHE_TTL: int = 86400  # 24 hours
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_fetcher_manager'):
            self._fetcher_manager = None
    
    @property
    def fetcher_manager(self):
        """Lazy initialization of DataFetcherManager."""
        if self._fetcher_manager is None:
            from data_provider import DataFetcherManager
            self._fetcher_manager = DataFetcherManager()
        return self._fetcher_manager
    
    def get_market_context(self, region: str = "cn", use_cache: bool = True) -> Dict[str, Any]:
        """
        Get market context data (indices, breadth, sector rankings).
        
        Uses singleton cache with TTL to avoid repeated API calls.
        
        Args:
            region: Market region ("cn" for A-shares)
            use_cache: Whether to use cached data
        
        Returns:
            {
                'indices': [...],
                'market_breadth': {...},
                'top_sectors': [...],
                'bottom_sectors': [...],
                'regime': 'bull/bear/range',
                'strength_score': 0-100,
                'sh_index': {...},
            }
        """
        current_time = time.time()
        
        # Check cache
        if use_cache and self._cache is not None:
            if current_time - self._cache_time < self._cache_ttl:
                logger.debug("[Market] Using cached market context")
                return self._cache
        
        result: Dict[str, Any] = {
            'indices': [],
            'market_breadth': {},
            'top_sectors': [],
            'bottom_sectors': [],
            'regime': 'range',
            'strength_score': 50,
        }
        
        # Get main indices
        try:
            indices = self.fetcher_manager.get_main_indices(region=region)
            if indices:
                result['indices'] = indices
                logger.debug(f"[Market] Got {len(indices)} indices")
        except Exception as e:
            logger.warning(f"[Market] Failed to get indices: {e}")
        
        # Get market breadth
        try:
            stats = self.fetcher_manager.get_market_stats()
            if stats:
                result['market_breadth'] = stats
                logger.debug(f"[Market] Got breadth stats: up={stats.get('up_count')}")
        except Exception as e:
            logger.warning(f"[Market] Failed to get market stats: {e}")
        
        # Get sector rankings
        try:
            top_bottom = self.fetcher_manager.get_sector_rankings(n=5)
            if top_bottom:
                top_sectors, bottom_sectors = top_bottom
                result['top_sectors'] = top_sectors or []
                result['bottom_sectors'] = bottom_sectors or []
        except Exception as e:
            logger.warning(f"[Market] Failed to get sector rankings: {e}")
        
        # Calculate regime and strength
        result['regime'] = self.infer_regime(result)
        result['strength_score'] = self.calc_strength(result)
        
        # Extract Shanghai index
        for idx in result.get('indices', []):
            if idx.get('code') == 'sh000001':
                result['sh_index'] = {
                    'current': idx.get('current'),
                    'change_pct': idx.get('change_pct'),
                }
                break
        
        # Update cache
        self._cache = result
        self._cache_time = current_time
        logger.info(f"[Market] Market context updated: regime={result['regime']}, strength={result['strength_score']}")
        
        return result
    
    def infer_regime(self, market_data: Dict[str, Any]) -> str:
        """
        Infer market regime: bull/bear/range.
        
        Rules:
        - bull: Shanghai Index change > +1% AND up_ratio > 60%
        - bear: Shanghai Index change < -1% AND down_ratio > 60%
        - range: otherwise
        """
        indices = market_data.get('indices', [])
        breadth = market_data.get('market_breadth', {})
        
        # Get Shanghai index change
        sh_change = 0.0
        for idx in indices:
            if idx.get('code') == 'sh000001':
                sh_change = idx.get('change_pct', 0) or 0
                break
        
        # Calculate up/down ratio
        up_count = breadth.get('up_count', 0) or 0
        down_count = breadth.get('down_count', 0) or 0
        total = up_count + down_count
        
        if total == 0:
            up_ratio = 0.5
        else:
            up_ratio = up_count / total
        
        # Determine regime
        if sh_change > 1.0 and up_ratio > 0.6:
            return 'bull'
        elif sh_change < -1.0 and up_ratio < 0.4:
            return 'bear'
        else:
            return 'range'
    
    def calc_strength(self, market_data: Dict[str, Any]) -> int:
        """
        Calculate market strength score (0-100).
        
        Factors:
        - Index change (Shanghai)
        - Up/down ratio
        - Limit up/down count
        """
        indices = market_data.get('indices', [])
        breadth = market_data.get('market_breadth', {})
        
        score = 50  # Base score
        
        # Index change contribution
        for idx in indices:
            if idx.get('code') == 'sh000001':
                change = idx.get('change_pct', 0) or 0
                score += change * 5  # +5 per 1% change
                break
        
        # Up/down ratio contribution
        up_count = breadth.get('up_count', 0) or 0
        down_count = breadth.get('down_count', 0) or 0
        limit_up = breadth.get('limit_up_count', 0) or 0
        limit_down = breadth.get('limit_down_count', 0) or 0
        
        if up_count + down_count > 0:
            up_ratio = up_count / (up_count + down_count)
            score += (up_ratio - 0.5) * 20  # -10 to +10
        
        # Limit up/down contribution
        score += limit_up * 0.3
        score -= limit_down * 0.3
        
        return max(0, min(100, int(score)))
    
    def get_sector_strength(self, stock_code: str, market_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get sector strength for a stock (Plan A: main industry only).
        
        Uses 24h cache to avoid repeated API calls.
        
        Args:
            stock_code: Stock code
            market_context: Market context with top/bottom sectors
        
        Returns:
            {
                'name': '行业名称',
                'strength_score': 50,  # or None if unavailable
                'change_5d': 5.2,
                'is_leader': False,
                'is_laggard': False,
                'data_available': True,  # False if data unavailable
                'message': None,  # Error message if data_available is False
            }
        """
        # Check cache first
        cache_key = f"sector:{stock_code}"
        if cache_key in self._sector_cache:
            cached_time = self._sector_cache_time.get(cache_key, 0)
            if time.time() - cached_time < self._SECTOR_CACHE_TTL:
                logger.debug(f"[Sector] Cache hit for {stock_code}")
                return self._sector_cache[cache_key]
        
        # Default result structure
        result = {
            'name': None,
            'strength_score': None,
            'change_5d': None,
            'is_leader': False,
            'is_laggard': False,
            'data_available': False,
            'message': None
        }
        
        # Get stock's primary industry
        try:
            sectors = self.fetcher_manager.get_stock_sectors(stock_code)
            
            if sectors is None:
                # API failed after retries
                result['message'] = '板块数据暂时不可用'
                logger.warning(f"[Sector] Failed to get industry for {stock_code} after retries")
                return result
            
            if not sectors:
                # Stock has no industry info
                result['message'] = '未找到行业信息'
                logger.debug(f"[Sector] No industry found for {stock_code}")
                return result
            
            sector_name = sectors[0]  # Plan A: main industry only
            result['name'] = sector_name
            logger.debug(f"[Sector] {stock_code} primary industry: {sector_name}")
            
        except Exception as e:
            result['message'] = '板块数据暂时不可用'
            logger.warning(f"[Sector] Exception getting industry for {stock_code}: {e}")
            return result
        
        # Get sector historical data
        try:
            sector_data = self.fetcher_manager.get_sector_history(sector_name, days=10)
            
            if sector_data is None:
                # API failed after retries
                result['message'] = '板块数据暂时不可用'
                logger.warning(f"[Sector] Failed to get sector history for {sector_name} after retries")
                return result
            
            # Check if in top/bottom sectors
            top_sectors = market_context.get('top_sectors', []) if market_context else []
            bottom_sectors = market_context.get('bottom_sectors', []) if market_context else []
            is_leader = any(s.get('name') == sector_name for s in top_sectors)
            is_laggard = any(s.get('name') == sector_name for s in bottom_sectors)
            
            result.update({
                'strength_score': sector_data.get('strength_score', 50),
                'change_5d': sector_data.get('change_5d'),
                'is_leader': is_leader,
                'is_laggard': is_laggard,
                'data_available': True,
            })
            
            # Cache the result
            self._sector_cache[cache_key] = result
            self._sector_cache_time[cache_key] = time.time()
            
            logger.info(f"[Sector] {stock_code} sector strength: {sector_name} ({result['strength_score']})")
            
            return result
            
        except Exception as e:
            result['message'] = '板块数据暂时不可用'
            logger.warning(f"[Sector] Exception getting sector history for {sector_name}: {e}")
            return result


# ============================================================
# Bear Market Constraints
# ============================================================

def apply_bear_constraints(
    sentiment_score: Optional[int],
    operation_advice: str,
    dashboard: Optional[Dict[str, Any]],
    market_context: Optional[Dict[str, Any]],
    is_etf: bool = False
) -> Dict[str, Any]:
    """
    Apply bear market hard constraints to analysis result.
    
    This function is used by both Pipeline and Agent modes.
    
    Args:
        sentiment_score: Original sentiment score (0-100)
        operation_advice: Original operation advice
        dashboard: Dashboard dict (will be modified in-place for core_conclusion)
        market_context: Market context dict with 'regime' key
        is_etf: Whether the stock is an ETF
    
    Returns:
        {
            'sentiment_score': modified sentiment score,
            'operation_advice': modified operation advice,
            'one_sentence': modified one sentence (or None),
            'applied': True if constraints were applied
        }
    """
    result = {
        'sentiment_score': sentiment_score,
        'operation_advice': operation_advice,
        'one_sentence': None,
        'applied': False
    }
    
    if not market_context:
        return result
    
    regime = market_context.get('regime', 'range')
    if regime != 'bear':
        return result
    
    result['applied'] = True
    
    # Score ceiling: max 65 in bear market
    if sentiment_score is not None and sentiment_score > 65:
        result['sentiment_score'] = 65
        logger.info(f"[BearConstraint] Score ceiling: {sentiment_score} -> 65")
    
    # Operation advice constraints
    forbidden_advice = ['强烈买入', '加仓', '买入']
    if operation_advice in forbidden_advice:
        if is_etf:
            result['operation_advice'] = '定投'
            logger.info("[BearConstraint] ETF advice changed to: 定投")
        else:
            result['operation_advice'] = '观望'
            logger.info("[BearConstraint] Advice changed to: 观望")
    
    # Force risk warning in core_conclusion
    if dashboard and isinstance(dashboard, dict):
        core = dashboard.get('core_conclusion')
        if core and isinstance(core, dict):
            one_sentence = core.get('one_sentence', '')
            if '大盘弱势' not in one_sentence and '风险' not in one_sentence:
                result['one_sentence'] = f"⚠️ 当前大盘弱势，风险偏好降低。{one_sentence}"
                logger.info("[BearConstraint] Added bear market warning")
    
    return result


def validate_bear_constraints(result: Dict[str, Any], market_context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate and apply bear market constraints to Agent result.
    
    This is a wrapper for Agent mode to ensure bear constraints are applied
    even if the LLM doesn't follow the SYSTEM_PROMPT perfectly.
    
    Args:
        result: Analysis result dict (dashboard JSON from LLM)
        market_context: Market context dict
    
    Returns:
        Modified result dict with bear constraints applied
    """
    if not market_context or market_context.get('regime') != 'bear':
        return result
    
    # Extract fields from result
    sentiment_score = result.get('sentiment_score')
    operation_advice = result.get('operation_advice', '')
    dashboard = result.get('dashboard')
    is_etf = False
    
    # Check if ETF
    if dashboard and isinstance(dashboard, dict):
        # Try to detect ETF from various fields
        stock_name = result.get('stock_name', '')
        if 'ETF' in stock_name or 'etf' in stock_name.lower():
            is_etf = True
    
    # Apply constraints
    constrained = apply_bear_constraints(
        sentiment_score=sentiment_score,
        operation_advice=operation_advice,
        dashboard=dashboard,
        market_context=market_context,
        is_etf=is_etf
    )
    
    # Update result
    if constrained['applied']:
        result['sentiment_score'] = constrained['sentiment_score']
        result['operation_advice'] = constrained['operation_advice']
        
        if constrained['one_sentence'] and dashboard:
            if 'core_conclusion' not in dashboard:
                dashboard['core_conclusion'] = {}
            dashboard['core_conclusion']['one_sentence'] = constrained['one_sentence']
    
    return result


# ============================================================
# Shared SYSTEM_PROMPT Sections
# ============================================================

# This section is shared between Pipeline (analyzer.py) and Agent (executor.py)
# It defines the hard rules for market environment

MARKET_RULES_PROMPT = """
### 10. 大盘环境硬规则（必须严格遵守）

#### 10.1 乖离率阈值动态调整
根据系统注入或工具获取的 market regime 字段，动态调整乖离率上限：

| 大盘状态 | regime 值 | 乖离率上限 | 规则 |
|----------|-----------|-----------|------|
| 牛市     | bull      | < 8%      | 可适度追涨，强势股放宽 |
| 震荡     | range     | < 5%      | 标准要求 |
| 熊市     | bear      | < 3%      | 严格不追高，风险优先 |

**判定逻辑**：分析时必须将当前乖离率与此阈值对比，超过即判定为"严禁追高"。

#### 10.2 大盘状态判定标准
大盘状态由以下条件判定：
- bull: 上证指数涨 > 1% 且 上涨家数占比 > 60%
- bear: 上证指数跌 > 1% 且 下跌家数占比 > 60%（上涨家数占比 < 40%）
- range: 其他情况

可通过以下方式获取：
- Pipeline 模式：context['market']['regime']
- Agent 模式：调用 get_market_context 工具

#### 10.3 熊市交易限制（强制执行）
当 regime = "bear" 时：

**评分约束**：
- sentiment_score 最高不超过 65 分
- 即使技术面完美，也禁止判定为"强烈买入"

**操作建议约束**：
- operation_advice 禁止输出："强烈买入"、"加仓"、"买入"
- 仅允许输出："观望"、"减仓"、"卖出"、"持有（已有仓位）"

**输出格式强制**：
在 dashboard.core_conclusion.one_sentence 中必须包含："⚠️ 当前大盘弱势，风险偏好降低"

**ETF 例外**：
若股票为 ETF（名称包含 "ETF"），可放宽为"定投"建议（熊市定投宽基指数）

#### 10.4 板块联动规则
当个股所属板块在市场数据中出现时：

- 板块在领涨榜（top_sectors）：在 positive_catalysts 中标注"板块强势"
- 板块在领跌榜（bottom_sectors）：在 risk_alerts 中标注"板块弱势"

#### 10.5 板块强度评分规则
当系统注入或工具返回 sector.strength_score 时：

- strength_score ≥ 70：允许买入，板块强势
- strength_score 50-70：正常评估
- strength_score < 50：谨慎观望，在 risk_alerts 中标注"板块走势偏弱"
- strength_score < 30：禁止买入（除非股价已大幅下跌），标注"板块极弱"
"""

# Shared trading philosophy (sections 1-9 are also shared but may have minor differences)

TRADING_PHILOSOPHY_PROMPT = """
## 核心交易理念（必须严格遵守）

### 1. 严进策略（不追高）
- **绝对不追高**：当股价偏离 MA5 超过 5% 时，坚决不买入
- **乖离率公式**：(现价 - MA5) / MA5 × 100%
- 乖离率 < 2%：最佳买点区间
- 乖离率 2-5%：可小仓介入
- 乖离率 > 5%：严禁追高！直接判定为"观望"

### 2. 趋势交易（顺势而为）
- **多头排列必须条件**：MA5 > MA10 > MA20
- 只做多头排列的股票，空头排列坚决不碰
- 均线发散上行优于均线粘合
- 趋势强度判断：看均线间距是否在扩大

### 3. 效率优先（筹码结构）
- 关注筹码集中度：90%集中度 < 15% 表示筹码集中
- 获利比例分析：70-90% 获利盘时需警惕获利回吐
- 平均成本与现价关系：现价高于平均成本 5-15% 为健康

### 4. 买点偏好（回踩支撑）
- **最佳买点**：缩量回踩 MA5 获得支撑
- **次优买点**：回踩 MA10 获得支撑
- **观望情况**：跌破 MA20 时观望

### 5. 风险排查重点
- 减持公告（股东、高管减持）
- 业绩预亏/大幅下滑
- 监管处罚/立案调查
- 行业政策利空
- 大额解禁

### 6. 估值关注（PE/PB）
- 分析时请关注市盈率（PE）是否合理
- PE 明显偏高时（如远超行业平均或历史均值），需在风险点中说明
- 高成长股可适当容忍较高 PE，但需有业绩支撑

### 7. 强势趋势股放宽
- 强势趋势股（多头排列且趋势强度高、量能配合）可适当放宽乖离率要求
- 此类股票可轻仓追踪，但仍需设置止损，不盲目追高

### 8. 成交量动力学校验（量价匹配二次校验）
在给出最终决策前，必须进行"量价匹配"校验：

#### 8.1 放量突破（攻击态）
- **触发场景**：股价涨幅 > 3% 且试图突破压力位或中轨
- **校验要求**：当日量比需 > 1.2（即成交量 > 5日平均成交量的 1.2 倍）
- **结论**：若无放量配合，视为"诱多"，决策强制降级为"观望"

#### 8.2 缩量回踩（蓄势态）
- **触发场景**：股价回撤至 MA5 或 MA10 支撑位
- **校验要求**：当日量比需 < 0.8（即成交量 < 5日平均成交量的 0.8 倍）
- **结论**：若放量下跌，视为"主力出货"，取消买入计划，决策改为"离场/观望"

#### 8.3 无量阴跌（衰竭态）
- **触发场景**：空头排列且成交额/成交量持续萎缩
- **结论**：严禁抄底，即便评分较高也必须标注"资金关注度极低"

### 9. 数据缺失容错机制：筹码状态反推
- **若筹码/获利比例数据缺失**：禁止直接报错，需按以下优先级进行逻辑推理：

#### 9.1 趋势推论（优先级最高）
- **强多头**：MA5 > MA10 > MA20 且创近期新高 → 判定为"获利盘占比高，筹码结构健康，无套牢盘压力"
- **弱多头**：多头排列但接近均线支撑或乖离率偏低 → 判定为"结构一般，获利盘适中"
- **空头**：空头排列且远离 MA20 → 判定为"上方套牢盘堆积，筹码散乱"

#### 9.2 量能推论（次优先级）
- **锁仓迹象**：横盘且换手率极低(< 2%)、量比萎缩 → 判定为"筹码高度锁定，主力锁仓迹象明显"
- **筹码松动**：横盘但放量异常 → 判定为"筹码松动，存在短期抛压"

#### 9.3 风险推论（最低优先级）
- **部分锁定**：空头排列中若出现短期小幅反弹 → 判定为"筹码散乱但部分锁定"
- **极其松散**：空头排列下放量下跌 → 判定为"筹码极其松散，风险高"

#### 9.4 替代描述格式
- 在检查清单中将"筹码数据缺失"替换为"基于量价推导：[健康 / 结构一般 / 松动 / 散乱]"
- **必须注明推理依据**，格式示例：
  - "基于量价推导：健康（理由：强多头排列且缩量回踩，显示获利盘锁定良好）"
  - "基于量价推导：结构一般（理由：多头排列但乖离率偏低）"
  - "基于量价推导：松动（理由：横盘但放量异常）"
  - "基于量价推导：散乱（理由：空头排列且远离MA20）"
"""

# Full shared prompt combining all sections
SHARED_TRADING_PROMPT = TRADING_PHILOSOPHY_PROMPT + MARKET_RULES_PROMPT