# -*- coding: utf-8 -*-
"""
Market tools — wraps DataFetcherManager market-level methods as agent tools.

Tools:
- get_market_indices: major market index data
- get_sector_rankings: sector performance rankings
- get_market_context: comprehensive market context (regime, strength, sectors)
- get_sector_strength: sector strength analysis for a stock
"""

import logging

from src.agent.tools.registry import ToolParameter, ToolDefinition

logger = logging.getLogger(__name__)


def _get_fetcher_manager():
    """Lazy import to avoid circular deps."""
    from data_provider import DataFetcherManager
    return DataFetcherManager()


# ============================================================
# get_market_indices
# ============================================================

def _handle_get_market_indices(region: str = "cn") -> dict:
    """Get major market indices."""
    manager = _get_fetcher_manager()
    indices = manager.get_main_indices(region=region)

    if not indices:
        return {"error": f"No market index data available for region '{region}'"}

    return {
        "region": region,
        "indices_count": len(indices),
        "indices": indices,
    }


get_market_indices_tool = ToolDefinition(
    name="get_market_indices",
    description="Get major market indices (e.g., Shanghai Composite, Shenzhen Component, "
                "CSI 300 for China; S&P 500, Nasdaq, Dow for US). Provides market overview.",
    parameters=[
        ToolParameter(
            name="region",
            type="string",
            description="Market region: 'cn' for China A-shares, 'us' for US stocks (default: 'cn')",
            required=False,
            default="cn",
            enum=["cn", "us"],
        ),
    ],
    handler=_handle_get_market_indices,
    category="market",
)


# ============================================================
# get_sector_rankings
# ============================================================

def _handle_get_sector_rankings(top_n: int = 10) -> dict:
    """Get sector performance rankings."""
    manager = _get_fetcher_manager()
    result = manager.get_sector_rankings(n=top_n)

    if result is None:
        return {"error": "No sector ranking data available"}

    # get_sector_rankings returns Tuple[List[Dict], List[Dict]]
    # (top_sectors, bottom_sectors)
    if isinstance(result, tuple) and len(result) == 2:
        top_sectors, bottom_sectors = result
        return {
            "top_sectors": top_sectors,
            "bottom_sectors": bottom_sectors,
        }
    elif isinstance(result, list):
        return {"sectors": result}
    else:
        return {"data": str(result)}


get_sector_rankings_tool = ToolDefinition(
    name="get_sector_rankings",
    description="Get sector/industry performance rankings. Returns top N and bottom N "
                "sectors by daily change percentage. Useful for sector rotation analysis.",
    parameters=[
        ToolParameter(
            name="top_n",
            type="integer",
            description="Number of top/bottom sectors to return (default: 10)",
            required=False,
            default=10,
        ),
    ],
    handler=_handle_get_sector_rankings,
    category="market",
)


# ============================================================
# get_market_context (NEW)
# ============================================================

def _handle_get_market_context(region: str = "cn") -> dict:
    """
    Get comprehensive market context including regime detection.
    
    Returns:
        - indices: Major index quotes
        - market_breadth: Up/down/limit stats
        - top_sectors: Top gaining sectors
        - bottom_sectors: Top losing sectors
        - regime: 'bull' | 'bear' | 'range'
        - strength_score: 0-100 market strength
        - sh_index: Shanghai index data
    """
    from src.services.market_service import MarketService
    
    service = MarketService()
    context = service.get_market_context(region=region)
    
    # Format for Agent output
    result = {
        "regime": context.get("regime", "range"),
        "strength_score": context.get("strength_score", 50),
        "indices": context.get("indices", []),
        "market_breadth": context.get("market_breadth", {}),
        "top_sectors": context.get("top_sectors", []),
        "bottom_sectors": context.get("bottom_sectors", []),
    }
    
    if "sh_index" in context:
        result["sh_index"] = context["sh_index"]
    
    return result


get_market_context_tool = ToolDefinition(
    name="get_market_context",
    description="Get comprehensive market context including regime (bull/bear/range), "
                "strength score (0-100), sector rankings, and index data. "
                "Use this BEFORE analyzing a stock to understand overall market conditions. "
                "The regime affects trading rules: bear market limits score to 65 and forbids 'buy' advice.",
    parameters=[
        ToolParameter(
            name="region",
            type="string",
            description="Market region: 'cn' for China A-shares (default)",
            required=False,
            default="cn",
        ),
    ],
    handler=_handle_get_market_context,
    category="market",
)


# ============================================================
# get_sector_strength (NEW)
# ============================================================

def _handle_get_sector_strength(stock_code: str) -> dict:
    """
    Get sector strength analysis for a stock's primary industry.
    
    Returns:
        - name: Sector/industry name
        - strength_score: 0-100
        - change_5d: 5-day change percentage
        - is_leader: True if in top gaining sectors
        - is_laggard: True if in bottom losing sectors
    """
    from src.services.market_service import MarketService
    
    service = MarketService()
    
    # First get market context for sector rankings
    market_context = service.get_market_context()
    
    result = service.get_sector_strength(stock_code, market_context)
    
    return result


get_sector_strength_tool = ToolDefinition(
    name="get_sector_strength",
    description="Get sector strength analysis for a stock's primary industry. "
                "Returns strength_score (0-100), 5-day change, and whether the sector "
                "is in top/bottom rankings. Use after get_market_context to evaluate "
                "sector health. Rules: score>=70 allows buy, score<50 requires caution, "
                "score<30 forbids buy.",
    parameters=[
        ToolParameter(
            name="stock_code",
            type="string",
            description="Stock code, e.g., '600519' for Kweichow Moutai",
            required=True,
        ),
    ],
    handler=_handle_get_sector_strength,
    category="market",
)


ALL_MARKET_TOOLS = [
    get_market_indices_tool,
    get_sector_rankings_tool,
    get_market_context_tool,      # NEW
    get_sector_strength_tool,      # NEW
]
