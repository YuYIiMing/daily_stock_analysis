# -*- coding: utf-8 -*-
"""
===================================
Strategy Selector Module
===================================

Responsibilities:
1. Detect stock type (ETF/index vs regular stock)
2. Select appropriate analysis strategies based on stock type
"""

import re
from typing import Tuple, List, Optional


ETF_CODE_PATTERNS = [
    r'^51[0-9]{4}$',      # Shanghai ETF (510xxx, 511xxx, 512xxx, 513xxx, 515xxx, 516xxx, 517xxx, 518xxx)
    r'^159[0-9]{3}$',     # Shenzhen ETF (159xxx)
    r'^50[0-9]{4}$',      # Shanghai 50ETF series (501xxx)
    r'^56[0-9]{4}$',      # Shanghai 56 series
    r'^58[0-9]{4}$',      # Shanghai 58 series
]

ETF_NAME_KEYWORDS = [
    'ETF', 'etf', '指数', '指数基金', '基金', 'LOF', 'QDII',
    '沪深300', '中证500', '中证1000', '上证50', '创业板',
    '科创50', '纳指', '标普', '恒生', '恒科', '中概',
    '消费', '医药', '科技', '新能源', '芯片', '半导体',
    '红利', '债券', '国债', '货币', '黄金',
]


def detect_stock_type(code: str, stock_name: str = "") -> Tuple[bool, str]:
    """
    Detect if a stock code represents an ETF or index fund.
    
    Args:
        code: Stock code (e.g., '510300', '159915', '600519')
        stock_name: Stock name (e.g., '沪深300ETF', '贵州茅台')
    
    Returns:
        Tuple of (is_etf, etf_type)
        - is_etf: True if the stock is an ETF or index fund
        - etf_type: Description of the ETF type, or empty string if not an ETF
    """
    code = str(code).strip().upper()
    stock_name = str(stock_name or '').strip()
    
    # Check by code pattern
    for pattern in ETF_CODE_PATTERNS:
        if re.match(pattern, code):
            return True, _infer_etf_type(code, stock_name)
    
    # Check by name keywords
    name_lower = stock_name.lower()
    for keyword in ETF_NAME_KEYWORDS:
        if keyword.lower() in name_lower:
            # Additional check: make sure it's not a regular stock with ETF-like name
            if 'ETF' in stock_name or 'etf' in name_lower or '基金' in stock_name or '指数' in stock_name:
                return True, _infer_etf_type(code, stock_name)
    
    # Hong Kong ETF
    if code.startswith('0') and len(code) == 5:
        # Some HK ETFs like 03034, 02846
        if any(kw in stock_name for kw in ['ETF', '指数', '基金']):
            return True, 'HK_ETF'
    
    # US ETF
    if code.isalpha() and len(code) <= 5:
        # Common US ETFs: SPY, QQQ, IWM, etc.
        name_upper = stock_name.upper()
        if 'ETF' in name_upper or any(kw in name_upper for kw in ['INDEX', 'FUND']):
            return True, 'US_ETF'
    
    return False, ""


def _infer_etf_type(code: str, stock_name: str) -> str:
    """
    Infer the ETF type from code and name.
    
    Returns:
        ETF type string (e.g., 'stock_ETF', 'bond_ETF', 'index_ETF', etc.)
    """
    name_lower = stock_name.lower()
    
    # Bond ETF
    if any(kw in stock_name for kw in ['国债', '债券', '债']):
        return 'bond_ETF'
    
    # Money market
    if '货币' in stock_name or '理财' in stock_name:
        return 'money_market_ETF'
    
    # Gold/Commodity
    if '黄金' in stock_name or '黄金' in name_lower or '商品' in stock_name:
        return 'commodity_ETF'
    
    # Cross-border (QDII)
    if any(kw in stock_name for kw in ['纳指', '标普', '恒生', '中概', '日本', '德国', '纳斯达克']):
        return 'cross_border_ETF'
    
    # Index ETF
    if any(kw in stock_name for kw in ['沪深300', '中证', '上证50', '创业板', '科创', '中证500', '中证1000']):
        return 'index_ETF'
    
    # Sector/Theme ETF
    if any(kw in stock_name for kw in ['消费', '医药', '科技', '新能源', '芯片', '半导体', '军工', '银行', '地产', '证券', '白酒', '有色', '煤炭', '电力']):
        return 'sector_ETF'
    
    return 'stock_ETF'


def select_strategies(is_etf: bool = False) -> Optional[List[str]]:
    """
    Select analysis strategies based on stock type.
    
    For ETF: Use ETF-specific strategies (no earnings analysis, focus on index/sector trends)
    For stocks: Use comprehensive strategies
    
    Args:
        is_etf: Whether the analysis target is an ETF
    
    Returns:
        List of strategy names, or None to use default strategies
    """
    if is_etf:
        return ['etf_analysis']
    
    return None  # Use default strategies for regular stocks