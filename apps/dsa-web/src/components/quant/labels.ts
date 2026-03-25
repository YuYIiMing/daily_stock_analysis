export function getRegimeLabel(regime?: string | null): string {
  switch (regime) {
    case 'RiskOn':
      return '进攻'
    case 'Neutral':
      return '中性'
    case 'RiskOff':
      return '防守'
    default:
      return regime || '未知'
  }
}

export function getModuleLabel(module?: string | null): string {
  switch (module) {
    case 'BREAKOUT':
      return '突破'
    case 'PULLBACK':
      return '回调'
    case 'LATE_WEAK_TO_STRONG':
      return '后期弱转强'
    case 'ALL':
      return '全部'
    default:
      return module || '未知'
  }
}

export function getStageLabel(stage?: string | null): string {
  switch (stage) {
    case 'EMERGING':
      return '初期'
    case 'TREND':
      return '中期'
    case 'CLIMAX':
      return '后期'
    case 'IGNORE':
      return '震荡'
    case 'ALL':
      return '全部'
    default:
      return stage || '未知'
  }
}

export function getBlockedReasonLabel(reason?: string | null): string {
  switch (reason) {
    case 'risk_off':
      return '市场处于防守状态'
    case 'cooldown_active':
      return '连续止损后处于冷静期'
    case 'invalid_stop_distance':
      return '止损距离无效'
    case 'capacity_exhausted':
      return '仓位或板块额度已用尽'
    default:
      return reason || '--'
  }
}

export function getExitReasonLabel(reason?: string | null): string {
  switch (reason) {
    case 'take_profit':
      return '止盈'
    case 'take_profit_2r':
      return '2R 止盈'
    case 'take_profit_4r':
      return '4R 止盈'
    case 'trailing_stop':
      return '跟踪止盈'
    case 'hard_stop':
      return '硬止损'
    case 'time_stop':
      return '时间止损'
    case 'window_end':
      return '回测窗口结束平仓'
    default:
      return reason || '--'
  }
}

export function getTradeStatusLabel(status?: string | null): string {
  switch (status) {
    case 'closed':
      return '已平仓'
    case 'open':
      return '持仓中'
    default:
      return status || '未知'
  }
}

export function getTabLabel(tab: 'overview' | 'plan' | 'trades'): string {
  switch (tab) {
    case 'overview':
      return '总览'
    case 'plan':
      return '计划'
    case 'trades':
      return '交易'
    default:
      return tab
  }
}
