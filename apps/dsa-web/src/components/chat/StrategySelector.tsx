import React from 'react';
import { createPortal } from 'react-dom';
import { Check } from 'lucide-react';
import {
  STRATEGIES,
  STRATEGY_CATEGORIES,
  GENERAL_STRATEGY,
  STRATEGY_STORAGE_KEY,
} from '../../config/strategies';
import type { StrategyItem } from '../../config/strategies';

interface StrategySelectorProps {
  selectedStrategy: string;
  onStrategyChange: (strategyId: string) => void;
  disabled?: boolean;
}

interface TooltipPosition {
  x: number;
  y: number;
}

const StrategySelector: React.FC<StrategySelectorProps> = ({
  selectedStrategy,
  onStrategyChange,
  disabled = false,
}) => {
  const [activeCategory, setActiveCategory] = React.useState<string>(() => {
    if (selectedStrategy === '') return STRATEGY_CATEGORIES[0].key;
    const strategy = STRATEGIES.find((s) => s.id === selectedStrategy);
    return strategy?.category || STRATEGY_CATEGORIES[0].key;
  });

  const [hoveredStrategy, setHoveredStrategy] = React.useState<string | null>(null);
  const [tooltipPosition, setTooltipPosition] = React.useState<TooltipPosition>({ x: 0, y: 0 });
  const [tooltipStrategy, setTooltipStrategy] = React.useState<StrategyItem | null>(null);
  const [tooltipColor, setTooltipColor] = React.useState<string>('#00F2FE');

  const handleStrategySelect = (strategyId: string) => {
    if (disabled) return;
    setHoveredStrategy(null);
    setTooltipStrategy(null);
    onStrategyChange(strategyId);
    if (strategyId !== '') {
      const strategy = STRATEGIES.find((s) => s.id === strategyId);
      if (strategy) {
        setActiveCategory(strategy.category);
      }
    }
    try {
      localStorage.setItem(STRATEGY_STORAGE_KEY, strategyId);
    } catch {
      // Ignore storage errors
    }
  };

  const handleMouseEnter = (
    e: React.MouseEvent<HTMLDivElement>,
    strategy: StrategyItem,
    categoryColor: string
  ) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const tooltipWidth = 288;
    const tooltipHeight = 100;
    const padding = 10;

    // Position tooltip above the card
    let x = rect.left;
    let y = rect.top - tooltipHeight - padding;

    // Adjust if tooltip goes beyond left edge
    if (x < 0) {
      x = padding;
    }

    // Adjust if tooltip goes beyond right edge
    if (x + tooltipWidth > window.innerWidth) {
      x = window.innerWidth - tooltipWidth - padding;
    }

    // Adjust if tooltip goes beyond top edge - show below card instead
    if (y < 0) {
      y = rect.bottom + padding;
    }

    setTooltipPosition({ x, y });
    setTooltipStrategy(strategy);
    setTooltipColor(categoryColor);
    setHoveredStrategy(strategy.id);
  };

  const handleMouseLeave = () => {
    setHoveredStrategy(null);
    setTooltipStrategy(null);
  };

  const renderStrategyCard = (strategy: StrategyItem, categoryColor: string) => {
    const isSelected = selectedStrategy === strategy.id;
    const Icon = strategy.icon;

    return (
      <div
        key={strategy.id}
        onMouseEnter={(e) => handleMouseEnter(e, strategy, categoryColor)}
        onMouseLeave={handleMouseLeave}
        className="relative"
      >
        <button
          onClick={() => handleStrategySelect(strategy.id)}
          disabled={disabled}
          className={`
            w-full text-left rounded-xl p-3 transition-all duration-200
            ${disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer hover:-translate-y-0.5'}
            ${isSelected ? 'ring-2 ring-[rgba(0,242,254,0.5)]' : ''}
          `}
          style={{
            background: isSelected 
              ? `linear-gradient(135deg, rgba(0,242,254,0.08), rgba(138,43,226,0.08))`
              : 'rgba(255,255,255,0.03)',
            border: isSelected 
              ? '1px solid rgba(0,242,254,0.3)'
              : '1px solid rgba(255,255,255,0.08)',
            boxShadow: isSelected 
              ? '0 4px 20px rgba(0,242,254,0.15)'
              : '0 2px 8px rgba(0,0,0,0.2)',
          }}
        >
          {isSelected && (
            <div 
              className="absolute top-2 right-2 w-4 h-4 rounded-full flex items-center justify-center"
              style={{ background: 'linear-gradient(135deg, #00F2FE, #8A2BE2)' }}
            >
              <Check className="w-2.5 h-2.5 text-white" />
            </div>
          )}
          
          <div className="flex items-center gap-2">
            <Icon 
              className="w-4 h-4 flex-shrink-0" 
              style={{ color: isSelected ? '#00F2FE' : 'rgba(255,255,255,0.6)' }}
            />
            <span 
              className="text-sm font-medium truncate"
              style={{ color: isSelected ? '#00F2FE' : 'rgba(255,255,255,0.9)' }}
            >
              {strategy.name}
            </span>
          </div>
        </button>
      </div>
    );
  };

  const renderGeneralCard = () => {
    const isSelected = selectedStrategy === '';
    const Icon = GENERAL_STRATEGY.icon;
    const generalAsStrategy: StrategyItem = {
      id: '',
      name: GENERAL_STRATEGY.name,
      category: 'comprehensive',
      icon: GENERAL_STRATEGY.icon,
      coreLogic: GENERAL_STRATEGY.coreLogic,
      applicableScenarios: GENERAL_STRATEGY.applicableScenarios,
    };

    return (
      <div
        onMouseEnter={(e) => handleMouseEnter(e, generalAsStrategy, '#00F2FE')}
        onMouseLeave={handleMouseLeave}
        className="relative"
      >
        <button
          onClick={() => handleStrategySelect('')}
          disabled={disabled}
          className={`
            w-full text-left rounded-xl p-3 transition-all duration-200
            ${disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer hover:-translate-y-0.5'}
            ${isSelected ? 'ring-2 ring-[rgba(0,242,254,0.5)]' : ''}
          `}
          style={{
            background: isSelected
              ? 'linear-gradient(135deg, rgba(0,242,254,0.1), rgba(138,43,226,0.1))'
              : 'rgba(255,255,255,0.03)',
            border: isSelected
              ? '1px solid rgba(0,242,254,0.3)'
              : '1px solid rgba(255,255,255,0.1)',
            boxShadow: isSelected
              ? '0 4px 20px rgba(0,242,254,0.15)'
              : '0 2px 8px rgba(0,0,0,0.2)',
          }}
        >
          {isSelected && (
            <div 
              className="absolute top-2 right-2 w-4 h-4 rounded-full flex items-center justify-center"
              style={{ background: 'linear-gradient(135deg, #00F2FE, #8A2BE2)' }}
            >
              <Check className="w-2.5 h-2.5 text-white" />
            </div>
          )}
          
          <div className="flex items-center gap-2">
            <Icon 
              className="w-4 h-4 flex-shrink-0"
              style={{ color: isSelected ? '#00F2FE' : 'rgba(255,255,255,0.6)' }}
            />
            <span 
              className="text-sm font-medium"
              style={{ color: isSelected ? '#00F2FE' : 'rgba(255,255,255,0.9)' }}
            >
              {GENERAL_STRATEGY.name}
            </span>
          </div>
        </button>
      </div>
    );
  };

  const currentCategory = STRATEGY_CATEGORIES.find((c) => c.key === activeCategory);
  const isComprehensiveCategory = activeCategory === 'comprehensive';

  return (
    <div className="space-y-3">
      {/* Category Tabs */}
      <div className="flex items-center gap-1 overflow-x-auto pb-1 custom-scrollbar">
        {STRATEGY_CATEGORIES.map((category) => {
          const isActive = activeCategory === category.key;
          let count = 0;
          if (category.key === 'comprehensive') {
            count = 1;
          } else {
            count = STRATEGIES.filter((s) => s.category === category.key).length;
          }
          
          return (
            <button
              key={category.key}
              onClick={() => setActiveCategory(category.key)}
              className={`
                flex-shrink-0 px-3 py-1.5 rounded-lg text-xs font-medium transition-all duration-200
                ${disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}
              `}
              style={{
                background: isActive 
                  ? `${category.color}15`
                  : 'rgba(255,255,255,0.03)',
                color: isActive 
                  ? category.color
                  : 'rgba(255,255,255,0.5)',
                border: isActive 
                  ? `1px solid ${category.color}40`
                  : '1px solid rgba(255,255,255,0.08)',
              }}
            >
              {category.label}
              <span 
                className="ml-1 opacity-60"
                style={{ fontSize: '10px' }}
              >
                {count}
              </span>
            </button>
          );
        })}
      </div>

      {/* Strategy Cards - Horizontal Scrollable */}
      <div 
        className="flex gap-2 overflow-x-auto pb-2 custom-scrollbar"
        style={{ scrollbarWidth: 'thin' }}
      >
        {isComprehensiveCategory ? (
          renderGeneralCard()
        ) : (
          STRATEGIES
            .filter((s) => s.category === activeCategory)
            .map((strategy) => renderStrategyCard(strategy, currentCategory?.color || '#00F2FE'))
        )}
      </div>

      {/* Tooltip - Portal to body */}
      {hoveredStrategy && tooltipStrategy && createPortal(
        <div
          className="fixed w-72 p-4 rounded-xl animate-fade-in"
          style={{
            left: tooltipPosition.x,
            top: tooltipPosition.y,
            zIndex: 9999,
            background: 'rgba(16, 24, 36, 0.98)',
            backdropFilter: 'blur(20px)',
            border: '1px solid rgba(255, 255, 255, 0.1)',
            boxShadow: '0 10px 40px rgba(0, 0, 0, 0.5)',
            pointerEvents: 'none',
          }}
        >
          <div className="flex items-center gap-2 mb-2">
            <tooltipStrategy.icon 
              className="w-4 h-4" 
              style={{ color: tooltipColor }} 
            />
            <span className="text-sm font-medium text-[rgba(255,255,255,0.95)]">
              {tooltipStrategy.name}
            </span>
          </div>
          <p className="text-xs leading-relaxed text-[rgba(255,255,255,0.6)] mb-2">
            {tooltipStrategy.coreLogic}
          </p>
          <div className="flex flex-wrap gap-1">
            {tooltipStrategy.applicableScenarios.map((scenario, idx) => (
              <span
                key={idx}
                className="px-1.5 py-0.5 rounded text-[10px]"
                style={{
                  background: `${tooltipColor}20`,
                  color: tooltipColor,
                  border: `1px solid ${tooltipColor}40`,
                }}
              >
                {scenario}
              </span>
            ))}
          </div>
        </div>,
        document.body
      )}
    </div>
  );
};

export default StrategySelector;