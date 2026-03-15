import type React from 'react';
import { useState, useEffect, useRef, useMemo } from 'react';
import { getSentimentLabel } from '../../types/analysis';

interface ScoreGaugeProps {
  score: number;
  size?: 'sm' | 'md' | 'lg';
  showLabel?: boolean;
  className?: string;
}

/**
 * 霓虹情绪仪表盘 - 多色显示优化版
 * 0-40分: 红色(悲观), 41-60分: 黄色(中性), 61-100分: 绿色(乐观)
 */
export const ScoreGauge: React.FC<ScoreGaugeProps> = ({
  score,
  size = 'md',
  showLabel = true,
  className = '',
}) => {
  const [animatedScore, setAnimatedScore] = useState(0);
  const animationRef = useRef<number | null>(null);

  // 优化动画逻辑，避免抽搐
  useEffect(() => {
    // 取消之前的动画
    if (animationRef.current) {
      cancelAnimationFrame(animationRef.current);
    }

    const startScore = animatedScore;
    const endScore = score;
    const duration = 800;
    const startTime = performance.now();

    const animate = (currentTime: number) => {
      const elapsed = currentTime - startTime;
      const progress = Math.min(elapsed / duration, 1);
      // 使用更平滑的缓动函数
      const easeOut = 1 - Math.pow(1 - progress, 3);
      const currentScore = startScore + (endScore - startScore) * easeOut;
      
      setAnimatedScore(currentScore);

      if (progress < 1) {
        animationRef.current = requestAnimationFrame(animate);
      }
    };

    animationRef.current = requestAnimationFrame(animate);

    return () => {
      if (animationRef.current) {
        cancelAnimationFrame(animationRef.current);
      }
    };
  }, [score]);

  // 根据分数获取颜色 - 多色显示
  const getScoreColor = (s: number): { main: string; glow: string; label: string } => {
    if (s <= 40) {
      // 悲观区间 - 红色
      return { 
        main: '#FF3D00', 
        glow: 'rgba(255, 61, 0, 0.3)',
        label: 'text-[#FF3D00]'
      };
    } else if (s <= 60) {
      // 中性区间 - 琥珀色/黄色
      return { 
        main: '#FFD600', 
        glow: 'rgba(255, 214, 0, 0.3)',
        label: 'text-[#FFD600]'
      };
    } else {
      // 乐观区间 - 绿色
      return { 
        main: '#00E676', 
        glow: 'rgba(0, 230, 118, 0.3)',
        label: 'text-[#00E676]'
      };
    }
  };

  const label = getSentimentLabel(score);
  const colors = getScoreColor(animatedScore);
  const displayScore = Math.round(animatedScore);

  const sizeConfig = {
    sm: { width: 100, stroke: 6, fontSize: 'text-2xl', labelSize: 'text-xs', pointer: 12 },
    md: { width: 140, stroke: 8, fontSize: 'text-4xl', labelSize: 'text-sm', pointer: 16 },
    lg: { width: 180, stroke: 10, fontSize: 'text-5xl', labelSize: 'text-base', pointer: 20 },
  };

  const { width, stroke, fontSize, labelSize, pointer } = sizeConfig[size];
  const center = width / 2;
  const radius = (width - stroke) / 2 - 8;
  
  // 计算指针角度 - 使用useMemo避免重复计算
  const { pointerX, pointerY, arcPath } = useMemo(() => {
    const angle = 135 + (animatedScore / 100) * 270;
    const pointerRadians = (angle * Math.PI) / 180;
    const px = center + (radius - 5) * Math.cos(pointerRadians);
    const py = center + (radius - 5) * Math.sin(pointerRadians);
    
    // 计算弧线终点
    const endAngle = 135 + (animatedScore / 100) * 270;
    const endRadians = (endAngle * Math.PI) / 180;
    const endX = center + radius * Math.cos(endRadians);
    const endY = center + radius * Math.sin(endRadians);
    
    // 计算弧长角度 - 决定是否需要大弧标志（弧长 > 180° 时需要）
    const arcAngle = (animatedScore / 100) * 270;
    const largeArcFlag = arcAngle > 180 ? 1 : 0;
    
    // 弧线路径 - 修复起点计算，使用动态大弧标志
    const startRadians = (135 * Math.PI) / 180;
    const startX = center + radius * Math.cos(startRadians);
    const startY = center + radius * Math.sin(startRadians);
    const path = `M ${startX} ${startY} A ${radius} ${radius} 0 ${largeArcFlag} 1 ${endX} ${endY}`;
    
    return { pointerX: px, pointerY: py, arcPath: path };
  }, [animatedScore, center, radius]);

  // 生成刻度线
  const ticks = useMemo(() => {
    return Array.from({ length: 11 }, (_, i) => {
      const tickAngle = 135 + (i * 27);
      const tickRadians = (tickAngle * Math.PI) / 180;
      const innerRadius = radius - 8;
      const outerRadius = radius;
      return {
        x1: center + innerRadius * Math.cos(tickRadians),
        y1: center + innerRadius * Math.sin(tickRadians),
        x2: center + outerRadius * Math.cos(tickRadians),
        y2: center + outerRadius * Math.sin(tickRadians),
        isMajor: i === 0 || i === 5 || i === 10,
      };
    });
  }, [center, radius]);

  return (
    <div className={`flex flex-col items-center ${className}`}>
      {/* Gauge Container */}
      <div className="relative" style={{ width, height: width }}>
        <svg 
          width={width} 
          height={width}
          className="overflow-visible"
          style={{ transform: 'translateZ(0)' }} // 启用GPU加速
        >
          <defs>
            {/* 动态颜色渐变 */}
            <linearGradient id={`score-gradient-${score}`} x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stopColor={colors.main} />
              <stop offset="100%" stopColor={colors.main} stopOpacity="0.6" />
            </linearGradient>
            
            {/* Glow Filter */}
            <filter id={`neon-glow-${score}`} x="-50%" y="-50%" width="200%" height="200%">
              <feGaussianBlur stdDeviation="3" result="blur" />
              <feMerge>
                <feMergeNode in="blur" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
          </defs>

          {/* Tick Marks - 使用白色半透明 */}
          {ticks.map((tick, i) => (
            <line
              key={i}
              x1={tick.x1}
              y1={tick.y1}
              x2={tick.x2}
              y2={tick.y2}
              stroke="rgba(255, 255, 255, 0.3)"
              strokeWidth={tick.isMajor ? 2 : 1}
              opacity={tick.isMajor ? 0.8 : 0.4}
              strokeLinecap="round"
            />
          ))}

          {/* Background Arc */}
          <path
            d={`M ${center + radius * Math.cos((135 * Math.PI) / 180)} ${center + radius * Math.sin((135 * Math.PI) / 180)} A ${radius} ${radius} 0 1 1 ${center + radius * Math.cos((45 * Math.PI) / 180)} ${center + radius * Math.sin((45 * Math.PI) / 180)}`}
            fill="none"
            stroke="rgba(255, 255, 255, 0.05)"
            strokeWidth={stroke}
            strokeLinecap="round"
          />

          {/* Progress Arc - 使用当前分数颜色 */}
          <path
            d={arcPath}
            fill="none"
            stroke={colors.main}
            strokeWidth={stroke}
            strokeLinecap="round"
            filter={`url(#neon-glow-${score})`}
            opacity={0.7}
            style={{ transition: 'stroke 0.3s ease' }}
          />

          {/* Pointer */}
          <circle
            cx={pointerX}
            cy={pointerY}
            r={pointer / 2}
            fill={colors.main}
            filter={`url(#neon-glow-${score})`}
            style={{ transition: 'fill 0.3s ease' }}
          />
          
          {/* Pointer Center Dot */}
          <circle
            cx={pointerX}
            cy={pointerY}
            r={pointer / 4}
            fill="#ffffff"
          />

          {/* Center Hub */}
          <circle
            cx={center}
            cy={center}
            r={pointer / 2}
            fill="rgba(16, 24, 36, 0.9)"
            stroke={colors.main}
            strokeWidth={2}
            style={{ transition: 'stroke 0.3s ease' }}
          />
        </svg>

        {/* Center Score Display */}
        <div className="absolute inset-0 flex flex-col items-center justify-center">
          <span
            className={`font-bold ${fontSize}`}
            style={{
              color: colors.main,
              textShadow: `0 0 20px ${colors.glow}`,
              transition: 'color 0.3s ease',
            }}
          >
            {displayScore}
          </span>
          {showLabel && (
            <span
              className={`${labelSize} font-semibold mt-1`}
              style={{
                color: colors.main,
                textShadow: `0 0 10px ${colors.glow}`,
                transition: 'color 0.3s ease',
              }}
            >
              {label}
            </span>
          )}
        </div>
      </div>

      {/* Decorative Glow Ring - 使用当前颜色 */}
      <div 
        className="absolute w-32 h-32 rounded-full blur-3xl pointer-events-none"
        style={{
          background: `radial-gradient(circle, ${colors.glow} 0%, transparent 70%)`,
          transition: 'background 0.3s ease',
        }}
      />
    </div>
  );
};

export default ScoreGauge;
