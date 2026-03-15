import type React from 'react';

/**
 * Card Variants
 * - default: Standard glass card
 * - bordered: Glass card with visible border
 * - gradient: Gradient border effect
 * - metric: Data card with decorative glow
 * - data: Standard data card with hover effect
 */
type CardVariant = 'default' | 'bordered' | 'gradient' | 'metric' | 'data';

type CardPadding = 'none' | 'sm' | 'md' | 'lg';

interface CardProps {
  /** Card title */
  title?: string;
  /** Card subtitle */
  subtitle?: string;
  /** Card content */
  children: React.ReactNode;
  /** Additional CSS classes */
  className?: string;
  /** Visual variant */
  variant?: CardVariant;
  /** Enable hover effect */
  hoverable?: boolean;
  /** Padding size */
  padding?: CardPadding;
  /** Full height */
  fullHeight?: boolean;
}

// Padding styles - defined outside render
const PADDING_STYLES = {
  none: '',
  sm: 'p-3',
  md: 'p-4',
  lg: 'p-5',
} as const;

// Header component - defined outside render
interface CardHeaderProps {
  title?: string;
  subtitle?: string;
}

const CardHeader: React.FC<CardHeaderProps> = ({ title, subtitle }) => {
  if (!title && !subtitle) return null;
  
  return (
    <div className="mb-4">
      {subtitle && (
        <span className="text-xs font-semibold tracking-wider uppercase text-brand-secondary">
          {subtitle}
        </span>
      )}
      {title && (
        <h3 className="text-lg font-semibold text-content-primary mt-1">
          {title}
        </h3>
      )}
    </div>
  );
};

/**
 * Modern Card Component with Glass Morphism
 * 
 * @example
 * <Card variant="gradient" title="Performance" subtitle="Monthly">
 *   <div>Content here</div>
 * </Card>
 * <Card variant="metric" hoverable>
 *   <div>Data card content</div>
 * </Card>
 */
export const Card: React.FC<CardProps> = ({
  title,
  subtitle,
  children,
  className = '',
  variant = 'default',
  hoverable = false,
  padding = 'md',
  fullHeight = false,
}) => {
  // Base container styles
  const baseStyles = 'rounded-xl relative overflow-hidden';
  const heightStyles = fullHeight ? 'h-full flex flex-col' : '';

  // Variant styles
  const getVariantStyles = (): string => {
    switch (variant) {
      case 'gradient':
        return 'card-gradient-border';
      case 'metric':
        return 'card-metric';
      case 'data':
        return 'card-data';
      case 'bordered':
        return 'glass-card-v3 border border-white/8';
      case 'default':
      default:
        return 'glass-card-v3';
    }
  };

  // Hover styles
  const hoverStyles = hoverable
    ? 'glass-hover cursor-pointer'
    : '';

  // Render gradient variant with inner wrapper
  if (variant === 'gradient') {
    return (
      <div className={`${baseStyles} ${getVariantStyles()} ${className}`}>
        <div className={`card-gradient-border-inner ${PADDING_STYLES[padding]} ${heightStyles}`}>
          <CardHeader title={title} subtitle={subtitle} />
          {children}
        </div>
      </div>
    );
  }

  // Standard variants
  return (
    <div
      className={`
        ${baseStyles}
        ${getVariantStyles()}
        ${hoverStyles}
        ${PADDING_STYLES[padding]}
        ${heightStyles}
        ${className}
      `}
    >
      <CardHeader title={title} subtitle={subtitle} />
      {children}
    </div>
  );
};

export default Card;
