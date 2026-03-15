import React from 'react';

/**
 * Badge Variants
 * - aurora: Primary brand color (cyan)
 * - nebula: Secondary brand color (purple)
 * - success: Green for positive states
 * - warning: Yellow/Orange for caution
 * - danger: Red for errors/negative
 * - default: Neutral gray
 */
type BadgeVariant = 
  | 'aurora' 
  | 'nebula' 
  | 'success' 
  | 'warning' 
  | 'danger' 
  | 'default';

type BadgeSize = 'sm' | 'md';

interface BadgeProps {
  /** Badge content */
  children: React.ReactNode;
  /** Visual variant */
  variant?: BadgeVariant;
  /** Size variant */
  size?: BadgeSize;
  /** Add glow effect */
  glow?: boolean;
  /** Custom className */
  className?: string;
  /** Click handler */
  onClick?: () => void;
}

/**
 * Modern Badge Component
 * 
 * @example
 * <Badge variant="aurora" glow>NEW</Badge>
 * <Badge variant="success">+12.5%</Badge>
 * <Badge variant="danger" size="md">Error</Badge>
 */
export const Badge: React.FC<BadgeProps> = ({
  children,
  variant = 'default',
  size = 'sm',
  glow = false,
  className = '',
  onClick,
}) => {
  // Size styles
  const sizeStyles = size === 'sm' 
    ? 'px-2 py-0.5 text-xs' 
    : 'px-3 py-1 text-sm';

  // Variant styles
  const variantStyles: Record<BadgeVariant, string> = {
    aurora: `
      bg-brand-primary/10 
      text-brand-primary 
      border border-brand-primary/20
    `,
    nebula: `
      bg-brand-secondary/10 
      text-brand-secondary-light 
      border border-brand-secondary/20
    `,
    success: `
      bg-semantic-success/10 
      text-semantic-success 
      border border-semantic-success/20
    `,
    warning: `
      bg-semantic-warning/10 
      text-semantic-warning 
      border border-semantic-warning/20
    `,
    danger: `
      bg-semantic-danger/10 
      text-semantic-danger 
      border border-semantic-danger/20
    `,
    default: `
      bg-surface-5 
      text-content-secondary 
      border border-white/10
    `,
  };

  // Glow styles
  const glowStyles: Record<BadgeVariant, string> = {
    aurora: 'shadow-glow-primary',
    nebula: 'shadow-glow-secondary',
    success: 'shadow-glow-success',
    warning: '',
    danger: 'shadow-glow-danger',
    default: '',
  };

  // Clickable styles
  const clickableStyles = onClick 
    ? 'cursor-pointer hover:opacity-80 active:opacity-60' 
    : '';

  return (
    <span
      className={`
        inline-flex items-center gap-1 
        rounded-full font-medium
        backdrop-blur-sm
        ${sizeStyles}
        ${variantStyles[variant]}
        ${glow ? glowStyles[variant] : ''}
        ${clickableStyles}
        ${className}
      `}
      onClick={onClick}
      role={onClick ? 'button' : undefined}
    >
      {children}
    </span>
  );
};

export default Badge;
