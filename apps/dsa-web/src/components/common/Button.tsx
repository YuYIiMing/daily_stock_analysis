import React from 'react';

/**
 * Button Variants
 * - aurora: Primary brand gradient (aurora blue #00F2FE)
 * - nebula: Secondary brand gradient (neon purple #8A2BE2)
 * - ghost: Transparent with subtle border
 * - gradient: Dual-color gradient (aurora → neon)
 * - danger: Error state
 */
type ButtonVariant = 'aurora' | 'nebula' | 'ghost' | 'gradient' | 'danger';

type ButtonSize = 'sm' | 'md' | 'lg';

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  /** Button visual variant */
  variant?: ButtonVariant;
  /** Button size */
  size?: ButtonSize;
  /** Show loading spinner */
  isLoading?: boolean;
  /** Full width button */
  fullWidth?: boolean;
  /** Custom className */
  className?: string;
}

// Size styles
const SIZE_STYLES = {
  sm: 'px-3 py-1.5 text-xs rounded-xl gap-1.5',
  md: 'px-5 py-2.5 text-sm rounded-xl gap-2',
  lg: 'px-6 py-3 text-base rounded-xl gap-2.5',
} as const;

// Variant styles - Bento Glassmorphism design
const VARIANT_STYLES: Record<ButtonVariant, string> = {
  aurora: `
    bg-gradient-to-br from-[#00F2FE] to-[#00D4E0]
    text-[#050C16] font-semibold
    hover:shadow-[0_4px_20px_rgba(0,242,254,0.3)] hover:-translate-y-0.5
    active:translate-y-0
  `,
  nebula: `
    bg-gradient-to-br from-[#8A2BE2] to-[#7B1FA2]
    text-white font-semibold
    hover:shadow-[0_4px_20px_rgba(138,43,226,0.3)] hover:-translate-y-0.5
    active:translate-y-0
  `,
  ghost: `
    bg-transparent
    border border-[rgba(255,255,255,0.1)]
    text-[rgba(255,255,255,0.7)] font-medium
    hover:bg-[rgba(255,255,255,0.05)] hover:border-[rgba(255,255,255,0.2)] hover:text-white
  `,
  gradient: `
    bg-gradient-to-r from-[#00F2FE] via-[#4facfe] to-[#8A2BE2]
    text-white font-semibold
    hover:shadow-[0_4px_20px_rgba(0,242,254,0.3)] hover:-translate-y-0.5
    active:translate-y-0
  `,
  danger: `
    bg-[#FF3D00]
    text-white font-semibold
    hover:shadow-[0_4px_20px_rgba(255,61,0,0.3)] hover:-translate-y-0.5
    active:translate-y-0
  `,
};

// Loading spinner component - defined outside render
const LoadingSpinner = () => (
  <svg
    className="animate-spin -ml-1 h-4 w-4"
    xmlns="http://www.w3.org/2000/svg"
    fill="none"
    viewBox="0 0 24 24"
    aria-hidden="true"
  >
    <circle
      className="opacity-25"
      cx="12"
      cy="12"
      r="10"
      stroke="currentColor"
      strokeWidth="4"
    />
    <path
      className="opacity-75"
      fill="currentColor"
      d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
    />
  </svg>
);

/**
 * Modern Button Component - Bento Glassmorphism Design
 * 
 * @example
 * <Button variant="aurora">分析</Button>
 * <Button variant="nebula">日报</Button>
 * <Button variant="ghost" size="sm">取消</Button>
 * <Button variant="aurora" isLoading>分析中...</Button>
 */
export const Button: React.FC<ButtonProps> = ({
  children,
  variant = 'aurora',
  size = 'md',
  isLoading = false,
  fullWidth = false,
  className = '',
  disabled,
  ...props
}) => {
  // Base styles
  const baseStyles = `
    inline-flex items-center justify-center
    font-medium
    transition-all duration-200
    focus:outline-none
    disabled:opacity-50 disabled:cursor-not-allowed disabled:transform-none
    will-change: transform, box-shadow
  `;

  // Width styles
  const widthStyles = fullWidth ? 'w-full' : '';

  return (
    <button
      className={`
        ${baseStyles}
        ${SIZE_STYLES[size]}
        ${VARIANT_STYLES[variant]}
        ${widthStyles}
        ${className}
      `}
      disabled={disabled || isLoading}
      aria-busy={isLoading}
      aria-disabled={disabled || isLoading}
      {...props}
    >
      {isLoading ? (
        <>
          <LoadingSpinner />
          <span>加载中...</span>
        </>
      ) : (
        children
      )}
    </button>
  );
};

export default Button;