import React from 'react';

interface LoadingProps {
  /** Size of the spinner */
  size?: 'sm' | 'md' | 'lg';
  /** Optional text label */
  text?: string;
  /** Show pulse glow effect */
  glow?: boolean;
  /** Custom className */
  className?: string;
}

/**
 * Modern Loading Spinner Component
 * 
 * @example
 * <Loading />
 * <Loading size="lg" text="Analyzing..." glow />
 * <Loading size="sm" />
 */
export const Loading: React.FC<LoadingProps> = ({
  size = 'md',
  text,
  glow = false,
  className = '',
}) => {
  // Size mappings
  const sizeStyles = {
    sm: 'w-5 h-5 border-2',
    md: 'w-8 h-8 border-3',
    lg: 'w-12 h-12 border-4',
  };

  // Text size mappings
  const textSizes = {
    sm: 'text-xs',
    md: 'text-sm',
    lg: 'text-base',
  };

  return (
    <div 
      className={`flex justify-center items-center p-8 ${className}`}
      role="status"
      aria-label={text || 'Loading'}
    >
      <div className="flex flex-col items-center gap-3">
        {/* Spinner */}
        <div className="relative">
          {/* Glow effect background */}
          {glow && (
            <div 
              className="absolute inset-0 rounded-full animate-pulse-glow"
              style={{ 
                background: 'var(--brand-primary-glow)',
                filter: 'blur(8px)',
              }}
            />
          )}
          
          {/* Spinning ring */}
          <div
            className={`
              ${sizeStyles[size]}
              rounded-full
              border-brand-primary/20
              border-t-brand-primary
              animate-spin
              relative
            `}
            style={{
              borderStyle: 'solid',
            }}
          />
        </div>
        
        {/* Optional text */}
        {text && (
          <span className={`text-content-secondary ${textSizes[size]}`}>
            {text}
          </span>
        )}
      </div>
      
      {/* Screen reader text */}
      <span className="sr-only">{text || 'Loading'}</span>
    </div>
  );
};

export default Loading;
