/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // Bento Glassmorphism Color System
        // Deep Space Background
        space: {
          900: '#050C16',
          800: '#0A111C',
          700: '#101824',
          600: '#162030',
        },
        
        // Neon Aurora Brand Colors
        aurora: {
          DEFAULT: '#00F2FE',
          glow: 'rgba(0, 242, 254, 0.15)',
          soft: 'rgba(0, 242, 254, 0.08)',
        },
        
        // Neon Purple Accent
        neon: {
          DEFAULT: '#8A2BE2',
          glow: 'rgba(138, 43, 226, 0.15)',
          soft: 'rgba(138, 43, 226, 0.08)',
        },
        
        // Rise/Fall with subtle glow
        rise: {
          DEFAULT: '#00E676',
          glow: 'rgba(0, 230, 118, 0.25)',
        },
        fall: {
          DEFAULT: '#FF3D00',
          glow: 'rgba(255, 61, 0, 0.25)',
        },
        
        // Content hierarchy
        mist: {
          100: 'rgba(255, 255, 255, 0.95)',
          80: 'rgba(255, 255, 255, 0.80)',
          60: 'rgba(255, 255, 255, 0.60)',
          40: 'rgba(255, 255, 255, 0.40)',
          20: 'rgba(255, 255, 255, 0.20)',
          10: 'rgba(255, 255, 255, 0.10)',
          5: 'rgba(255, 255, 255, 0.05)',
        },
        
        // Legacy compatibility
        brand: {
          primary: '#00F2FE',
          secondary: '#8A2BE2',
        },
        surface: {
          0: '#050C16',
          1: '#0A111C',
          2: '#101824',
          3: '#162030',
        },
        content: {
          primary: 'rgba(255, 255, 255, 0.95)',
          secondary: 'rgba(255, 255, 255, 0.65)',
          tertiary: 'rgba(255, 255, 255, 0.40)',
        },
      },
      
      backgroundImage: {
        // Deep space gradient
        'deep-space': 'linear-gradient(180deg, #050C16 0%, #101824 100%)',
        'deep-space-radial': 'radial-gradient(ellipse at 50% 0%, rgba(0, 242, 254, 0.03) 0%, transparent 50%)',
        
        // Aurora gradients
        'aurora-glow': 'linear-gradient(135deg, rgba(0, 242, 254, 0.2) 0%, rgba(138, 43, 226, 0.1) 100%)',
        'aurora-text': 'linear-gradient(135deg, #00F2FE 0%, #8A2BE2 100%)',
        
        // Glass layers
        'glass-70': 'rgba(16, 24, 36, 0.7)',
        'glass-60': 'rgba(16, 24, 36, 0.6)',
        'glass-50': 'rgba(16, 24, 36, 0.5)',
        'glass-40': 'rgba(16, 24, 36, 0.4)',
      },
      
      boxShadow: {
        // Inset shadows for bento edges
        'bento-inset': 'inset 0 1px 1px rgba(255, 255, 255, 0.08)',
        'bento-inset-hover': 'inset 0 1px 1px rgba(255, 255, 255, 0.12)',
        
        // Subtle glow effects (restrained)
        'glow-aurora': '0 0 20px rgba(0, 242, 254, 0.15)',
        'glow-neon': '0 0 20px rgba(138, 43, 226, 0.15)',
        'glow-rise': '0 0 12px rgba(0, 230, 118, 0.25)',
        'glow-fall': '0 0 12px rgba(255, 61, 0, 0.25)',
        
        // Float shadow
        'float': '0 8px 32px rgba(0, 0, 0, 0.4)',
      },
      
      borderRadius: {
        'bento': '20px',
      },
      
      backdropBlur: {
        'bento': '10px',
      },
      
      saturate: {
        'bento': '0.7',
      },
      
      animation: {
        'float': 'float 6s ease-in-out infinite',
        'pulse-subtle': 'pulse-subtle 4s ease-in-out infinite',
        'shimmer': 'shimmer 2s linear infinite',
      },
      
      keyframes: {
        float: {
          '0%, 100%': { transform: 'translateY(0px)' },
          '50%': { transform: 'translateY(-10px)' },
        },
        'pulse-subtle': {
          '0%, 100%': { opacity: '0.4' },
          '50%': { opacity: '0.7' },
        },
        shimmer: {
          '0%': { backgroundPosition: '-200% 0' },
          '100%': { backgroundPosition: '200% 0' },
        },
      },
    },
  },
  plugins: [],
};