/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // YSRCP Party Colors
        ysrcp: {
          primary: '#0066CC',
          secondary: '#004999',
          light: '#3399FF',
          dark: '#003366',
        },
        // TDP Party Colors
        tdp: {
          primary: '#FFD700',
          secondary: '#FFC000',
          light: '#FFE44D',
          dark: '#CC9900',
        },
        // Dashboard Theme
        dashboard: {
          bg: '#0f0f1a',
          card: '#1a1a2e',
          cardHover: '#252542',
          border: '#2a2a4a',
          text: '#e4e4e7',
          textMuted: '#9ca3af',
        },
        // Sentiment Colors
        sentiment: {
          positive: '#10B981',
          negative: '#EF4444',
          neutral: '#6B7280',
        }
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
      },
      animation: {
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'slide-up': 'slideUp 0.3s ease-out',
        'fade-in': 'fadeIn 0.5s ease-out',
      },
      keyframes: {
        slideUp: {
          '0%': { transform: 'translateY(10px)', opacity: '0' },
          '100%': { transform: 'translateY(0)', opacity: '1' },
        },
        fadeIn: {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
      },
    },
  },
  plugins: [],
}
