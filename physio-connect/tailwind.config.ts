import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./src/pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/components/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        primary: {
          DEFAULT: "#0D7377",
          light: "#E6F5F5",
          dark: "#095456",
          50: "#F0FAFA",
          100: "#D4F1F1",
          200: "#A8E3E4",
          300: "#5DC4C6",
          400: "#2A9D9F",
          500: "#0D7377",
          600: "#095456",
          700: "#074042",
          800: "#052E30",
          900: "#031C1D",
        },
        coral: {
          DEFAULT: "#E8734A",
          light: "#FEF0EB",
          dark: "#C85A34",
          50: "#FFF5F0",
          100: "#FEE8DE",
          200: "#FDD0BD",
          300: "#F5A88A",
          400: "#EF8C64",
          500: "#E8734A",
          600: "#C85A34",
        },
        sage: {
          DEFAULT: "#7CB69D",
          light: "#EFF7F2",
          dark: "#5A9A7E",
        },
        cream: {
          DEFAULT: "#FFFBF5",
          dark: "#FFF5E8",
        },
        navy: {
          DEFAULT: "#1A2332",
          light: "#2A3A4E",
        },
        warning: {
          DEFAULT: "#F5A623",
          light: "#FEF3E2",
        },
        error: {
          DEFAULT: "#D63031",
          light: "#FDE8E8",
        },
        accent: {
          DEFAULT: "#0D7377",
          light: "#E6F5F5",
        },
      },
      fontFamily: {
        sans: ["Inter", "system-ui", "sans-serif"],
      },
      backgroundImage: {
        'gradient-radial': 'radial-gradient(var(--tw-gradient-stops))',
      },
      animation: {
        'float': 'float 6s ease-in-out infinite',
        'float-delay': 'float 6s ease-in-out 2s infinite',
        'pulse-soft': 'pulse-soft 3s ease-in-out infinite',
        'slide-up': 'slide-up 0.6s ease-out',
        'fade-in': 'fade-in 0.8s ease-out',
      },
      keyframes: {
        float: {
          '0%, 100%': { transform: 'translateY(0px)' },
          '50%': { transform: 'translateY(-20px)' },
        },
        'pulse-soft': {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '0.7' },
        },
        'slide-up': {
          '0%': { transform: 'translateY(30px)', opacity: '0' },
          '100%': { transform: 'translateY(0)', opacity: '1' },
        },
        'fade-in': {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
      },
    },
  },
  plugins: [],
};

export default config;
