/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        primary: {
          50: '#e6f7f7',
          100: '#b3e8e8',
          200: '#80d9d9',
          300: '#4dcaca',
          400: '#1abbbb',
          500: '#16a3a3',
          600: '#128080',
          700: '#0e5d5d',
          800: '#0a3a3a',
          900: '#061717',
        },
      },
    },
  },
  plugins: [],
}
