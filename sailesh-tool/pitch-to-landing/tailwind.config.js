/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    './app/**/*.{js,ts,jsx,tsx,mdx}',
    './components/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        primary: {
          gold: '#D4A84B',
          navy: '#1a1a5e',
          'dark-navy': '#0d0d3d',
        },
      },
    },
  },
  plugins: [],
}
