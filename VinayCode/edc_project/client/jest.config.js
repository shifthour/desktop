module.exports = {
  // Use Create React App's default configuration
  testEnvironment: 'jsdom',
  setupFilesAfterEnv: ['<rootDir>/src/setupTests.ts'],
  moduleNameMapping: {
    '\\.(css|less|scss|sass)$': 'identity-obj-proxy',
  },
  collectCoverageFrom: [
    'src/**/*.{js,jsx,ts,tsx}',
    '!src/**/*.d.ts',
    '!src/index.tsx',
    '!src/serviceWorker.ts',
    '!src/reportWebVitals.ts',
    '!src/setupTests.ts',
    '!src/react-app-env.d.ts',
  ],
  coverageReporters: [
    'text',
    'text-summary', 
    'html',
    'lcov',
    'json'
  ],
  coverageThreshold: {
    global: {
      branches: 85,
      functions: 90,
      lines: 90,
      statements: 90,
    },
    './src/components/': {
      branches: 80,
      functions: 85,
      lines: 85,
      statements: 85,
    }
  },
  testMatch: [
    '<rootDir>/src/**/__tests__/**/*.{js,jsx,ts,tsx}',
    '<rootDir>/src/**/*.(test|spec).{js,jsx,ts,tsx}',
    '<rootDir>/src/tests/**/*.{js,jsx,ts,tsx}'
  ],
  testPathIgnorePatterns: [
    '/node_modules/',
    '/build/',
    '/coverage/',
  ],
  verbose: true,
  bail: false,
  maxWorkers: '50%',
  testTimeout: 10000
};