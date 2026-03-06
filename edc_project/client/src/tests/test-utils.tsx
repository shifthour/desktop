import React from 'react';
import { render, RenderOptions } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { BrowserRouter } from 'react-router-dom';
import CssBaseline from '@mui/material/CssBaseline';

const theme = createTheme({
  palette: {
    primary: {
      main: '#1976d2',
    },
    secondary: {
      main: '#dc004e',
    },
    background: {
      default: '#f5f5f5',
    },
  },
});

interface AllTheProvidersProps {
  children: React.ReactNode;
}

const AllTheProviders: React.FC<AllTheProvidersProps> = ({ children }) => {
  return (
    <BrowserRouter>
      <ThemeProvider theme={theme}>
        <CssBaseline />
        {children}
      </ThemeProvider>
    </BrowserRouter>
  );
};

const customRender = (
  ui: React.ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
) => render(ui, { wrapper: AllTheProviders, ...options });

export * from '@testing-library/react';
export { customRender as render };

// Mock data for tests
export const mockUserData = {
  id: '1',
  username: 'testuser',
  firstName: 'Test',
  lastName: 'User',
  role: 'CRC',
  phase: 'CONDUCT',
  permissions: {
    canCreateForms: true,
    canEditForms: true,
    canDeleteForms: false,
    canManageUsers: false,
    canViewAuditTrail: true
  }
};

export const mockStudyData = {
  studyInfo: {
    protocolNumber: 'TEST-001',
    title: 'Test Clinical Trial',
    status: 'ACTIVE',
    currentEnrollment: 25,
    projectedEnrollment: 100
  },
  metrics: {
    totalSubjects: 25,
    completedVisits: 120,
    openQueries: 5,
    lockedForms: 15
  },
  sites: [
    {
      id: 'SITE-001',
      name: 'Test Medical Center',
      status: 'ACTIVE',
      subjects: 12
    },
    {
      id: 'SITE-002', 
      name: 'University Hospital',
      status: 'ACTIVE',
      subjects: 13
    }
  ]
};

export const mockAuditEntries = [
  {
    id: '1',
    auditId: 'AUD-TEST-001',
    studyId: 'TEST-001',
    studyName: 'Test Clinical Trial',
    subjectId: 'SUBJ-001',
    actionType: 'UPDATE' as const,
    oldValue: '120',
    newValue: '125',
    user: 'Test User',
    userId: 'USR-001',
    role: 'CRC',
    timestamp: '2024-09-10T14:32:15.123Z',
    reasonForChange: 'Test data correction',
    ipAddress: '192.168.1.100',
    sessionId: 'SES-TEST-001',
    signature: true,
    isSystemAction: false,
    severity: 'MEDIUM' as const,
    entityType: 'DATA' as const,
    entityId: 'SUBJ-001-VS-BASELINE',
    complianceFlags: ['21_CFR_PART_11', 'GCP_COMPLIANT']
  }
];

// Mock localStorage
export const mockLocalStorage = (() => {
  let store: { [key: string]: string } = {};

  return {
    getItem: (key: string) => store[key] || null,
    setItem: (key: string, value: string) => {
      store[key] = value.toString();
    },
    removeItem: (key: string) => {
      delete store[key];
    },
    clear: () => {
      store = {};
    }
  };
})();

// Setup mock localStorage before tests
beforeEach(() => {
  Object.defineProperty(window, 'localStorage', {
    value: mockLocalStorage
  });
  
  // Set default test data in localStorage
  mockLocalStorage.setItem('user', JSON.stringify(mockUserData));
  mockLocalStorage.setItem('studyData', JSON.stringify(mockStudyData));
  mockLocalStorage.setItem('token', 'mock-jwt-token');
});

// Mock axios for API calls
export const mockAxios = {
  get: jest.fn(() => Promise.resolve({ data: {} })),
  post: jest.fn(() => Promise.resolve({ data: {} })),
  put: jest.fn(() => Promise.resolve({ data: {} })),
  delete: jest.fn(() => Promise.resolve({ data: {} }))
};

// Mock console methods to avoid noise in tests
global.console = {
  ...console,
  log: jest.fn(),
  error: jest.fn(),
  warn: jest.fn(),
  info: jest.fn()
};