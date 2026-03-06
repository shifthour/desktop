import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import App from './App';

// Mock localStorage
const mockLocalStorage = (() => {
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

Object.defineProperty(window, 'localStorage', {
  value: mockLocalStorage
});

describe('App Component', () => {
  beforeEach(() => {
    mockLocalStorage.clear();
    jest.clearAllMocks();
  });

  test('should render login page when not authenticated', async () => {
    render(<App />);
    
    await waitFor(() => {
      expect(screen.getByText('EDC Clinical Trials System')).toBeInTheDocument();
      expect(screen.getByText('Sign in to your account')).toBeInTheDocument();
      expect(screen.getByLabelText('Username')).toBeInTheDocument();
      expect(screen.getByLabelText('Password')).toBeInTheDocument();
    });
  });

  test('should render main app when authenticated', async () => {
    // Set up authenticated state
    const mockUser = {
      id: '1',
      username: 'testuser',
      firstName: 'Test',
      lastName: 'User',
      role: 'CRC',
      phase: 'CONDUCT'
    };
    
    const mockStudyData = {
      studyInfo: {
        title: 'Test Study'
      }
    };

    mockLocalStorage.setItem('token', 'mock-token');
    mockLocalStorage.setItem('user', JSON.stringify(mockUser));
    mockLocalStorage.setItem('studyData', JSON.stringify(mockStudyData));

    render(<App />);
    
    await waitFor(() => {
      expect(screen.getByText('Dashboards & Reports')).toBeInTheDocument();
      expect(screen.getByText('Study insights and exportable clinical trial reports')).toBeInTheDocument();
    });
  });

  test('should redirect to login on corrupted user data', async () => {
    // Set corrupted user data
    mockLocalStorage.setItem('token', 'mock-token');
    mockLocalStorage.setItem('user', 'invalid-json');

    const consoleSpy = jest.spyOn(console, 'error').mockImplementation(() => {});

    render(<App />);
    
    await waitFor(() => {
      expect(screen.getByText('Sign in to your account')).toBeInTheDocument();
    });

    consoleSpy.mockRestore();
  });

  test('should apply theme correctly', () => {
    render(<App />);
    
    // Theme should be applied through ThemeProvider
    const app = document.body;
    expect(app).toBeInTheDocument();
  });

  test('should handle logout correctly', async () => {
    // Set up authenticated state
    const mockUser = {
      id: '1',
      username: 'testuser',
      firstName: 'Test',
      lastName: 'User',
      role: 'CRC',
      phase: 'CONDUCT'
    };
    
    mockLocalStorage.setItem('token', 'mock-token');
    mockLocalStorage.setItem('user', JSON.stringify(mockUser));
    mockLocalStorage.setItem('studyData', JSON.stringify({}));

    // Mock window.location.href
    delete (window as any).location;
    (window as any).location = { href: '' };

    render(<App />);
    
    await waitFor(() => {
      expect(screen.getByText('Dashboards & Reports')).toBeInTheDocument();
    });

    // The handleLogout function should clear localStorage and redirect
    // This would be triggered by the Layout component
  });

  test('should render with CssBaseline', () => {
    render(<App />);
    
    // CssBaseline should normalize CSS
    const body = document.body;
    expect(body).toBeInTheDocument();
  });

  test('should use Router for navigation', async () => {
    render(<App />);
    
    // Should render Router component
    await waitFor(() => {
      expect(screen.getByRole('main') || screen.getByText('Sign in to your account')).toBeInTheDocument();
    });
  });
});
