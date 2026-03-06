import React from 'react';
import { screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import Login from '../components/Login';
import { render, mockAxios } from './test-utils';
import axios from 'axios';

// Mock axios
jest.mock('axios');
const mockedAxios = axios as jest.Mocked<typeof axios>;

const mockSetIsAuthenticated = jest.fn();
const mockSetUserData = jest.fn();

const defaultProps = {
  setIsAuthenticated: mockSetIsAuthenticated,
  setUserData: mockSetUserData
};

describe('Login Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockSetIsAuthenticated.mockClear();
    mockSetUserData.mockClear();
    localStorage.clear();
  });

  describe('Initial Rendering', () => {
    test('should render login form with all elements', () => {
      render(<Login {...defaultProps} />);
      
      expect(screen.getByText('EDC Clinical Trials System')).toBeInTheDocument();
      expect(screen.getByText('Phase-based Electronic Data Capture System for Clinical Trials')).toBeInTheDocument();
      expect(screen.getByText('Sign in to your account')).toBeInTheDocument();
      
      expect(screen.getByLabelText('Username')).toBeInTheDocument();
      expect(screen.getByLabelText('Password')).toBeInTheDocument();
      expect(screen.getByRole('button', { name: /sign in/i })).toBeInTheDocument();
    });

    test('should display version information', () => {
      render(<Login {...defaultProps} />);
      
      expect(screen.getByText('Version 1.0.0')).toBeInTheDocument();
      expect(screen.getByText('Last Updated: 2024-09-10')).toBeInTheDocument();
    });

    test('should display study phase cards', () => {
      render(<Login {...defaultProps} />);
      
      expect(screen.getByText('START-UP Phase')).toBeInTheDocument();
      expect(screen.getByText('CONDUCT Phase')).toBeInTheDocument();
      expect(screen.getByText('CLOSE-OUT Phase')).toBeInTheDocument();
    });

    test('should display test credentials', () => {
      render(<Login {...defaultProps} />);
      
      expect(screen.getByText('Test Credentials')).toBeInTheDocument();
      expect(screen.getByText('startup_admin / password123')).toBeInTheDocument();
      expect(screen.getByText('conduct_crc / password123')).toBeInTheDocument();
      expect(screen.getByText('closeout_manager / password123')).toBeInTheDocument();
    });
  });

  describe('Form Validation', () => {
    test('should require username field', async () => {
      render(<Login {...defaultProps} />);
      
      const passwordInput = screen.getByLabelText('Password');
      const signInButton = screen.getByRole('button', { name: /sign in/i });
      
      await userEvent.type(passwordInput, 'password123');
      await userEvent.click(signInButton);
      
      // Form should not submit without username
      expect(mockSetIsAuthenticated).not.toHaveBeenCalled();
    });

    test('should require password field', async () => {
      render(<Login {...defaultProps} />);
      
      const usernameInput = screen.getByLabelText('Username');
      const signInButton = screen.getByRole('button', { name: /sign in/i });
      
      await userEvent.type(usernameInput, 'testuser');
      await userEvent.click(signInButton);
      
      // Form should not submit without password
      expect(mockSetIsAuthenticated).not.toHaveBeenCalled();
    });

    test('should accept valid input', async () => {
      render(<Login {...defaultProps} />);
      
      const usernameInput = screen.getByLabelText('Username');
      const passwordInput = screen.getByLabelText('Password');
      
      await userEvent.type(usernameInput, 'testuser');
      await userEvent.type(passwordInput, 'password123');
      
      expect(usernameInput).toHaveValue('testuser');
      expect(passwordInput).toHaveValue('password123');
    });
  });

  describe('Phase-specific Login', () => {
    test('should login with startup admin credentials', async () => {
      const mockResponse = {
        data: {
          success: true,
          token: 'mock-jwt-token',
          user: {
            id: '1',
            username: 'startup_admin',
            firstName: 'Admin',
            lastName: 'User',
            role: 'Admin',
            phase: 'START_UP',
            permissions: { canCreateForms: true }
          },
          studyData: { studyInfo: { title: 'Test Study' } }
        }
      };

      mockedAxios.post.mockResolvedValueOnce(mockResponse);

      render(<Login {...defaultProps} />);
      
      const usernameInput = screen.getByLabelText('Username');
      const passwordInput = screen.getByLabelText('Password');
      const signInButton = screen.getByRole('button', { name: /sign in/i });
      
      await userEvent.type(usernameInput, 'startup_admin');
      await userEvent.type(passwordInput, 'password123');
      await userEvent.click(signInButton);
      
      await waitFor(() => {
        expect(mockedAxios.post).toHaveBeenCalledWith('/api/auth/login', {
          username: 'startup_admin',
          password: 'password123'
        });
        
        expect(mockSetIsAuthenticated).toHaveBeenCalledWith(true);
        expect(mockSetUserData).toHaveBeenCalledWith(mockResponse.data.user);
      });
    });

    test('should login with conduct CRC credentials', async () => {
      const mockResponse = {
        data: {
          success: true,
          token: 'mock-jwt-token',
          user: {
            id: '2',
            username: 'conduct_crc',
            firstName: 'CRC',
            lastName: 'User',
            role: 'CRC',
            phase: 'CONDUCT',
            permissions: { canEnterData: true }
          },
          studyData: { studyInfo: { title: 'Conduct Study' } }
        }
      };

      mockedAxios.post.mockResolvedValueOnce(mockResponse);

      render(<Login {...defaultProps} />);
      
      const usernameInput = screen.getByLabelText('Username');
      const passwordInput = screen.getByLabelText('Password');
      const signInButton = screen.getByRole('button', { name: /sign in/i });
      
      await userEvent.type(usernameInput, 'conduct_crc');
      await userEvent.type(passwordInput, 'password123');
      await userEvent.click(signInButton);
      
      await waitFor(() => {
        expect(mockedAxios.post).toHaveBeenCalledWith('/api/auth/login', {
          username: 'conduct_crc',
          password: 'password123'
        });
        
        expect(mockSetIsAuthenticated).toHaveBeenCalledWith(true);
        expect(mockSetUserData).toHaveBeenCalledWith(mockResponse.data.user);
      });
    });

    test('should login with closeout manager credentials', async () => {
      const mockResponse = {
        data: {
          success: true,
          token: 'mock-jwt-token',
          user: {
            id: '3',
            username: 'closeout_manager',
            firstName: 'Manager',
            lastName: 'User',
            role: 'Manager',
            phase: 'CLOSE_OUT',
            permissions: { canManageCloseout: true }
          },
          studyData: { studyInfo: { title: 'Closeout Study' } }
        }
      };

      mockedAxios.post.mockResolvedValueOnce(mockResponse);

      render(<Login {...defaultProps} />);
      
      const usernameInput = screen.getByLabelText('Username');
      const passwordInput = screen.getByLabelText('Password');
      const signInButton = screen.getByRole('button', { name: /sign in/i });
      
      await userEvent.type(usernameInput, 'closeout_manager');
      await userEvent.type(passwordInput, 'password123');
      await userEvent.click(signInButton);
      
      await waitFor(() => {
        expect(mockedAxios.post).toHaveBeenCalledWith('/api/auth/login', {
          username: 'closeout_manager',
          password: 'password123'
        });
        
        expect(mockSetIsAuthenticated).toHaveBeenCalledWith(true);
        expect(mockSetUserData).toHaveBeenCalledWith(mockResponse.data.user);
      });
    });
  });

  describe('Error Handling', () => {
    test('should display error message on login failure', async () => {
      const mockError = {
        response: {
          data: {
            message: 'Invalid credentials'
          }
        }
      };

      mockedAxios.post.mockRejectedValueOnce(mockError);

      render(<Login {...defaultProps} />);
      
      const usernameInput = screen.getByLabelText('Username');
      const passwordInput = screen.getByLabelText('Password');
      const signInButton = screen.getByRole('button', { name: /sign in/i });
      
      await userEvent.type(usernameInput, 'invaliduser');
      await userEvent.type(passwordInput, 'wrongpassword');
      await userEvent.click(signInButton);
      
      await waitFor(() => {
        expect(screen.getByText('Invalid credentials')).toBeInTheDocument();
      });
    });

    test('should display generic error message for network errors', async () => {
      mockedAxios.post.mockRejectedValueOnce(new Error('Network Error'));

      render(<Login {...defaultProps} />);
      
      const usernameInput = screen.getByLabelText('Username');
      const passwordInput = screen.getByLabelText('Password');
      const signInButton = screen.getByRole('button', { name: /sign in/i });
      
      await userEvent.type(usernameInput, 'testuser');
      await userEvent.type(passwordInput, 'password123');
      await userEvent.click(signInButton);
      
      await waitFor(() => {
        expect(screen.getByText('Login failed. Please try again.')).toBeInTheDocument();
      });
    });

    test('should clear error message when user types', async () => {
      const mockError = {
        response: {
          data: {
            message: 'Invalid credentials'
          }
        }
      };

      mockedAxios.post.mockRejectedValueOnce(mockError);

      render(<Login {...defaultProps} />);
      
      const usernameInput = screen.getByLabelText('Username');
      const passwordInput = screen.getByLabelText('Password');
      const signInButton = screen.getByRole('button', { name: /sign in/i });
      
      // Trigger error
      await userEvent.type(usernameInput, 'invaliduser');
      await userEvent.type(passwordInput, 'wrongpassword');
      await userEvent.click(signInButton);
      
      await waitFor(() => {
        expect(screen.getByText('Invalid credentials')).toBeInTheDocument();
      });
      
      // Error should clear when user types
      await userEvent.clear(usernameInput);
      await userEvent.type(usernameInput, 'newuser');
      
      await waitFor(() => {
        expect(screen.queryByText('Invalid credentials')).not.toBeInTheDocument();
      });
    });
  });

  describe('Loading State', () => {
    test('should show loading state during login', async () => {
      let resolvePromise: (value: any) => void;
      const loginPromise = new Promise(resolve => {
        resolvePromise = resolve;
      });
      
      mockedAxios.post.mockReturnValueOnce(loginPromise as any);

      render(<Login {...defaultProps} />);
      
      const usernameInput = screen.getByLabelText('Username');
      const passwordInput = screen.getByLabelText('Password');
      const signInButton = screen.getByRole('button', { name: /sign in/i });
      
      await userEvent.type(usernameInput, 'testuser');
      await userEvent.type(passwordInput, 'password123');
      await userEvent.click(signInButton);
      
      // Should show loading state
      expect(screen.getByText('Signing In...')).toBeInTheDocument();
      expect(signInButton).toBeDisabled();
      
      // Resolve the promise
      resolvePromise!({
        data: {
          success: true,
          token: 'token',
          user: { id: '1', username: 'testuser' },
          studyData: {}
        }
      });
      
      await waitFor(() => {
        expect(screen.queryByText('Signing In...')).not.toBeInTheDocument();
      });
    });

    test('should disable form during loading', async () => {
      let resolvePromise: (value: any) => void;
      const loginPromise = new Promise(resolve => {
        resolvePromise = resolve;
      });
      
      mockedAxios.post.mockReturnValueOnce(loginPromise as any);

      render(<Login {...defaultProps} />);
      
      const usernameInput = screen.getByLabelText('Username');
      const passwordInput = screen.getByLabelText('Password');
      const signInButton = screen.getByRole('button', { name: /sign in/i });
      
      await userEvent.type(usernameInput, 'testuser');
      await userEvent.type(passwordInput, 'password123');
      await userEvent.click(signInButton);
      
      // Form fields should be disabled
      expect(usernameInput).toBeDisabled();
      expect(passwordInput).toBeDisabled();
      expect(signInButton).toBeDisabled();
      
      resolvePromise!({
        data: {
          success: true,
          token: 'token',
          user: { id: '1', username: 'testuser' },
          studyData: {}
        }
      });
    });
  });

  describe('LocalStorage Integration', () => {
    test('should store authentication data in localStorage on successful login', async () => {
      const mockResponse = {
        data: {
          success: true,
          token: 'mock-jwt-token',
          user: {
            id: '1',
            username: 'testuser',
            firstName: 'Test',
            lastName: 'User'
          },
          studyData: { studyInfo: { title: 'Test Study' } }
        }
      };

      mockedAxios.post.mockResolvedValueOnce(mockResponse);

      render(<Login {...defaultProps} />);
      
      const usernameInput = screen.getByLabelText('Username');
      const passwordInput = screen.getByLabelText('Password');
      const signInButton = screen.getByRole('button', { name: /sign in/i });
      
      await userEvent.type(usernameInput, 'testuser');
      await userEvent.type(passwordInput, 'password123');
      await userEvent.click(signInButton);
      
      await waitFor(() => {
        expect(localStorage.getItem('token')).toBe('mock-jwt-token');
        expect(localStorage.getItem('user')).toBe(JSON.stringify(mockResponse.data.user));
        expect(localStorage.getItem('studyData')).toBe(JSON.stringify(mockResponse.data.studyData));
      });
    });
  });

  describe('Form Submission', () => {
    test('should handle form submission via Enter key', async () => {
      const mockResponse = {
        data: {
          success: true,
          token: 'mock-jwt-token',
          user: { id: '1', username: 'testuser' },
          studyData: {}
        }
      };

      mockedAxios.post.mockResolvedValueOnce(mockResponse);

      render(<Login {...defaultProps} />);
      
      const usernameInput = screen.getByLabelText('Username');
      const passwordInput = screen.getByLabelText('Password');
      
      await userEvent.type(usernameInput, 'testuser');
      await userEvent.type(passwordInput, 'password123');
      await userEvent.keyboard('{Enter}');
      
      await waitFor(() => {
        expect(mockedAxios.post).toHaveBeenCalled();
      });
    });

    test('should prevent multiple submissions', async () => {
      let resolvePromise: (value: any) => void;
      const loginPromise = new Promise(resolve => {
        resolvePromise = resolve;
      });
      
      mockedAxios.post.mockReturnValueOnce(loginPromise as any);

      render(<Login {...defaultProps} />);
      
      const usernameInput = screen.getByLabelText('Username');
      const passwordInput = screen.getByLabelText('Password');
      const signInButton = screen.getByRole('button', { name: /sign in/i });
      
      await userEvent.type(usernameInput, 'testuser');
      await userEvent.type(passwordInput, 'password123');
      
      // Click multiple times
      await userEvent.click(signInButton);
      await userEvent.click(signInButton);
      await userEvent.click(signInButton);
      
      expect(mockedAxios.post).toHaveBeenCalledTimes(1);
    });
  });

  describe('Password Visibility Toggle', () => {
    test('should toggle password visibility', async () => {
      render(<Login {...defaultProps} />);
      
      const passwordInput = screen.getByLabelText('Password');
      const toggleButton = screen.getByLabelText('toggle password visibility');
      
      // Initially password should be hidden
      expect(passwordInput).toHaveAttribute('type', 'password');
      
      // Toggle to show password
      await userEvent.click(toggleButton);
      expect(passwordInput).toHaveAttribute('type', 'text');
      
      // Toggle back to hide password
      await userEvent.click(toggleButton);
      expect(passwordInput).toHaveAttribute('type', 'password');
    });
  });

  describe('Accessibility', () => {
    test('should have proper form labels and ARIA attributes', () => {
      render(<Login {...defaultProps} />);
      
      expect(screen.getByLabelText('Username')).toBeInTheDocument();
      expect(screen.getByLabelText('Password')).toBeInTheDocument();
      expect(screen.getByRole('button', { name: /sign in/i })).toBeInTheDocument();
    });

    test('should associate error messages with form fields', async () => {
      const mockError = {
        response: {
          data: {
            message: 'Invalid credentials'
          }
        }
      };

      mockedAxios.post.mockRejectedValueOnce(mockError);

      render(<Login {...defaultProps} />);
      
      const usernameInput = screen.getByLabelText('Username');
      const passwordInput = screen.getByLabelText('Password');
      const signInButton = screen.getByRole('button', { name: /sign in/i });
      
      await userEvent.type(usernameInput, 'invalid');
      await userEvent.type(passwordInput, 'wrong');
      await userEvent.click(signInButton);
      
      await waitFor(() => {
        const errorAlert = screen.getByRole('alert');
        expect(errorAlert).toBeInTheDocument();
        expect(errorAlert).toHaveTextContent('Invalid credentials');
      });
    });
  });
});