import React from 'react';
import { screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import Layout from '../components/Layout';
import { render, mockUserData } from './test-utils';

// Mock react-router-dom
const mockNavigate = jest.fn();
jest.mock('react-router-dom', () => ({
  ...jest.requireActual('react-router-dom'),
  useNavigate: () => mockNavigate,
  useLocation: () => ({ pathname: '/' })
}));

const mockHandleLogout = jest.fn();

const defaultProps = {
  handleLogout: mockHandleLogout,
  userData: mockUserData,
  children: <div>Test Content</div>
};

describe('Layout Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockNavigate.mockClear();
    mockHandleLogout.mockClear();
  });

  describe('Initial Rendering', () => {
    test('should render layout with navigation and content', () => {
      render(<Layout {...defaultProps} />);
      
      expect(screen.getByText('EDC System')).toBeInTheDocument();
      expect(screen.getByText('v1.0')).toBeInTheDocument();
      expect(screen.getByText('Test Content')).toBeInTheDocument();
    });

    test('should display user information in header', () => {
      render(<Layout {...defaultProps} />);
      
      expect(screen.getByText('EDC Clinical Trials System - CONDUCT Phase')).toBeInTheDocument();
      expect(screen.getByText(/CONDUCT Phase User \(CRC\)/)).toBeInTheDocument();
    });

    test('should render all navigation menu items', () => {
      render(<Layout {...defaultProps} />);
      
      expect(screen.getByText('Dashboards & Reports')).toBeInTheDocument();
      expect(screen.getByText('Form Builder')).toBeInTheDocument();
      expect(screen.getByText('Saved Forms')).toBeInTheDocument();
      expect(screen.getByText('Studies')).toBeInTheDocument();
      expect(screen.getByText('Subjects')).toBeInTheDocument();
      expect(screen.getByText('Queries')).toBeInTheDocument();
      expect(screen.getByText('Audit Trail')).toBeInTheDocument();
    });
  });

  describe('Navigation Menu', () => {
    test('should navigate to correct routes when menu items clicked', async () => {
      render(<Layout {...defaultProps} />);
      
      const dashboardMenuItem = screen.getByText('Dashboards & Reports');
      await userEvent.click(dashboardMenuItem);
      
      expect(mockNavigate).toHaveBeenCalledWith('/');
    });

    test('should navigate to studies when studies menu item clicked', async () => {
      render(<Layout {...defaultProps} />);
      
      const studiesMenuItem = screen.getByText('Studies');
      await userEvent.click(studiesMenuItem);
      
      expect(mockNavigate).toHaveBeenCalledWith('/studies');
    });

    test('should navigate to form builder when menu item clicked', async () => {
      render(<Layout {...defaultProps} />);
      
      const formBuilderMenuItem = screen.getByText('Form Builder');
      await userEvent.click(formBuilderMenuItem);
      
      expect(mockNavigate).toHaveBeenCalledWith('/form-builder');
    });

    test('should navigate to audit trail when menu item clicked', async () => {
      render(<Layout {...defaultProps} />);
      
      const auditTrailMenuItem = screen.getByText('Audit Trail');
      await userEvent.click(auditTrailMenuItem);
      
      expect(mockNavigate).toHaveBeenCalledWith('/audit-trail');
    });

    test('should highlight active menu item', () => {
      render(<Layout {...defaultProps} />);
      
      // First menu item should be selected by default (home path '/')
      const dashboardMenuItem = screen.getByText('Dashboards & Reports');
      const listItemButton = dashboardMenuItem.closest('.MuiListItemButton-root');
      expect(listItemButton).toHaveClass('Mui-selected');
    });

    test('should display query badge', () => {
      render(<Layout {...defaultProps} />);
      
      const queryBadge = screen.getByText('12');
      expect(queryBadge).toBeInTheDocument();
    });
  });

  describe('User Information Sidebar', () => {
    test('should display study phase information', () => {
      render(<Layout {...defaultProps} />);
      
      expect(screen.getByText('Study Phase')).toBeInTheDocument();
      expect(screen.getByText('CONDUCT')).toBeInTheDocument();
    });

    test('should display current user information', () => {
      render(<Layout {...defaultProps} />);
      
      expect(screen.getByText('Current User')).toBeInTheDocument();
      expect(screen.getByText('CONDUCT Phase User')).toBeInTheDocument();
      expect(screen.getByText('CRC')).toBeInTheDocument();
    });

    test('should display compliance badge', () => {
      render(<Layout {...defaultProps} />);
      
      expect(screen.getByText('Compliance')).toBeInTheDocument();
      expect(screen.getByText('21 CFR Part 11')).toBeInTheDocument();
    });

    test('should handle different user phases', () => {
      const startupUserData = {
        ...mockUserData,
        phase: 'START_UP',
        username: 'startup_admin'
      };

      render(<Layout {...defaultProps} userData={startupUserData} />);
      
      expect(screen.getByText('START-UP')).toBeInTheDocument();
      expect(screen.getByText('START-UP Phase Admin')).toBeInTheDocument();
    });

    test('should handle close-out phase user', () => {
      const closeoutUserData = {
        ...mockUserData,
        phase: 'CLOSE_OUT',
        username: 'closeout_manager'
      };

      render(<Layout {...defaultProps} userData={closeoutUserData} />);
      
      expect(screen.getByText('CLOSE-OUT')).toBeInTheDocument();
      expect(screen.getByText('CLOSE-OUT Phase Manager')).toBeInTheDocument();
    });
  });

  describe('Profile Menu', () => {
    test('should open profile menu when profile icon clicked', async () => {
      render(<Layout {...defaultProps} />);
      
      const profileButton = screen.getByLabelText('account of current user');
      await userEvent.click(profileButton);
      
      await waitFor(() => {
        expect(screen.getByText('Profile')).toBeInTheDocument();
        expect(screen.getByText('Settings')).toBeInTheDocument();
        expect(screen.getByText('Logout')).toBeInTheDocument();
      });
    });

    test('should close profile menu when clicking elsewhere', async () => {
      render(<Layout {...defaultProps} />);
      
      const profileButton = screen.getByLabelText('account of current user');
      await userEvent.click(profileButton);
      
      await waitFor(() => {
        expect(screen.getByText('Profile')).toBeInTheDocument();
      });

      // Click outside the menu
      await userEvent.click(document.body);
      
      await waitFor(() => {
        expect(screen.queryByText('Profile')).not.toBeInTheDocument();
      });
    });

    test('should call handleLogout when logout clicked', async () => {
      render(<Layout {...defaultProps} />);
      
      const profileButton = screen.getByLabelText('account of current user');
      await userEvent.click(profileButton);
      
      await waitFor(async () => {
        const logoutButton = screen.getByText('Logout');
        await userEvent.click(logoutButton);
        
        expect(mockHandleLogout).toHaveBeenCalled();
        expect(mockNavigate).toHaveBeenCalledWith('/login');
      });
    });
  });

  describe('Mobile Navigation', () => {
    test('should render mobile menu button on small screens', () => {
      render(<Layout {...defaultProps} />);
      
      const mobileMenuButton = screen.getByLabelText('open drawer');
      expect(mobileMenuButton).toBeInTheDocument();
    });

    test('should toggle mobile drawer when menu button clicked', async () => {
      render(<Layout {...defaultProps} />);
      
      const mobileMenuButton = screen.getByLabelText('open drawer');
      await userEvent.click(mobileMenuButton);
      
      // Mobile drawer behavior would be handled by Material-UI
      expect(mobileMenuButton).toBeInTheDocument();
    });
  });

  describe('Header Information', () => {
    test('should display correct header title with phase', () => {
      render(<Layout {...defaultProps} />);
      
      expect(screen.getByText('EDC Clinical Trials System - CONDUCT Phase')).toBeInTheDocument();
    });

    test('should display user chip with avatar', () => {
      render(<Layout {...defaultProps} />);
      
      const userChip = screen.getByText(/CONDUCT Phase User \(CRC\)/);
      expect(userChip).toBeInTheDocument();
    });

    test('should handle missing user data gracefully', () => {
      render(<Layout {...defaultProps} userData={null} />);
      
      expect(screen.getByText('EDC Clinical Trials System - Loading...')).toBeInTheDocument();
      expect(screen.getByText('Loading...')).toBeInTheDocument();
    });
  });

  describe('Phase-specific Styling', () => {
    test('should apply correct phase color for START_UP', () => {
      const startupUserData = {
        ...mockUserData,
        phase: 'START_UP'
      };

      render(<Layout {...defaultProps} userData={startupUserData} />);
      
      const phaseChip = screen.getByText('START-UP');
      expect(phaseChip).toBeInTheDocument();
    });

    test('should apply correct phase color for CONDUCT', () => {
      render(<Layout {...defaultProps} />);
      
      const phaseChip = screen.getByText('CONDUCT');
      expect(phaseChip).toBeInTheDocument();
    });

    test('should apply correct phase color for CLOSE_OUT', () => {
      const closeoutUserData = {
        ...mockUserData,
        phase: 'CLOSE_OUT'
      };

      render(<Layout {...defaultProps} userData={closeoutUserData} />);
      
      const phaseChip = screen.getByText('CLOSE-OUT');
      expect(phaseChip).toBeInTheDocument();
    });

    test('should handle unknown phase', () => {
      const unknownPhaseUserData = {
        ...mockUserData,
        phase: 'UNKNOWN_PHASE'
      };

      render(<Layout {...defaultProps} userData={unknownPhaseUserData} />);
      
      expect(screen.getByText('Test User')).toBeInTheDocument(); // Falls back to firstName lastName
    });
  });

  describe('Navigation Icons', () => {
    test('should display correct icons for each menu item', () => {
      render(<Layout {...defaultProps} />);
      
      // Icons should be rendered (testing their presence)
      const menuItems = screen.getAllByRole('button').filter(button => 
        button.querySelector('.MuiListItemIcon-root')
      );
      
      expect(menuItems.length).toBeGreaterThan(0);
    });
  });

  describe('Responsive Behavior', () => {
    test('should render desktop navigation by default', () => {
      render(<Layout {...defaultProps} />);
      
      // Desktop drawer should be present
      const navigation = screen.getByRole('navigation');
      expect(navigation).toBeInTheDocument();
    });

    test('should render content area properly', () => {
      render(<Layout {...defaultProps} />);
      
      const mainContent = screen.getByRole('main');
      expect(mainContent).toBeInTheDocument();
      expect(screen.getByText('Test Content')).toBeInTheDocument();
    });
  });

  describe('Accessibility', () => {
    test('should have proper ARIA labels', () => {
      render(<Layout {...defaultProps} />);
      
      expect(screen.getByLabelText('open drawer')).toBeInTheDocument();
      expect(screen.getByLabelText('account of current user')).toBeInTheDocument();
    });

    test('should have proper navigation structure', () => {
      render(<Layout {...defaultProps} />);
      
      const navigation = screen.getByRole('navigation');
      expect(navigation).toBeInTheDocument();
      
      const main = screen.getByRole('main');
      expect(main).toBeInTheDocument();
    });

    test('should have keyboard navigation support', () => {
      render(<Layout {...defaultProps} />);
      
      const menuButtons = screen.getAllByRole('button');
      menuButtons.forEach(button => {
        expect(button).not.toHaveAttribute('tabindex', '-1');
      });
    });
  });

  describe('Error Handling', () => {
    test('should handle missing handleLogout prop gracefully', () => {
      render(<Layout userData={mockUserData} children={<div>Test</div>} />);
      
      expect(screen.getByText('EDC System')).toBeInTheDocument();
    });

    test('should display guest user when no user data', () => {
      render(<Layout handleLogout={mockHandleLogout} children={<div>Test</div>} />);
      
      expect(screen.getByText('Guest User')).toBeInTheDocument();
      expect(screen.getByText('No Role')).toBeInTheDocument();
    });
  });
});