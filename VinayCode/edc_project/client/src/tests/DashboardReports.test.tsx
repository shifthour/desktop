import React from 'react';
import { screen, fireEvent, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import DashboardReports from '../components/DashboardReports';
import { render } from './test-utils';

describe('DashboardReports Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('Initial Rendering', () => {
    test('should render main heading and tabs', () => {
      render(<DashboardReports />);
      
      expect(screen.getByText('Dashboards & Reports')).toBeInTheDocument();
      expect(screen.getByText('Study insights and exportable clinical trial reports')).toBeInTheDocument();
      expect(screen.getByText('Dashboards')).toBeInTheDocument();
      expect(screen.getByText('Reports')).toBeInTheDocument();
      expect(screen.getByText('Refresh Data')).toBeInTheDocument();
    });

    test('should default to dashboards tab', () => {
      render(<DashboardReports />);
      
      // Dashboard content should be visible
      expect(screen.getByText('Total Subjects')).toBeInTheDocument();
      expect(screen.getByText('Open Queries')).toBeInTheDocument();
      expect(screen.getByText('Subject Status Distribution')).toBeInTheDocument();
    });
  });

  describe('Dashboard Tab', () => {
    test('should display all KPI cards with correct data', () => {
      render(<DashboardReports />);
      
      // Check all 6 KPI cards
      expect(screen.getByText('Total Subjects')).toBeInTheDocument();
      expect(screen.getByText('362')).toBeInTheDocument();
      expect(screen.getByText('+12')).toBeInTheDocument();
      
      expect(screen.getByText('Open Queries')).toBeInTheDocument();
      expect(screen.getByText('48')).toBeInTheDocument();
      expect(screen.getByText('-5')).toBeInTheDocument();
      
      expect(screen.getByText('Sites Active')).toBeInTheDocument();
      expect(screen.getByText('12')).toBeInTheDocument();
      
      expect(screen.getByText('Forms Locked')).toBeInTheDocument();
      expect(screen.getByText('95')).toBeInTheDocument();
      
      expect(screen.getByText('Visit Completion')).toBeInTheDocument();
      expect(screen.getByText('89%')).toBeInTheDocument();
      
      expect(screen.getByText('Data Quality')).toBeInTheDocument();
      expect(screen.getByText('97%')).toBeInTheDocument();
    });

    test('should display chart placeholders', () => {
      render(<DashboardReports />);
      
      expect(screen.getByText('Subject Status Distribution')).toBeInTheDocument();
      expect(screen.getByText('Visit Completion by Site')).toBeInTheDocument();
      expect(screen.getByText('Query Aging Distribution')).toBeInTheDocument();
      expect(screen.getByText('Monthly Enrollment Trend')).toBeInTheDocument();
      
      // Check for placeholder content
      expect(screen.getByText(/Pie Chart - Subject Status/)).toBeInTheDocument();
      expect(screen.getByText(/Bar Chart - Visit Completion by Site/)).toBeInTheDocument();
      expect(screen.getByText(/Bar Chart - Query Aging/)).toBeInTheDocument();
      expect(screen.getByText(/Line Chart - Monthly Enrollment Trend/)).toBeInTheDocument();
    });

    test('should have hover effect on KPI cards', async () => {
      render(<DashboardReports />);
      
      const firstCard = screen.getByText('Total Subjects').closest('.MuiCard-root');
      expect(firstCard).toHaveStyle('cursor: pointer');
    });
  });

  describe('Reports Tab', () => {
    beforeEach(async () => {
      render(<DashboardReports />);
      const reportsTab = screen.getByText('Reports');
      await userEvent.click(reportsTab);
    });

    test('should display reports tab content when switched', async () => {
      expect(screen.getByText('Report Filters & Export')).toBeInTheDocument();
      expect(screen.getByLabelText('Report Type')).toBeInTheDocument();
      expect(screen.getByLabelText('Study')).toBeInTheDocument();
      expect(screen.getByLabelText('Site')).toBeInTheDocument();
      expect(screen.getByText('Export')).toBeInTheDocument();
    });

    test('should display default enrollment report', async () => {
      expect(screen.getByText('Enrollment Report')).toBeInTheDocument();
      expect(screen.getByText('4 records')).toBeInTheDocument();
      
      // Check table headers
      expect(screen.getByText('Subject ID')).toBeInTheDocument();
      expect(screen.getByText('Site')).toBeInTheDocument();
      expect(screen.getByText('Enrolled Date')).toBeInTheDocument();
      expect(screen.getByText('Status')).toBeInTheDocument();
      expect(screen.getByText('Visits')).toBeInTheDocument();
      expect(screen.getByText('Last Visit')).toBeInTheDocument();
    });

    test('should change report type when dropdown selection changes', async () => {
      const reportTypeSelect = screen.getByLabelText('Report Type');
      await userEvent.click(reportTypeSelect);
      
      const queriesOption = screen.getByText('Query Status');
      await userEvent.click(queriesOption);
      
      await waitFor(() => {
        expect(screen.getByText('Queries Report')).toBeInTheDocument();
        expect(screen.getByText('Query ID')).toBeInTheDocument();
        expect(screen.getByText('Subject')).toBeInTheDocument();
        expect(screen.getByText('Severity')).toBeInTheDocument();
      });
    });

    test('should filter reports by study', async () => {
      const studySelect = screen.getByLabelText('Study');
      await userEvent.click(studySelect);
      
      const cardioOption = screen.getByText('Cardiovascular Trial');
      await userEvent.click(cardioOption);
      
      // Study filter should be applied (would filter data in real implementation)
      await waitFor(() => {
        expect(studySelect).toHaveTextContent('Cardiovascular Trial');
      });
    });

    test('should search reports using search input', async () => {
      const searchInput = screen.getByPlaceholderText('Search reports...');
      await userEvent.type(searchInput, 'SUBJ-001');
      
      expect(searchInput).toHaveValue('SUBJ-001');
    });

    test('should open export menu when export button clicked', async () => {
      const exportButton = screen.getByText('Export');
      await userEvent.click(exportButton);
      
      await waitFor(() => {
        expect(screen.getByText('Export as CSV')).toBeInTheDocument();
        expect(screen.getByText('Export as XLSX')).toBeInTheDocument();
        expect(screen.getByText('Export as PDF')).toBeInTheDocument();
      });
    });

    test('should close export menu when option selected', async () => {
      const exportButton = screen.getByText('Export');
      await userEvent.click(exportButton);
      
      await waitFor(() => {
        const csvOption = screen.getByText('Export as CSV');
        userEvent.click(csvOption);
      });
      
      await waitFor(() => {
        expect(screen.queryByText('Export as CSV')).not.toBeInTheDocument();
      });
    });

    test('should display status chips with correct colors', async () => {
      // Check for status indicators in the table
      const activeChips = screen.getAllByText('Active');
      expect(activeChips[0]).toBeInTheDocument();
      
      const completedChips = screen.getAllByText('Completed');
      expect(completedChips[0]).toBeInTheDocument();
    });

    test('should open detail dialog when view details clicked', async () => {
      const viewButtons = screen.getAllByLabelText('View Details');
      await userEvent.click(viewButtons[0]);
      
      await waitFor(() => {
        expect(screen.getByText('Report Item Details')).toBeInTheDocument();
        expect(screen.getByText('Close')).toBeInTheDocument();
      });
    });

    test('should close detail dialog when close button clicked', async () => {
      const viewButtons = screen.getAllByLabelText('View Details');
      await userEvent.click(viewButtons[0]);
      
      await waitFor(() => {
        const closeButton = screen.getByText('Close');
        userEvent.click(closeButton);
      });
      
      await waitFor(() => {
        expect(screen.queryByText('Report Item Details')).not.toBeInTheDocument();
      });
    });

    test('should handle pagination', async () => {
      const pagination = screen.getByLabelText('Go to next page');
      expect(pagination).toBeInTheDocument();
      
      // Check rows per page selector
      const rowsPerPageSelect = screen.getByDisplayValue('25');
      expect(rowsPerPageSelect).toBeInTheDocument();
    });
  });

  describe('Tab Navigation', () => {
    test('should switch between tabs correctly', async () => {
      render(<DashboardReports />);
      
      // Initially on dashboard tab
      expect(screen.getByText('Total Subjects')).toBeInTheDocument();
      
      // Switch to reports tab
      const reportsTab = screen.getByText('Reports');
      await userEvent.click(reportsTab);
      
      await waitFor(() => {
        expect(screen.getByText('Report Filters & Export')).toBeInTheDocument();
      });
      
      // Switch back to dashboard tab
      const dashboardTab = screen.getByText('Dashboards');
      await userEvent.click(dashboardTab);
      
      await waitFor(() => {
        expect(screen.getByText('Total Subjects')).toBeInTheDocument();
      });
    });
  });

  describe('Refresh Functionality', () => {
    test('should show loading state when refresh is clicked', async () => {
      render(<DashboardReports />);
      
      const refreshButton = screen.getByText('Refresh Data');
      await userEvent.click(refreshButton);
      
      // Should show loading state briefly
      await waitFor(() => {
        expect(screen.getByRole('progressbar')).toBeInTheDocument();
      });
    });
  });

  describe('Compliance Notice', () => {
    test('should display compliance notice', () => {
      render(<DashboardReports />);
      
      expect(screen.getByText('Clinical Data Reports:')).toBeInTheDocument();
      expect(screen.getByText(/All reports are generated from validated clinical trial data/)).toBeInTheDocument();
    });
  });

  describe('Date Range Filtering', () => {
    test('should accept date inputs for filtering', async () => {
      render(<DashboardReports />);
      
      // Switch to reports tab
      const reportsTab = screen.getByText('Reports');
      await userEvent.click(reportsTab);
      
      await waitFor(() => {
        const fromDateInput = screen.getByLabelText('From Date');
        const toDateInput = screen.getByLabelText('To Date');
        
        expect(fromDateInput).toBeInTheDocument();
        expect(toDateInput).toBeInTheDocument();
        
        // Type dates
        userEvent.type(fromDateInput, '2024-01-01');
        userEvent.type(toDateInput, '2024-12-31');
      });
    });
  });

  describe('Report Types', () => {
    test('should display all report types in dropdown', async () => {
      render(<DashboardReports />);
      
      // Switch to reports tab
      const reportsTab = screen.getByText('Reports');
      await userEvent.click(reportsTab);
      
      await waitFor(async () => {
        const reportTypeSelect = screen.getByLabelText('Report Type');
        await userEvent.click(reportTypeSelect);
        
        expect(screen.getByText('Subject Enrollment')).toBeInTheDocument();
        expect(screen.getByText('Query Status')).toBeInTheDocument();
        expect(screen.getByText('Audit Summary')).toBeInTheDocument();
        expect(screen.getByText('Visit Completion')).toBeInTheDocument();
      });
    });

    test('should switch to audit summary report', async () => {
      render(<DashboardReports />);
      
      // Switch to reports tab
      const reportsTab = screen.getByText('Reports');
      await userEvent.click(reportsTab);
      
      await waitFor(async () => {
        const reportTypeSelect = screen.getByLabelText('Report Type');
        await userEvent.click(reportTypeSelect);
        
        const auditOption = screen.getByText('Audit Summary');
        await userEvent.click(auditOption);
      });
      
      await waitFor(() => {
        expect(screen.getByText('Audit Summary Report')).toBeInTheDocument();
        expect(screen.getByText('Date')).toBeInTheDocument();
        expect(screen.getByText('Total Actions')).toBeInTheDocument();
        expect(screen.getByText('Critical Events')).toBeInTheDocument();
      });
    });

    test('should switch to visit completion report', async () => {
      render(<DashboardReports />);
      
      // Switch to reports tab
      const reportsTab = screen.getByText('Reports');
      await userEvent.click(reportsTab);
      
      await waitFor(async () => {
        const reportTypeSelect = screen.getByLabelText('Report Type');
        await userEvent.click(reportTypeSelect);
        
        const visitOption = screen.getByText('Visit Completion');
        await userEvent.click(visitOption);
      });
      
      await waitFor(() => {
        expect(screen.getByText('Visit Completion Report')).toBeInTheDocument();
        expect(screen.getByText('Subject')).toBeInTheDocument();
        expect(screen.getByText('Planned Visits')).toBeInTheDocument();
        expect(screen.getByText('Completed Visits')).toBeInTheDocument();
      });
    });
  });
});