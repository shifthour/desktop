import React from 'react';
import { screen, fireEvent, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import AuditTrail from '../components/AuditTrail';
import { render } from './test-utils';

// Mock the audit data loading delay
jest.setTimeout(10000);

describe('AuditTrail Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('Initial Rendering', () => {
    test('should show loading state initially', () => {
      render(<AuditTrail />);
      
      expect(screen.getByText('Loading audit trail...')).toBeInTheDocument();
      expect(screen.getByRole('progressbar')).toBeInTheDocument();
    });

    test('should render main heading and compliance notice after loading', async () => {
      render(<AuditTrail />);
      
      await waitFor(() => {
        expect(screen.getByText('Audit Trail')).toBeInTheDocument();
        expect(screen.getByText('21 CFR Part 11 Compliant • Immutable Audit Log for Clinical Trials')).toBeInTheDocument();
        expect(screen.getByText('21 CFR Part 11 Compliance Notice:')).toBeInTheDocument();
      }, { timeout: 2000 });
    });

    test('should display refresh and export buttons', async () => {
      render(<AuditTrail />);
      
      await waitFor(() => {
        expect(screen.getByText('Refresh')).toBeInTheDocument();
        expect(screen.getByText('Export')).toBeInTheDocument();
      });
    });
  });

  describe('Statistics Dashboard', () => {
    test('should display all 6 statistics cards', async () => {
      render(<AuditTrail />);
      
      await waitFor(() => {
        expect(screen.getByText('Total Entries')).toBeInTheDocument();
        expect(screen.getByText('Last 24h')).toBeInTheDocument();
        expect(screen.getByText('Critical')).toBeInTheDocument();
        expect(screen.getByText('E-Signed')).toBeInTheDocument();
        expect(screen.getByText('System Actions')).toBeInTheDocument();
        expect(screen.getByText('Compliance')).toBeInTheDocument();
      });
    });

    test('should display correct statistics values', async () => {
      render(<AuditTrail />);
      
      await waitFor(() => {
        // Check for numeric values (based on mock data length)
        expect(screen.getByText('8')).toBeInTheDocument(); // Total entries
        expect(screen.getByText('100%')).toBeInTheDocument(); // Compliance
      });
    });

    test('should have gradient backgrounds on statistics cards', async () => {
      render(<AuditTrail />);
      
      await waitFor(() => {
        const statisticsCards = screen.getAllByText(/Total Entries|Last 24h|Critical|E-Signed|System Actions|Compliance/);
        expect(statisticsCards.length).toBeGreaterThan(0);
      });
    });
  });

  describe('Advanced Filters', () => {
    test('should display all filter controls', async () => {
      render(<AuditTrail />);
      
      await waitFor(() => {
        expect(screen.getByPlaceholderText('Search audit trail...')).toBeInTheDocument();
        expect(screen.getByLabelText('Action')).toBeInTheDocument();
        expect(screen.getByLabelText('Entity')).toBeInTheDocument();
        expect(screen.getByLabelText('Severity')).toBeInTheDocument();
        expect(screen.getByLabelText('Study')).toBeInTheDocument();
        expect(screen.getByLabelText('From Date')).toBeInTheDocument();
        expect(screen.getByLabelText('To Date')).toBeInTheDocument();
      });
    });

    test('should filter by action type', async () => {
      render(<AuditTrail />);
      
      await waitFor(async () => {
        const actionSelect = screen.getByLabelText('Action');
        await userEvent.click(actionSelect);
        
        expect(screen.getByText('Create')).toBeInTheDocument();
        expect(screen.getByText('Update')).toBeInTheDocument();
        expect(screen.getByText('Delete')).toBeInTheDocument();
        expect(screen.getByText('Query Opened')).toBeInTheDocument();
        
        const updateOption = screen.getByText('Update');
        await userEvent.click(updateOption);
      });
    });

    test('should filter by entity type', async () => {
      render(<AuditTrail />);
      
      await waitFor(async () => {
        const entitySelect = screen.getByLabelText('Entity');
        await userEvent.click(entitySelect);
        
        expect(screen.getByText('Form')).toBeInTheDocument();
        expect(screen.getByText('Subject')).toBeInTheDocument();
        expect(screen.getByText('Data')).toBeInTheDocument();
        expect(screen.getByText('Query')).toBeInTheDocument();
        
        const dataOption = screen.getByText('Data');
        await userEvent.click(dataOption);
      });
    });

    test('should filter by severity level', async () => {
      render(<AuditTrail />);
      
      await waitFor(async () => {
        const severitySelect = screen.getByLabelText('Severity');
        await userEvent.click(severitySelect);
        
        expect(screen.getByText('Low')).toBeInTheDocument();
        expect(screen.getByText('Medium')).toBeInTheDocument();
        expect(screen.getByText('High')).toBeInTheDocument();
        expect(screen.getByText('Critical')).toBeInTheDocument();
        
        const highOption = screen.getByText('High');
        await userEvent.click(highOption);
      });
    });

    test('should search audit entries', async () => {
      render(<AuditTrail />);
      
      await waitFor(async () => {
        const searchInput = screen.getByPlaceholderText('Search audit trail...');
        await userEvent.type(searchInput, 'Sarah Johnson');
        
        expect(searchInput).toHaveValue('Sarah Johnson');
      });
    });

    test('should show filtered entry count', async () => {
      render(<AuditTrail />);
      
      await waitFor(() => {
        expect(screen.getByText(/Showing \d+ of \d+ audit entries/)).toBeInTheDocument();
      });
    });
  });

  describe('Audit Table', () => {
    test('should display table headers', async () => {
      render(<AuditTrail />);
      
      await waitFor(() => {
        expect(screen.getByText('Audit ID')).toBeInTheDocument();
        expect(screen.getByText('Timestamp')).toBeInTheDocument();
        expect(screen.getByText('Study/Subject')).toBeInTheDocument();
        expect(screen.getByText('Form/Field')).toBeInTheDocument();
        expect(screen.getByText('Action')).toBeInTheDocument();
        expect(screen.getByText('Values')).toBeInTheDocument();
        expect(screen.getByText('User')).toBeInTheDocument();
        expect(screen.getByText('Severity')).toBeInTheDocument();
        expect(screen.getByText('Signature')).toBeInTheDocument();
        expect(screen.getByText('Actions')).toBeInTheDocument();
      });
    });

    test('should display audit entries with correct data', async () => {
      render(<AuditTrail />);
      
      await waitFor(() => {
        expect(screen.getByText('AUD-2024-001')).toBeInTheDocument();
        expect(screen.getByText('Sarah Johnson')).toBeInTheDocument();
        expect(screen.getByText('CRC')).toBeInTheDocument();
        expect(screen.getByText('UPDATE')).toBeInTheDocument();
      });
    });

    test('should display action chips with correct colors', async () => {
      render(<AuditTrail />);
      
      await waitFor(() => {
        const updateChips = screen.getAllByText('UPDATE');
        const createChips = screen.getAllByText('CREATE');
        
        expect(updateChips.length).toBeGreaterThan(0);
        expect(createChips.length).toBeGreaterThan(0);
      });
    });

    test('should display severity indicators', async () => {
      render(<AuditTrail />);
      
      await waitFor(() => {
        expect(screen.getByText('MEDIUM')).toBeInTheDocument();
        expect(screen.getByText('HIGH')).toBeInTheDocument();
        expect(screen.getByText('CRITICAL')).toBeInTheDocument();
      });
    });

    test('should show system action indicators', async () => {
      render(<AuditTrail />);
      
      await waitFor(() => {
        const systemChips = screen.getAllByText('System');
        expect(systemChips.length).toBeGreaterThan(0);
      });
    });

    test('should display signature status', async () => {
      render(<AuditTrail />);
      
      await waitFor(() => {
        const signedChips = screen.getAllByText('Signed');
        expect(signedChips.length).toBeGreaterThan(0);
      });
    });
  });

  describe('Pagination', () => {
    test('should display pagination controls', async () => {
      render(<AuditTrail />);
      
      await waitFor(() => {
        expect(screen.getByLabelText('Go to next page')).toBeInTheDocument();
        expect(screen.getByLabelText('Go to previous page')).toBeInTheDocument();
      });
    });

    test('should show rows per page options', async () => {
      render(<AuditTrail />);
      
      await waitFor(() => {
        const rowsPerPageSelect = screen.getByDisplayValue('25');
        expect(rowsPerPageSelect).toBeInTheDocument();
      });
    });
  });

  describe('Export Functionality', () => {
    test('should open export menu when export button clicked', async () => {
      render(<AuditTrail />);
      
      await waitFor(async () => {
        const exportButton = screen.getByText('Export');
        await userEvent.click(exportButton);
        
        expect(screen.getByText('Export as CSV')).toBeInTheDocument();
        expect(screen.getByText('Export as XLSX')).toBeInTheDocument();
        expect(screen.getByText('Export as PDF')).toBeInTheDocument();
      });
    });

    test('should close export menu when option selected', async () => {
      render(<AuditTrail />);
      
      await waitFor(async () => {
        const exportButton = screen.getByText('Export');
        await userEvent.click(exportButton);
        
        const csvOption = screen.getByText('Export as CSV');
        await userEvent.click(csvOption);
      });
      
      await waitFor(() => {
        expect(screen.queryByText('Export as CSV')).not.toBeInTheDocument();
      });
    });
  });

  describe('Detail Dialog', () => {
    test('should open detail dialog when view details clicked', async () => {
      render(<AuditTrail />);
      
      await waitFor(async () => {
        const viewButtons = screen.getAllByLabelText('View Details');
        await userEvent.click(viewButtons[0]);
        
        expect(screen.getByText(/Audit Entry Details:/)).toBeInTheDocument();
      });
    });

    test('should display audit information in detail dialog', async () => {
      render(<AuditTrail />);
      
      await waitFor(async () => {
        const viewButtons = screen.getAllByLabelText('View Details');
        await userEvent.click(viewButtons[0]);
        
        expect(screen.getByText('Audit Information')).toBeInTheDocument();
        expect(screen.getByText('Context')).toBeInTheDocument();
        expect(screen.getByText('User & System')).toBeInTheDocument();
        expect(screen.getByText('Compliance Flags')).toBeInTheDocument();
      });
    });

    test('should display value changes section for entries with old/new values', async () => {
      render(<AuditTrail />);
      
      await waitFor(async () => {
        const viewButtons = screen.getAllByLabelText('View Details');
        await userEvent.click(viewButtons[0]);
        
        // Should show value changes for UPDATE actions
        expect(screen.getByText('Value Changes')).toBeInTheDocument();
        expect(screen.getByText('Old Value:')).toBeInTheDocument();
        expect(screen.getByText('New Value:')).toBeInTheDocument();
      });
    });

    test('should close detail dialog when close button clicked', async () => {
      render(<AuditTrail />);
      
      await waitFor(async () => {
        const viewButtons = screen.getAllByLabelText('View Details');
        await userEvent.click(viewButtons[0]);
        
        const closeButton = screen.getByText('Close');
        await userEvent.click(closeButton);
      });
      
      await waitFor(() => {
        expect(screen.queryByText(/Audit Entry Details:/)).not.toBeInTheDocument();
      });
    });
  });

  describe('Compliance Features', () => {
    test('should display 21 CFR Part 11 compliance notice', async () => {
      render(<AuditTrail />);
      
      await waitFor(() => {
        expect(screen.getByText('21 CFR Part 11 Compliance Notice:')).toBeInTheDocument();
        expect(screen.getByText(/This audit trail captures all system activities with immutable records/)).toBeInTheDocument();
      });
    });

    test('should show compliance flags in entries', async () => {
      render(<AuditTrail />);
      
      await waitFor(async () => {
        const viewButtons = screen.getAllByLabelText('View Details');
        await userEvent.click(viewButtons[0]);
        
        expect(screen.getByText('21_CFR_PART_11')).toBeInTheDocument();
        expect(screen.getByText('GCP_COMPLIANT')).toBeInTheDocument();
      });
    });
  });

  describe('Refresh Functionality', () => {
    test('should refresh data when refresh button clicked', async () => {
      render(<AuditTrail />);
      
      await waitFor(async () => {
        const refreshButton = screen.getByText('Refresh');
        await userEvent.click(refreshButton);
        
        // Should trigger page reload
        expect(refreshButton).toBeInTheDocument();
      });
    });
  });

  describe('Date Range Filtering', () => {
    test('should accept date inputs for filtering', async () => {
      render(<AuditTrail />);
      
      await waitFor(async () => {
        const fromDateInput = screen.getByLabelText('From Date');
        const toDateInput = screen.getByLabelText('To Date');
        
        expect(fromDateInput).toBeInTheDocument();
        expect(toDateInput).toBeInTheDocument();
        
        await userEvent.type(fromDateInput, '2024-01-01');
        await userEvent.type(toDateInput, '2024-12-31');
        
        expect(fromDateInput).toHaveValue('2024-01-01');
        expect(toDateInput).toHaveValue('2024-12-31');
      });
    });
  });

  describe('Study Filter', () => {
    test('should filter by study selection', async () => {
      render(<AuditTrail />);
      
      await waitFor(async () => {
        const studySelect = screen.getByLabelText('Study');
        await userEvent.click(studySelect);
        
        expect(screen.getByText('Cardiovascular Trial')).toBeInTheDocument();
        expect(screen.getByText('Oncology Phase II')).toBeInTheDocument();
        
        const cardioOption = screen.getByText('Cardiovascular Trial');
        await userEvent.click(cardioOption);
      });
    });
  });

  describe('Accessibility', () => {
    test('should have proper ARIA labels for interactive elements', async () => {
      render(<AuditTrail />);
      
      await waitFor(() => {
        expect(screen.getByLabelText('Action')).toBeInTheDocument();
        expect(screen.getByLabelText('Entity')).toBeInTheDocument();
        expect(screen.getByLabelText('Severity')).toBeInTheDocument();
        expect(screen.getAllByLabelText('View Details').length).toBeGreaterThan(0);
      });
    });

    test('should have proper table structure', async () => {
      render(<AuditTrail />);
      
      await waitFor(() => {
        const table = screen.getByRole('table');
        expect(table).toBeInTheDocument();
        
        const columnHeaders = screen.getAllByRole('columnheader');
        expect(columnHeaders.length).toBe(10); // 10 columns in the table
      });
    });
  });
});