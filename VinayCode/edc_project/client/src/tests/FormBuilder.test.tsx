import React from 'react';
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import FormBuilder from '../components/FormBuilder';
import { ThemeProvider, createTheme } from '@mui/material/styles';

const theme = createTheme();

const renderWithTheme = (component: React.ReactElement) => {
  return render(
    <ThemeProvider theme={theme}>
      {component}
    </ThemeProvider>
  );
};

describe('FormBuilder Component', () => {
  describe('Form Properties', () => {
    test('should render form builder with initial form name and version', () => {
      renderWithTheme(<FormBuilder />);
      
      expect(screen.getByText('Form Builder')).toBeInTheDocument();
      expect(screen.getByDisplayValue('Patient Demographics Form')).toBeInTheDocument();
      expect(screen.getByDisplayValue('1.0.0')).toBeInTheDocument();
    });

    test('should update form name when changed', async () => {
      renderWithTheme(<FormBuilder />);
      
      const formNameInput = screen.getByDisplayValue('Patient Demographics Form');
      await userEvent.clear(formNameInput);
      await userEvent.type(formNameInput, 'New Form Name');
      
      expect(formNameInput).toHaveValue('New Form Name');
    });

    test('should update form version when changed', async () => {
      renderWithTheme(<FormBuilder />);
      
      const versionInput = screen.getByDisplayValue('1.0.0');
      await userEvent.clear(versionInput);
      await userEvent.type(versionInput, '2.0.0');
      
      expect(versionInput).toHaveValue('2.0.0');
    });

    test('should change form status', async () => {
      renderWithTheme(<FormBuilder />);
      
      const statusChip = screen.getByText('DRAFT');
      expect(statusChip).toBeInTheDocument();
      
      // Status should be changeable through dropdown
      const statusSelect = screen.getByLabelText('Status');
      fireEvent.mouseDown(statusSelect);
      
      const uatOption = await screen.findByText('UAT');
      fireEvent.click(uatOption);
      
      await waitFor(() => {
        expect(screen.getByText('UAT')).toBeInTheDocument();
      });
    });
  });

  describe('Section Management', () => {
    test('should display initial sections', () => {
      renderWithTheme(<FormBuilder />);
      
      expect(screen.getByText('Section 1: Basic Information')).toBeInTheDocument();
      expect(screen.getByText('Section 2: Vital Signs')).toBeInTheDocument();
    });

    test('should add new section when button clicked', async () => {
      renderWithTheme(<FormBuilder />);
      
      const addSectionButton = screen.getByText('Add New Section');
      fireEvent.click(addSectionButton);
      
      await waitFor(() => {
        expect(screen.getByText(/New Section/)).toBeInTheDocument();
      });
    });

    test('should display field count for each section', () => {
      renderWithTheme(<FormBuilder />);
      
      const fieldCounts = screen.getAllByText(/fields$/);
      expect(fieldCounts[0]).toHaveTextContent('3 fields');
      expect(fieldCounts[1]).toHaveTextContent('2 fields');
    });
  });

  describe('Field Management', () => {
    test('should display all field types in sidebar', () => {
      renderWithTheme(<FormBuilder />);
      
      expect(screen.getByText('Text Field')).toBeInTheDocument();
      expect(screen.getByText('Number')).toBeInTheDocument();
      expect(screen.getByText('Date')).toBeInTheDocument();
      expect(screen.getByText('Dropdown')).toBeInTheDocument();
      expect(screen.getByText('Radio Button')).toBeInTheDocument();
      expect(screen.getByText('Checkbox')).toBeInTheDocument();
      expect(screen.getByText('Text Area')).toBeInTheDocument();
      expect(screen.getByText('File Upload')).toBeInTheDocument();
      expect(screen.getByText('Calculated Field')).toBeInTheDocument();
    });

    test('should add new text field to section', async () => {
      renderWithTheme(<FormBuilder />);
      
      // Expand first section if needed
      const firstSection = screen.getByText('Section 1: Basic Information');
      fireEvent.click(firstSection);
      
      // Find the Add Text Field button within the section
      const addButtons = screen.getAllByText(/Add Text Field/);
      fireEvent.click(addButtons[0]);
      
      await waitFor(() => {
        expect(screen.getByText(/New text field/)).toBeInTheDocument();
      });
    });

    test('should delete field when delete button clicked', async () => {
      renderWithTheme(<FormBuilder />);
      
      // Expand first section
      const firstSection = screen.getByText('Section 1: Basic Information');
      fireEvent.click(firstSection);
      
      // Find delete buttons
      const deleteButtons = screen.getAllByTestId('DeleteIcon');
      const initialFieldCount = deleteButtons.length;
      
      // Click first delete button
      fireEvent.click(deleteButtons[0]);
      
      await waitFor(() => {
        const newDeleteButtons = screen.getAllByTestId('DeleteIcon');
        expect(newDeleteButtons.length).toBe(initialFieldCount - 1);
      });
    });

    test('should open field configuration dialog when edit clicked', async () => {
      renderWithTheme(<FormBuilder />);
      
      // Expand first section
      const firstSection = screen.getByText('Section 1: Basic Information');
      fireEvent.click(firstSection);
      
      // Find edit buttons
      const editButtons = screen.getAllByTestId('EditIcon');
      fireEvent.click(editButtons[0]);
      
      await waitFor(() => {
        expect(screen.getByText(/Configure Field:/)).toBeInTheDocument();
        expect(screen.getByLabelText('Field Label')).toBeInTheDocument();
        expect(screen.getByLabelText('Field Name (Database)')).toBeInTheDocument();
      });
    });
  });

  describe('Field Configuration', () => {
    test('should update field properties in configuration dialog', async () => {
      renderWithTheme(<FormBuilder />);
      
      // Open configuration dialog
      const firstSection = screen.getByText('Section 1: Basic Information');
      fireEvent.click(firstSection);
      
      const editButtons = screen.getAllByTestId('EditIcon');
      fireEvent.click(editButtons[0]);
      
      await waitFor(() => {
        expect(screen.getByText(/Configure Field:/)).toBeInTheDocument();
      });
    });
  });
});
