// Dynamic import for client-side only
let XLSX: any;

export interface ExportColumn {
  key: string;
  label: string;
  width?: number;
}

export interface ExportOptions {
  filename: string;
  sheetName?: string;
  columns: ExportColumn[];
}

/**
 * Export data to Excel file
 * @param data Array of objects to export
 * @param options Export configuration
 */
export async function exportToExcel(data: any[], options: ExportOptions) {
  try {
    console.log('Starting Excel export...', { dataLength: data.length, options });
    
    // Dynamic import of XLSX for client-side
    if (!XLSX) {
      console.log('Loading XLSX library...');
      XLSX = await import('xlsx');
      console.log('XLSX library loaded successfully');
    }
    
    const { filename, sheetName = 'Sheet1', columns } = options;
    
    if (!data || data.length === 0) {
      console.warn('No data to export');
      return false;
    }
    
    console.log('Data sample:', data[0]);
    
    // Create workbook and worksheet
    const workbook = XLSX.utils.book_new();
    
    // Prepare data for export by mapping to column structure
    const exportData = data.map(item => {
      const row: any = {};
      columns.forEach(column => {
        // Handle nested properties (e.g., 'user.name')
        const value = getNestedValue(item, column.key);
        row[column.label] = value || '';
      });
      return row;
    });
    
    console.log('Export data prepared:', { exportDataLength: exportData.length, sample: exportData[0] });
    
    // Create worksheet from data
    const worksheet = XLSX.utils.json_to_sheet(exportData);
    
    // Set column widths if specified
    const colWidths = columns.map(col => ({ 
      wch: col.width || 15 
    }));
    worksheet['!cols'] = colWidths;
    
    // Add worksheet to workbook
    XLSX.utils.book_append_sheet(workbook, worksheet, sheetName);
    
    // Generate Excel file and download
    const excelBuffer = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });
    const blob = new Blob([excelBuffer], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
    
    // Create download link
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${filename}.xlsx`;
    
    // Trigger download
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    
    // Cleanup
    window.URL.revokeObjectURL(url);
    
    console.log('Excel export completed successfully');
    return true;
  } catch (error) {
    console.error('Error exporting to Excel:', error);
    return false;
  }
}

/**
 * Get nested property value from object
 * @param obj Object to get value from
 * @param path Property path (e.g., 'user.name')
 */
function getNestedValue(obj: any, path: string): any {
  if (!obj || !path) return '';
  
  return path.split('.').reduce((current, key) => {
    return current && current[key] !== undefined ? current[key] : '';
  }, obj);
}

/**
 * Format date for Excel export
 * @param date Date to format
 */
export function formatDateForExcel(date: string | Date): string {
  if (!date) return '';
  
  try {
    const dateObj = typeof date === 'string' ? new Date(date) : date;
    return dateObj.toLocaleDateString();
  } catch {
    return String(date);
  }
}

/**
 * Format currency for Excel export
 * @param amount Amount to format
 * @param currency Currency symbol (default: ₹)
 */
export function formatCurrencyForExcel(amount: number | string, currency: string = '₹'): string {
  if (!amount) return '';
  
  const numAmount = typeof amount === 'string' ? parseFloat(amount) : amount;
  return `${currency}${numAmount.toLocaleString()}`;
}