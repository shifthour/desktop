const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');

// Read the Excel file
const filePath = '/Users/safestorage/Desktop/labgigs/errorscreen/report_Account Reportlabgig_.xlsx';
const workbook = XLSX.readFile(filePath);

// Get the first sheet
const sheetName = workbook.SheetNames[0];
const worksheet = workbook.Sheets[sheetName];

// Convert to JSON
const data = XLSX.utils.sheet_to_json(worksheet, { defval: null });

console.log(`Total accounts found: ${data.length}`);
console.log('\n=== Column Analysis ===\n');

// Analyze columns and their data
const columnStats = {};
const sampleData = [];

if (data.length > 0) {
  // Get all columns
  const allColumns = Object.keys(data[0]);
  
  allColumns.forEach(col => {
    columnStats[col] = {
      nonEmptyCount: 0,
      uniqueValues: new Set(),
      samples: []
    };
  });

  // Analyze each row
  data.forEach((row, index) => {
    if (index < 5) {
      sampleData.push(row);
    }
    
    Object.keys(row).forEach(col => {
      if (row[col] !== null && row[col] !== '' && row[col] !== undefined) {
        columnStats[col].nonEmptyCount++;
        columnStats[col].uniqueValues.add(row[col]);
        if (columnStats[col].samples.length < 3) {
          columnStats[col].samples.push(row[col]);
        }
      }
    });
  });

  // Display column analysis
  console.log('Columns with data (non-empty):');
  console.log('--------------------------------');
  
  Object.keys(columnStats)
    .filter(col => columnStats[col].nonEmptyCount > 0)
    .sort((a, b) => columnStats[b].nonEmptyCount - columnStats[a].nonEmptyCount)
    .forEach(col => {
      const stats = columnStats[col];
      const percentage = ((stats.nonEmptyCount / data.length) * 100).toFixed(1);
      console.log(`\n${col}:`);
      console.log(`  - Non-empty: ${stats.nonEmptyCount}/${data.length} (${percentage}%)`);
      console.log(`  - Unique values: ${stats.uniqueValues.size}`);
      console.log(`  - Samples: ${stats.samples.slice(0, 3).join(', ')}`);
    });
  
  console.log('\n\n=== First 5 Records (Sample Data) ===\n');
  console.log(JSON.stringify(sampleData, null, 2));

  // Identify mapping to our CRM structure
  console.log('\n\n=== Suggested CRM Mapping ===\n');
  
  const mapping = {
    'Account Name': 'account_name',
    'Industry': 'industry',
    'Type': 'account_type',
    'Phone': 'phone',
    'Website': 'website',
    'Billing Street': 'billing_street',
    'Billing City': 'billing_city',
    'Billing State': 'billing_state',
    'Billing Country': 'billing_country',
    'Billing Code': 'billing_postal_code',
    'Account Owner': 'owner_name',
    'Created By': 'created_by_name',
    'Modified By': 'modified_by_name',
    'Created Time': 'original_created_at',
    'Modified Time': 'original_updated_at',
    'Description': 'description',
    'Annual Revenue': 'annual_revenue',
    'Employees': 'employee_count'
  };
  
  console.log('Columns that will be imported:');
  Object.keys(mapping).forEach(excelCol => {
    if (columnStats[excelCol] && columnStats[excelCol].nonEmptyCount > 0) {
      console.log(`  ${excelCol} -> ${mapping[excelCol]}`);
    }
  });
  
  // Save cleaned data for import
  const cleanedData = data.map(row => {
    const cleaned = {};
    Object.keys(mapping).forEach(excelCol => {
      if (row[excelCol] !== null && row[excelCol] !== '' && row[excelCol] !== undefined) {
        cleaned[mapping[excelCol]] = row[excelCol];
      }
    });
    return cleaned;
  }).filter(row => row.account_name); // Only keep rows with account name
  
  // Save to JSON for later import
  fs.writeFileSync(
    path.join(__dirname, 'accounts-to-import.json'),
    JSON.stringify(cleanedData, null, 2)
  );
  
  console.log(`\n\nCleaned data saved to accounts-to-import.json`);
  console.log(`Total accounts to import: ${cleanedData.length}`);
}