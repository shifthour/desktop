const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env.local') });

// Initialize Supabase client
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseServiceKey) {
  console.error('Missing Supabase credentials. Please check your .env.local file.');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseServiceKey);

// Read the cleaned accounts data
const accountsData = JSON.parse(
  fs.readFileSync(path.join(__dirname, 'accounts-to-import.json'), 'utf-8')
);

// Get a default company for testing - we'll need to assign accounts to a company
async function getDefaultCompany() {
  const { data, error } = await supabase
    .from('companies')
    .select('id, name')
    .limit(1)
    .single();
  
  if (error) {
    console.error('Error fetching company:', error);
    return null;
  }
  return data;
}

// Get or create default owner (admin user)
async function getDefaultOwner(companyId) {
  const { data, error } = await supabase
    .from('users')
    .select('id, full_name')
    .eq('company_id', companyId)
    .eq('is_admin', true)
    .limit(1)
    .single();
  
  if (error) {
    console.error('Error fetching owner:', error);
    return null;
  }
  return data;
}

// Parse date string from Excel format (DD/MM/YYYY) to ISO format
function parseExcelDate(dateStr) {
  if (!dateStr) return null;
  const parts = dateStr.split('/');
  if (parts.length === 3) {
    const [day, month, year] = parts;
    return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
  }
  return dateStr;
}

// Clean and format phone numbers
function cleanPhone(phone) {
  if (!phone) return null;
  // Remove extra spaces and normalize
  return phone.replace(/\s+/g, ' ').trim();
}

// Map industry names
function mapIndustry(industry) {
  if (!industry) return null;
  const industryMap = {
    'Educational institutions': 'Education',
    'Educational institution': 'Education',
    'Biotech Company': 'Biotechnology',
    'Diagnostics': 'Healthcare',
    'Diagnostic': 'Healthcare',
    'Dairy': 'Food & Beverage',
    'Distillery': 'Food & Beverage',
    'Environmental': 'Environmental Services',
    'Food Testing': 'Food & Beverage',
    'Instrumentation': 'Manufacturing',
    'Research Institute': 'Research'
  };
  return industryMap[industry] || industry;
}

// Import accounts in batches
async function importAccounts() {
  console.log('Starting account import...');
  
  // Get default company
  const company = await getDefaultCompany();
  if (!company) {
    console.error('No company found. Please ensure at least one company exists.');
    return;
  }
  console.log(`Using company: ${company.name} (${company.id})`);
  
  // Get default owner
  const owner = await getDefaultOwner(company.id);
  if (!owner) {
    console.error('No admin user found for the company.');
    return;
  }
  console.log(`Using default owner: ${owner.full_name} (${owner.id})`);
  
  // Prepare accounts for import
  const accountsToImport = [];
  const duplicates = [];
  const errors = [];
  
  for (let i = 0; i < accountsData.length; i++) {
    const account = accountsData[i];
    
    // Skip if no account name
    if (!account.account_name || account.account_name.trim() === '') {
      continue;
    }
    
    try {
      // Check for existing account with same name
      const { data: existing } = await supabase
        .from('accounts')
        .select('id, account_name')
        .eq('company_id', company.id)
        .eq('account_name', account.account_name)
        .single();
      
      if (existing) {
        duplicates.push({
          name: account.account_name,
          existing_id: existing.id
        });
        continue;
      }
      
      // Prepare account data for import
      const accountRecord = {
        company_id: company.id,
        account_name: account.account_name.trim(),
        industry: mapIndustry(account.industry),
        website: account.website,
        phone: cleanPhone(account.billing_phone),
        description: account.description,
        
        // Billing address
        billing_street: account.billing_street || account['Billing Address'],
        billing_city: account.billing_city || account['Billing City'],
        billing_state: account.billing_state || account['Billing State/Province'],
        billing_country: account.billing_country || account['Billing Country'],
        billing_postal_code: account.billing_postal_code || account['Billing Zip/PostalCode'],
        
        // Shipping address
        shipping_street: account['Shipping Address'],
        shipping_city: account['Shipping City'],
        shipping_state: account['Shipping State/Province'],
        shipping_country: account['Shipping Country'],
        shipping_postal_code: account['Shipping Zip/PostalCode'],
        
        // Business info
        turnover_range: account['TurnOver'],
        credit_days: account['Credit Days'] ? parseInt(account['Credit Days']) : null,
        credit_amount: account['Credit Amount'] ? parseFloat(account['Credit Amount']) : 0,
        
        // Tax info
        gstin: account['GSTIN'],
        pan_number: account['PAN No'],
        vat_tin: account['VAT TIN'],
        cst_number: account['CST NO'],
        
        // Metadata
        owner_id: owner.id,
        status: 'Active',
        original_id: account['Sr No']?.toString(),
        original_created_by: account.created_by_name || account['Created By'],
        original_modified_by: account.modified_by_name || account['Last Modified by'],
        original_created_at: parseExcelDate(account['Created By Date']),
        original_modified_at: parseExcelDate(account['Last Modified Date']),
        assigned_to_names: account['AssignTo'],
        
        // Default values
        account_type: 'Customer' // Default type, can be updated later
      };
      
      // Remove null/undefined values
      Object.keys(accountRecord).forEach(key => {
        if (accountRecord[key] === null || accountRecord[key] === undefined || accountRecord[key] === '') {
          delete accountRecord[key];
        }
      });
      
      accountsToImport.push(accountRecord);
      
    } catch (error) {
      errors.push({
        account: account.account_name,
        error: error.message
      });
    }
    
    // Show progress
    if ((i + 1) % 100 === 0) {
      console.log(`Processed ${i + 1}/${accountsData.length} accounts...`);
    }
  }
  
  console.log(`\nReady to import ${accountsToImport.length} accounts`);
  console.log(`Skipped ${duplicates.length} duplicates`);
  console.log(`Encountered ${errors.length} errors`);
  
  // Import in batches of 100
  const batchSize = 100;
  let importedCount = 0;
  let failedCount = 0;
  
  for (let i = 0; i < accountsToImport.length; i += batchSize) {
    const batch = accountsToImport.slice(i, i + batchSize);
    
    const { data, error } = await supabase
      .from('accounts')
      .insert(batch)
      .select();
    
    if (error) {
      console.error(`Error importing batch ${Math.floor(i/batchSize) + 1}:`, error);
      failedCount += batch.length;
    } else {
      importedCount += data.length;
      console.log(`Imported batch ${Math.floor(i/batchSize) + 1}: ${data.length} accounts`);
    }
  }
  
  // Final report
  console.log('\n=== Import Summary ===');
  console.log(`Total accounts processed: ${accountsData.length}`);
  console.log(`Successfully imported: ${importedCount}`);
  console.log(`Failed to import: ${failedCount}`);
  console.log(`Duplicates skipped: ${duplicates.length}`);
  
  if (duplicates.length > 0) {
    console.log('\nFirst 10 duplicates:');
    duplicates.slice(0, 10).forEach(dup => {
      console.log(`  - ${dup.name}`);
    });
  }
  
  if (errors.length > 0) {
    console.log('\nFirst 10 errors:');
    errors.slice(0, 10).forEach(err => {
      console.log(`  - ${err.account}: ${err.error}`);
    });
  }
}

// Run the import
importAccounts().catch(console.error);