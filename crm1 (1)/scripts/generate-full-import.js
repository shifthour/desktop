const fs = require('fs');
const path = require('path');

// Generate the full import SQL with proper formatting
function generateFullImport() {
  const accountsData = JSON.parse(
    fs.readFileSync(path.join(__dirname, 'accounts-to-import.json'), 'utf-8')
  );
  
  const companyId = '22adbd06-8ce1-49ea-9a03-d0b46720c624';
  const ownerId = '45b84401-ca13-4627-ac8f-42a11374633c';
  
  let sqlContent = [];
  
  // Header
  sqlContent.push('-- FULL IMPORT: All 3899 Accounts with Skip Duplicates');
  sqlContent.push('-- Temporarily disable RLS for import');
  sqlContent.push('ALTER TABLE accounts DISABLE ROW LEVEL SECURITY;');
  sqlContent.push('');
  sqlContent.push('-- Check current count before import');
  sqlContent.push(`SELECT COUNT(*) as before_full_import FROM accounts WHERE company_id = '${companyId}';`);
  sqlContent.push('');
  
  // Filter valid accounts
  const validAccounts = accountsData.filter(acc => acc.account_name && acc.account_name.trim() !== '');
  
  console.log(`Generating full import for ${validAccounts.length} accounts...`);
  
  // Process in batches of 50 for better performance
  const batchSize = 50;
  
  for (let i = 0; i < validAccounts.length; i += batchSize) {
    const batch = validAccounts.slice(i, i + batchSize);
    const batchNumber = Math.floor(i / batchSize) + 1;
    
    sqlContent.push(`-- Batch ${batchNumber}: Accounts ${i + 1} to ${Math.min(i + batchSize, validAccounts.length)}`);
    
    let insertSQL = 'INSERT INTO accounts (company_id, account_name, industry, website, phone, description, billing_street, billing_city, billing_state, billing_country, billing_postal_code, owner_id, status, account_type, original_id, original_created_by) VALUES\n';
    
    const values = batch.map(account => {
      const escapeSql = (str) => {
        if (!str || str === null || str === undefined || str === '') return 'NULL';
        return "'" + str.toString().replace(/'/g, "''") + "'";
      };
      
      return `('${companyId}', ${escapeSql(account.account_name)}, ${escapeSql(account.industry)}, ${escapeSql(account.website)}, ${escapeSql(account.phone)}, ${escapeSql(account.description)}, ${escapeSql(account.billing_street)}, ${escapeSql(account.billing_city)}, ${escapeSql(account.billing_state)}, ${escapeSql(account.billing_country)}, ${escapeSql(account.billing_postal_code)}, '${ownerId}', 'Active', 'Customer', ${escapeSql(account.original_id)}, ${escapeSql(account.original_created_by)})`;
    }).join(',\n');
    
    insertSQL += values + '\nON CONFLICT (company_id, account_name) DO NOTHING;\n';
    
    sqlContent.push(insertSQL);
    
    // Progress indicator every 10 batches
    if (batchNumber % 10 === 0) {
      sqlContent.push(`-- Progress: ${batchNumber} batches completed (${Math.min(i + batchSize, validAccounts.length)} accounts processed)`);
      sqlContent.push('');
    }
  }
  
  // Footer
  sqlContent.push('-- Re-enable RLS after import');
  sqlContent.push('ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;');
  sqlContent.push('');
  sqlContent.push('-- Check final count');
  sqlContent.push(`SELECT COUNT(*) as after_full_import FROM accounts WHERE company_id = '${companyId}';`);
  sqlContent.push('');
  sqlContent.push('-- Show sample of imported accounts');
  sqlContent.push(`SELECT account_name, billing_city, billing_state, billing_country FROM accounts WHERE company_id = '${companyId}' ORDER BY account_name LIMIT 20;`);
  sqlContent.push('');
  sqlContent.push('-- Show accounts by industry');
  sqlContent.push(`SELECT industry, COUNT(*) as count FROM accounts WHERE company_id = '${companyId}' AND industry IS NOT NULL GROUP BY industry ORDER BY count DESC;`);
  
  // Write to file
  const outputPath = path.join(__dirname, 'accounts-full-import.sql');
  fs.writeFileSync(outputPath, sqlContent.join('\n'));
  
  console.log(`✅ Generated: ${outputPath}`);
  console.log(`📊 Total valid accounts: ${validAccounts.length}`);
  console.log(`📦 Number of batches: ${Math.ceil(validAccounts.length / batchSize)}`);
  console.log(`🔧 Batch size: ${batchSize} accounts per batch`);
  
  return outputPath;
}

// Run the generator
generateFullImport();