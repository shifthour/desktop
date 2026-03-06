const fs = require('fs');
const path = require('path');

// Generate SQL INSERT statements that can be run directly in Supabase
function generateSQLInsert() {
  console.log('🚀 Generating SQL INSERT statements...');
  
  const accountsData = JSON.parse(
    fs.readFileSync(path.join(__dirname, 'accounts-to-import.json'), 'utf-8')
  );
  
  const companyId = '22adbd06-8ce1-49ea-9a03-d0b46720c624';
  const ownerId = '6dd33b6b-1524-4b77-b8ff-2b4a61aeff8f';
  
  let sqlStatements = [];
  
  // First, disable RLS
  sqlStatements.push('-- Temporarily disable RLS for import');
  sqlStatements.push('ALTER TABLE accounts DISABLE ROW LEVEL SECURITY;');
  sqlStatements.push('');
  
  // Generate INSERT statements in batches of 100
  const batchSize = 100;
  let validAccounts = accountsData.filter(acc => acc.account_name && acc.account_name.trim() !== '');
  
  console.log(`📊 Generating SQL for ${validAccounts.length} accounts...`);
  
  for (let i = 0; i < validAccounts.length; i += batchSize) {
    const batch = validAccounts.slice(i, i + batchSize);
    
    let insertSQL = 'INSERT INTO accounts (\n';
    insertSQL += '  company_id, account_name, industry, website, phone, description,\n';
    insertSQL += '  billing_street, billing_city, billing_state, billing_country, billing_postal_code,\n';
    insertSQL += '  owner_id, status, account_type, original_id, original_created_by\n';
    insertSQL += ') VALUES\n';
    
    const values = batch.map((account, index) => {
      const escapeSql = (str) => {
        if (!str || str === null || str === undefined) return 'NULL';
        return "'" + str.toString().replace(/'/g, "''") + "'";
      };
      
      return `(
    '${companyId}',
    ${escapeSql(account.account_name)},
    ${escapeSql(account.industry)},
    ${escapeSql(account.website)},
    ${escapeSql(account.phone)},
    ${escapeSql(account.description)},
    ${escapeSql(account.billing_street)},
    ${escapeSql(account.billing_city)},
    ${escapeSql(account.billing_state)},
    ${escapeSql(account.billing_country)},
    ${escapeSql(account.billing_postal_code)},
    '${ownerId}',
    'Active',
    'Customer',
    ${escapeSql(account.original_id)},
    ${escapeSql(account.original_created_by)}
  )`;
    }).join(',\n');
    
    insertSQL += values + ';\n\n';
    sqlStatements.push(`-- Batch ${Math.floor(i/batchSize) + 1}: ${batch.length} accounts`);
    sqlStatements.push(insertSQL);
  }
  
  // Re-enable RLS
  sqlStatements.push('-- Re-enable RLS after import');
  sqlStatements.push('ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;');
  sqlStatements.push('');
  sqlStatements.push('-- Check final count');
  sqlStatements.push(`SELECT COUNT(*) as total_accounts FROM accounts WHERE company_id = '${companyId}';`);
  
  // Write to file
  const sqlContent = sqlStatements.join('\n');
  const outputPath = path.join(__dirname, 'accounts-import.sql');
  
  fs.writeFileSync(outputPath, sqlContent);
  
  console.log(`✅ Generated SQL file: ${outputPath}`);
  console.log(`📊 Total accounts: ${validAccounts.length}`);
  console.log(`📝 Batches: ${Math.ceil(validAccounts.length / batchSize)}`);
  
  console.log('\n🎯 Next Steps:');
  console.log('1. Go to Supabase SQL Editor');
  console.log('2. Copy and paste the contents of scripts/accounts-import.sql');
  console.log('3. Click "Run" to execute all statements');
  console.log('4. Check your demo company for 3,899 accounts!');
  
  return outputPath;
}

// Run if called directly
if (require.main === module) {
  generateSQLInsert();
}

module.exports = { generateSQLInsert };