const fs = require('fs');
const path = require('path');

// Simple API-based import that works with authentication
async function simpleImport() {
  console.log('🚀 Starting simple API-based import...');
  
  // Read the cleaned data
  const accountsData = JSON.parse(
    fs.readFileSync(path.join(__dirname, 'accounts-to-import.json'), 'utf-8')
  );
  
  const companyId = '22adbd06-8ce1-49ea-9a03-d0b46720c624'; // Demo company
  const ownerId = '6dd33b6b-1524-4b77-b8ff-2b4a61aeff8f'; // Demo admin
  
  console.log(`📊 Processing ${accountsData.length} accounts...`);
  
  let successCount = 0;
  let errorCount = 0;
  const errors = [];
  
  // Import accounts one by one via API (slower but reliable)
  for (let i = 0; i < Math.min(accountsData.length, 100); i++) { // Start with first 100 as test
    const account = accountsData[i];
    
    if (!account.account_name || account.account_name.trim() === '') {
      continue;
    }
    
    const accountData = {
      companyId: companyId,
      account_name: account.account_name.trim(),
      industry: account.industry || null,
      website: account.website || null,
      phone: account.phone || null,
      description: account.description || null,
      billing_street: account.billing_street || null,
      billing_city: account.billing_city || null,
      billing_state: account.billing_state || null,
      billing_country: account.billing_country || null,
      billing_postal_code: account.billing_postal_code || null,
      owner_id: ownerId,
      status: 'Active',
      account_type: 'Customer'
    };
    
    try {
      const response = await fetch('http://localhost:3000/api/accounts', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(accountData)
      });
      
      if (response.ok) {
        successCount++;
        if (successCount % 10 === 0) {
          console.log(`✅ Imported ${successCount} accounts...`);
        }
      } else {
        const error = await response.json();
        errorCount++;
        if (error.error && !error.error.includes('already exists')) {
          errors.push({ account: account.account_name, error: error.error });
        }
      }
    } catch (err) {
      errorCount++;
      errors.push({ account: account.account_name, error: err.message });
    }
    
    // Small delay to avoid overwhelming the API
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  
  console.log('\n🎉 Import Complete!');
  console.log(`✅ Successfully imported: ${successCount} accounts`);
  console.log(`❌ Errors: ${errorCount}`);
  
  if (errors.length > 0) {
    console.log('\n❌ First 5 errors:');
    errors.slice(0, 5).forEach(err => {
      console.log(`  - ${err.account}: ${err.error}`);
    });
  }
  
  // Check final count
  try {
    const countResponse = await fetch(`http://localhost:3000/api/accounts?companyId=${companyId}&limit=1`);
    const countData = await countResponse.json();
    console.log(`\n📊 Total accounts in demo company: ${countData.total}`);
  } catch (err) {
    console.log('Could not fetch final count');
  }
}

simpleImport().catch(console.error);