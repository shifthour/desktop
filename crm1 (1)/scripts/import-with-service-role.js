const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env.local') });

// Use service role key to bypass RLS
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

const supabase = createClient(supabaseUrl, supabaseServiceKey, {
  auth: {
    autoRefreshToken: false,
    persistSession: false
  }
});

// Import with service role (bypasses RLS)
async function importWithServiceRole() {
  console.log('🚀 Starting import with service role to bypass RLS...');
  
  const accountsData = JSON.parse(
    fs.readFileSync(path.join(__dirname, 'accounts-to-import.json'), 'utf-8')
  );

  // Company and owner IDs
  const companyId = '22adbd06-8ce1-49ea-9a03-d0b46720c624';
  const ownerId = '6dd33b6b-1524-4b77-b8ff-2b4a61aeff8f'; // Demo admin ID
  
  const accountsToImport = accountsData.map(account => ({
    company_id: companyId,
    account_name: account.account_name,
    industry: account.industry,
    website: account.website,
    phone: account.phone,
    description: account.description,
    billing_street: account.billing_street,
    billing_city: account.billing_city,
    billing_state: account.billing_state,
    billing_country: account.billing_country,
    billing_postal_code: account.billing_postal_code,
    owner_id: ownerId,
    status: 'Active',
    account_type: 'Customer',
    original_id: account.original_id,
    original_created_by: account.original_created_by
  })).filter(acc => acc.account_name); // Remove empty names
  
  console.log(`📊 Importing ${accountsToImport.length} accounts directly...`);
  
  // Import all at once with service role (no RLS)
  const { data, error } = await supabase
    .from('accounts')
    .insert(accountsToImport);
  
  if (error) {
    console.error('❌ Import failed:', error);
    return;
  }
  
  console.log('✅ Import successful!');
  
  // Verify count
  const { count } = await supabase
    .from('accounts')
    .select('*', { count: 'exact', head: true })
    .eq('company_id', companyId);
  
  console.log(`🎉 Total accounts in demo company: ${count}`);
}

importWithServiceRole().catch(console.error);