const { createClient } = require('@supabase/supabase-js');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env.local') });

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

const supabase = createClient(supabaseUrl, supabaseServiceKey);

async function createDemoCompany() {
  console.log('Creating demo company for historical CRM data...');
  
  // Create demo company
  const { data: company, error: companyError } = await supabase
    .from('companies')
    .insert({
      name: 'LabGig Demo Company',
      domain: 'demo.labgig.com',
      max_users: 50,
      current_users: 0,
      subscription_status: 'active'
    })
    .select()
    .single();
  
  if (companyError) {
    console.error('Error creating company:', companyError);
    return null;
  }
  
  console.log(`✅ Created demo company: ${company.name} (${company.id})`);
  
  // Create demo admin user
  const { data: adminUser, error: userError } = await supabase
    .from('users')
    .insert({
      company_id: company.id,
      email: 'demo-admin@labgig.com',
      full_name: 'Demo Administrator',
      password: 'demo123',
      role_id: (await supabase.from('user_roles').select('id').eq('name', 'company_admin').single()).data.id,
      is_admin: true,
      is_active: true,
      password_changed: true
    })
    .select()
    .single();
  
  if (userError) {
    console.error('Error creating admin user:', userError);
    return company;
  }
  
  console.log(`✅ Created demo admin: ${adminUser.full_name} (${adminUser.email})`);
  
  return {
    company,
    adminUser
  };
}

// Run if called directly
if (require.main === module) {
  createDemoCompany()
    .then(result => {
      if (result) {
        console.log('\n🎉 Demo company setup complete!');
        console.log('Now you can:');
        console.log('1. Import historical accounts under this demo company');
        console.log('2. Use as template/demo for new customers');
        console.log('3. Each new customer gets their own company ID');
        console.log('\nNext: Run import-accounts.js with demo company ID');
      }
      process.exit(0);
    })
    .catch(console.error);
}

module.exports = { createDemoCompany };