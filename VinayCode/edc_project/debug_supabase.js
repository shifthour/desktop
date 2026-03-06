const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = 'https://eflvzsfgoelonfclzrjy.supabase.co';
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVmbHZ6c2Znb2Vsb25mY2x6cmp5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDc2NTYxOTAsImV4cCI6MjA2MzIzMjE5MH0.LCxuFDJuBO8ggiqdWpv5uoOkplKWbcF8z5E_kdpOEWU';

const supabase = createClient(supabaseUrl, supabaseKey);

async function debugSupabase() {
  console.log('🔍 Debugging Supabase Connection...\n');
  
  // Test 1: Check if we can connect
  console.log('1. Testing basic connection...');
  try {
    const { data, error } = await supabase.from('non_existent_table').select('*').limit(1);
    console.log('   Connection: ✅ Success (got expected error for non-existent table)');
  } catch (err) {
    console.log('   Connection: ❌ Failed -', err.message);
    return;
  }
  
  // Test 2: Check what tables exist
  console.log('\n2. Checking what tables exist...');
  
  // Try to query information_schema to see all tables
  const { data: tables, error: tableError } = await supabase.rpc('get_tables');
  
  if (tableError) {
    console.log('   Could not get table list:', tableError.message);
    console.log('   Trying direct table checks...');
  }
  
  // Test 3: Check specific EDC tables
  console.log('\n3. Testing specific EDC tables...');
  
  const edcTables = [
    'EDC_users',
    'EDC_studies', 
    'EDC_sites',
    'EDC_form_templates',
    'EDC_subjects'
  ];
  
  for (const table of edcTables) {
    try {
      const { data, error } = await supabase.from(table).select('*').limit(1);
      if (error) {
        console.log(`   ${table}: ❌ ${error.message}`);
      } else {
        console.log(`   ${table}: ✅ Exists (${data?.length || 0} rows in sample)`);
      }
    } catch (err) {
      console.log(`   ${table}: ❌ ${err.message}`);
    }
  }
  
  // Test 4: If EDC_users exists, check the data
  console.log('\n4. Checking EDC_users table specifically...');
  try {
    const { data: users, error } = await supabase
      .from('EDC_users')
      .select('username, email, phase, status')
      .limit(10);
      
    if (error) {
      console.log('   EDC_users query error:', error.message);
    } else {
      console.log('   EDC_users found:', users?.length || 0, 'users');
      if (users && users.length > 0) {
        console.log('   Users in database:');
        users.forEach(user => {
          console.log(`     - ${user.username} (${user.phase}) - Status: ${user.status}`);
        });
      }
    }
  } catch (err) {
    console.log('   EDC_users check failed:', err.message);
  }
  
  // Test 5: Test login credentials directly
  console.log('\n5. Testing login for startup_admin...');
  try {
    const { data: user, error } = await supabase
      .from('EDC_users')
      .select('*')
      .eq('username', 'startup_admin')
      .eq('status', 'ACTIVE')
      .single();
      
    if (error) {
      console.log('   Login test error:', error.message);
    } else if (user) {
      console.log('   ✅ startup_admin found in database');
      console.log('   User details:', {
        username: user.username,
        email: user.email,
        phase: user.phase,
        role: user.role,
        status: user.status
      });
    } else {
      console.log('   ❌ startup_admin not found');
    }
  } catch (err) {
    console.log('   Login test failed:', err.message);
  }
  
  console.log('\n🏁 Debug complete!');
}

debugSupabase().catch(console.error);