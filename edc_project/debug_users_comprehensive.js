const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = 'https://eflvzsfgoelonfclzrjy.supabase.co';
const anonKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVmbHZ6c2Znb2Vsb25mY2x6cmp5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDc2NTYxOTAsImV4cCI6MjA2MzIzMjE5MH0.LCxuFDJuBO8ggiqdWpv5uoOkplKWbcF8z5E_kdpOEWU';

const supabase = createClient(supabaseUrl, anonKey);

async function debugUsers() {
  console.log('🔍 COMPREHENSIVE USER DEBUG...\n');
  
  // Test 1: Check table structure
  console.log('1. Checking table structure...');
  try {
    const { data, error } = await supabase
      .from('edc_users')
      .select('*')
      .limit(0); // Just get schema
      
    if (error) {
      console.log('❌ Schema check error:', error.message);
    } else {
      console.log('✅ Table schema accessible');
    }
  } catch (err) {
    console.log('❌ Schema error:', err.message);
  }
  
  // Test 2: Try different query approaches
  console.log('\n2. Testing different query approaches...');
  
  // Approach A: Simple select all
  console.log('   A. Simple select all:');
  try {
    const { data, error, count } = await supabase
      .from('edc_users')
      .select('*', { count: 'exact' });
      
    console.log(`      Result: ${count} rows, Error: ${error?.message || 'none'}`);
    if (data && data.length > 0) {
      console.log('      Sample user:', data[0]);
    }
  } catch (err) {
    console.log('      Error:', err.message);
  }
  
  // Approach B: Count only
  console.log('   B. Count only:');
  try {
    const { count, error } = await supabase
      .from('edc_users')
      .select('*', { count: 'exact', head: true });
      
    console.log(`      Count: ${count}, Error: ${error?.message || 'none'}`);
  } catch (err) {
    console.log('      Error:', err.message);
  }
  
  // Approach C: Specific username search
  console.log('   C. Search for startup_admin:');
  try {
    const { data, error } = await supabase
      .from('edc_users')
      .select('username, email, phase, status')
      .eq('username', 'startup_admin');
      
    console.log(`      Found: ${data?.length || 0} users, Error: ${error?.message || 'none'}`);
    if (data && data.length > 0) {
      console.log('      User data:', data[0]);
    }
  } catch (err) {
    console.log('      Error:', err.message);
  }
  
  // Test 3: Check RLS status
  console.log('\n3. Testing if RLS is blocking queries...');
  try {
    const { data, error } = await supabase
      .from('edc_sites')
      .select('*')
      .limit(1);
      
    if (data && data.length > 0) {
      console.log('✅ Other tables work fine - RLS likely blocking edc_users');
    } else {
      console.log('❌ All tables seem blocked');
    }
  } catch (err) {
    console.log('❌ General DB issue:', err.message);
  }
  
  // Test 4: Try the exact login query
  console.log('\n4. Testing exact login query from server...');
  try {
    const { data: user, error: userError } = await supabase
      .from('edc_users')
      .select('*')
      .eq('username', 'startup_admin')
      .eq('status', 'ACTIVE')
      .single();
      
    console.log('   Login query result:');
    console.log(`   - User found: ${!!user}`);
    console.log(`   - Error code: ${userError?.code}`);
    console.log(`   - Error message: ${userError?.message}`);
    console.log(`   - Error details: ${userError?.details}`);
    
    if (user) {
      console.log('   - User data:', {
        username: user.username,
        email: user.email,
        phase: user.phase,
        status: user.status
      });
    }
  } catch (err) {
    console.log('   Error:', err.message);
  }
  
  console.log('\n📋 DIAGNOSIS:');
  console.log('If you see "0 rows" or "PGRST116" errors above, then either:');
  console.log('1. The SQL script failed to insert users');
  console.log('2. Row Level Security is blocking the anon key from seeing users');
  console.log('3. Users were inserted with different usernames/data');
  console.log('\nNext steps:');
  console.log('- Check Supabase dashboard directly for users table content');
  console.log('- Or run: SELECT * FROM edc_users; in SQL editor to see what exists');
  
  console.log('\n🏁 Debug complete!');
}

debugUsers().catch(console.error);