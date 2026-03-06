const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = 'https://eflvzsfgoelonfclzrjy.supabase.co';
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVmbHZ6c2Znb2Vsb25mY2x6cmp5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDc2NTYxOTAsImV4cCI6MjA2MzIzMjE5MH0.LCxuFDJuBO8ggiqdWpv5uoOkplKWbcF8z5E_kdpOEWU';

console.log('🔗 Testing Supabase connection...\n');
console.log('URL:', supabaseUrl);
console.log('Key (first 20 chars):', supabaseKey.substring(0, 20) + '...');

const supabase = createClient(supabaseUrl, supabaseKey);

async function testConnection() {
  try {
    // Test 1: Basic connection
    console.log('\n1. Testing basic table access...');
    const { data, error } = await supabase
      .from('edc_sites')
      .select('id')
      .limit(1);
      
    if (error) {
      console.log('❌ Basic connection failed:', error);
      console.log('Error code:', error.code);
      console.log('Error message:', error.message);
      console.log('Error details:', error.details);
      return;
    }
    
    console.log('✅ Basic connection works');
    console.log('Sample data from edc_sites:', data);
    
    // Test 2: Users table specific
    console.log('\n2. Testing edc_users table access...');
    const { data: usersData, error: usersError, count } = await supabase
      .from('edc_users')
      .select('*', { count: 'exact' });
      
    if (usersError) {
      console.log('❌ Users table access failed:', usersError);
      console.log('Error code:', usersError.code);
      console.log('Error message:', usersError.message);
    } else {
      console.log('✅ Users table accessible');
      console.log('Row count:', count);
      console.log('Data:', usersData);
    }
    
  } catch (err) {
    console.error('❌ Connection test error:', err);
  }
}

testConnection().catch(console.error);