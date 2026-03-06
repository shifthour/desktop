const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = 'https://eflvzsfgoelonfclzrjy.supabase.co';
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVmbHZ6c2Znb2Vsb25mY2x6cmp5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDc2NTYxOTAsImV4cCI6MjA2MzIzMjE5MH0.LCxuFDJuBO8ggiqdWpv5uoOkplKWbcF8z5E_kdpOEWU';

const supabase = createClient(supabaseUrl, supabaseKey);

async function advancedDebug() {
  console.log('🔍 Advanced Supabase Debugging...\n');
  
  // Test different table name variations
  const tableVariations = [
    'EDC_users',
    'edc_users', 
    'public.EDC_users',
    'public.edc_users',
    'Users',
    'users'
  ];
  
  console.log('1. Testing different table name variations...');
  for (const tableName of tableVariations) {
    try {
      const { data, error } = await supabase.from(tableName).select('count').limit(1);
      if (!error) {
        console.log(`   ✅ ${tableName}: Found!`);
        
        // If we found a users table, get the actual data
        if (tableName.toLowerCase().includes('user')) {
          const { data: users, error: userError } = await supabase
            .from(tableName)
            .select('username, email, phase, status')
            .limit(5);
          
          if (!userError && users) {
            console.log(`      Users in ${tableName}:`);
            users.forEach(user => {
              console.log(`        - ${user.username} (${user.phase || 'no phase'}) - ${user.status || 'no status'}`);
            });
          }
        }
      } else {
        console.log(`   ❌ ${tableName}: ${error.message}`);
      }
    } catch (err) {
      console.log(`   ❌ ${tableName}: ${err.message}`);
    }
  }
  
  // Test 2: Check if there are any tables at all
  console.log('\n2. Checking for any tables...');
  try {
    // Try to get table names using information_schema
    const { data, error } = await supabase.rpc('get_schema');
    if (error) {
      console.log('   Could not get schema info:', error.message);
    }
  } catch (err) {
    console.log('   Schema check failed:', err.message);
  }
  
  // Test 3: Try raw SQL query
  console.log('\n3. Testing raw SQL queries...');
  try {
    const { data, error } = await supabase.rpc('exec_sql', {
      query: "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE '%user%'"
    });
    
    if (error) {
      console.log('   Raw SQL failed:', error.message);
    } else {
      console.log('   Raw SQL result:', data);
    }
  } catch (err) {
    console.log('   Raw SQL error:', err.message);
  }
  
  // Test 4: Check specific error details with exact login query
  console.log('\n4. Testing exact login query...');
  try {
    const { data: user, error: userError } = await supabase
      .from('EDC_users')
      .select('*')
      .eq('username', 'startup_admin')
      .eq('status', 'ACTIVE')
      .single();
      
    console.log('   Login query result:', { 
      hasData: !!user, 
      errorCode: userError?.code,
      errorMessage: userError?.message,
      errorDetails: userError?.details
    });
    
  } catch (err) {
    console.log('   Login query error:', err.message);
  }
  
  console.log('\n🏁 Advanced debug complete!');
}

advancedDebug().catch(console.error);