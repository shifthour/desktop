const { createClient } = require('@supabase/supabase-js');

// Use service role key for admin operations - bypass RLS
const supabaseUrl = 'https://eflvzsfgoelonfclzrjy.supabase.co';
const serviceKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVmbHZ6c2Znb2Vsb25mY2x6cmp5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0NzY1NjE5MCwiZXhwIjoyMDYzMjMyMTkwfQ.8TH5b1Td9sLGdvNDHLPHUj8ZZZx0nJXHK4w2vC1RHeg';

const supabase = createClient(supabaseUrl, serviceKey, {
  auth: {
    autoRefreshToken: false,
    persistSession: false
  }
});

async function tempAddUser() {
  console.log('🔧 Attempting to add startup_admin user...\n');
  
  try {
    // Try to add just one user for testing
    const user = {
      username: 'startup_admin',
      email: 'startup@edc.com',
      password_hash: '$2b$10$hashedpassword1',
      first_name: 'Sarah',
      last_name: 'Johnson', 
      role: 'Study Designer',
      phase: 'START_UP',
      status: 'ACTIVE',
      permissions: {
        "createStudies": true,
        "designForms": true,
        "setupSites": true,
        "configureWorkflows": true,
        "viewReports": true,
        "manageUsers": false
      }
    };
    
    console.log('Trying to insert with service role key...');
    const { data, error } = await supabase
      .from('edc_users')
      .insert([user])
      .select();
      
    if (error) {
      console.log('❌ Service role insert failed:', error.message);
      console.log('Error code:', error.code);
      
      // Try with regular anon key but disable RLS first
      console.log('\nTrying alternative approach...');
      
      const anonSupabase = createClient(supabaseUrl, 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVmbHZ6c2Znb2Vsb25mY2x6cmp5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDc2NTYxOTAsImV4cCI6MjA2MzIzMjE5MH0.LCxuFDJuBO8ggiqdWpv5uoOkplKWbcF8z5E_kdpOEWU');
      
      // Check if user already exists
      const { data: existingUser } = await anonSupabase
        .from('edc_users')
        .select('username')
        .eq('username', 'startup_admin')
        .single();
        
      if (existingUser) {
        console.log('✅ User already exists in database');
      } else {
        console.log('❌ User does not exist and cannot be created via API');
        console.log('📝 You need to run the SQL script manually in Supabase SQL Editor');
      }
      
    } else {
      console.log('✅ User added successfully:', data);
    }
    
    // Verify users exist
    console.log('\n🔍 Checking current users...');
    const anonSupabase = createClient(supabaseUrl, 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVmbHZ6c2Znb2Vsb25mY2x6cmp5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDc2NTYxOTAsImV4cCI6MjA2MzIzMjE5MH0.LCxuFDJuBO8ggiqdWpv5uoOkplKWbcF8z5E_kdpOEWU');
    
    const { data: allUsers, count } = await anonSupabase
      .from('edc_users')
      .select('username, email, phase, status', { count: 'exact' });
      
    console.log(`Found ${count} users:`);
    if (allUsers) {
      allUsers.forEach(u => console.log(`  - ${u.username} (${u.phase})`));
    }
    
  } catch (error) {
    console.error('❌ Script error:', error.message);
  }
  
  console.log('\n🏁 User addition attempt complete!');
}

tempAddUser().catch(console.error);