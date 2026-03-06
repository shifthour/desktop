const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = 'https://eflvzsfgoelonfclzrjy.supabase.co';
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVmbHZ6c2Znb2Vsb25mY2x6cmp5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDc2NTYxOTAsImV4cCI6MjA2MzIzMjE5MH0.LCxuFDJuBO8ggiqdWpv5uoOkplKWbcF8z5E_kdpOEWU';

const supabase = createClient(supabaseUrl, supabaseKey);

async function checkUsers() {
  console.log('👥 Checking users in lowercase table...\n');
  
  try {
    const { data: users, error } = await supabase
      .from('edc_users')  // lowercase
      .select('*')
      .limit(10);
      
    if (error) {
      console.log('❌ Error:', error.message);
      return;
    }
    
    console.log(`✅ Found ${users.length} users in edc_users table:`);
    users.forEach((user, index) => {
      console.log(`\n${index + 1}. Username: ${user.username}`);
      console.log(`   Email: ${user.email}`);
      console.log(`   Phase: ${user.phase}`);
      console.log(`   Role: ${user.role}`);
      console.log(`   Status: ${user.status}`);
      console.log(`   Name: ${user.first_name} ${user.last_name}`);
    });
    
    // Test specific login
    console.log('\n🔐 Testing login for startup_admin...');
    const { data: user, error: loginError } = await supabase
      .from('edc_users')  // lowercase
      .select('*')
      .eq('username', 'startup_admin')
      .eq('status', 'ACTIVE')
      .single();
      
    if (loginError) {
      console.log('❌ Login test error:', loginError.message);
    } else if (user) {
      console.log('✅ startup_admin found successfully!');
      console.log('   User details:', {
        id: user.id,
        username: user.username,
        phase: user.phase,
        role: user.role
      });
    } else {
      console.log('❌ startup_admin not found');
    }
    
  } catch (err) {
    console.log('❌ Check failed:', err.message);
  }
}

checkUsers().catch(console.error);