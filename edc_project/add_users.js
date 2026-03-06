const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = 'https://eflvzsfgoelonfclzrjy.supabase.co';
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVmbHZ6c2Znb2Vsb25mY2x6cmp5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDc2NTYxOTAsImV4cCI6MjA2MzIzMjE5MH0.LCxuFDJuBO8ggiqdWpv5uoOkplKWbcF8z5E_kdpOEWU';

const supabase = createClient(supabaseUrl, supabaseKey);

async function addUsers() {
  console.log('➕ Adding EDC users...\n');
  
  const users = [
    {
      username: 'startup_admin',
      email: 'startup@edc.com',
      password_hash: '$2b$10$hashedpassword1',
      first_name: 'Sarah',
      last_name: 'Johnson',
      role: 'Study Designer',
      phase: 'START_UP',
      permissions: {
        "createStudies": true, 
        "designForms": true, 
        "setupSites": true, 
        "configureWorkflows": true, 
        "viewReports": true, 
        "manageUsers": false
      }
    },
    {
      username: 'conduct_crc',
      email: 'conduct@edc.com',
      password_hash: '$2b$10$hashedpassword2',
      first_name: 'Michael',
      last_name: 'Chen',
      role: 'Clinical Research Coordinator',
      phase: 'CONDUCT',
      permissions: {
        "enrollSubjects": true, 
        "enterData": true, 
        "manageQueries": true, 
        "viewReports": true, 
        "exportData": false, 
        "lockDatabase": false
      }
    },
    {
      username: 'closeout_manager',
      email: 'closeout@edc.com',
      password_hash: '$2b$10$hashedpassword3',
      first_name: 'Emily',
      last_name: 'Rodriguez',
      role: 'Data Manager',
      phase: 'CLOSE_OUT',
      permissions: {
        "closeStudy": true, 
        "exportData": true, 
        "generateReports": true, 
        "verifyData": true, 
        "lockDatabase": true, 
        "archiveData": true
      }
    }
  ];
  
  for (const user of users) {
    console.log(`Adding user: ${user.username}...`);
    
    const { data, error } = await supabase
      .from('edc_users')
      .insert([user])
      .select();
      
    if (error) {
      if (error.code === '23505') { // Unique constraint violation
        console.log(`⚠️  User ${user.username} already exists`);
      } else {
        console.log(`❌ Error adding ${user.username}:`, error.message);
      }
    } else {
      console.log(`✅ Added user: ${user.username}`);
    }
  }
  
  // Verify users were added
  console.log('\n🔍 Verifying users...');
  const { data: allUsers, error: fetchError } = await supabase
    .from('edc_users')
    .select('username, email, phase, status');
    
  if (fetchError) {
    console.log('❌ Error fetching users:', fetchError.message);
  } else {
    console.log(`✅ Total users in database: ${allUsers.length}`);
    allUsers.forEach(user => {
      console.log(`   - ${user.username} (${user.phase}) - Status: ${user.status}`);
    });
  }
  
  console.log('\n🏁 User setup complete!');
}

addUsers().catch(console.error);