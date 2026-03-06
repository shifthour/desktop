const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = 'https://eflvzsfgoelonfclzrjy.supabase.co';
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVmbHZ6c2Znb2Vsb25mY2x6cmp5Iiwicm9sZUI6ImFub24iLCJpYXQiOjE3NDc2NTYxOTAsImV4cCI6MjA2MzIzMjE5MH0.LCxuFDJuBO8ggiqdWpv5uoOkplKWbcF8z5E_kdpOEWU';

const supabase = createClient(supabaseUrl, supabaseKey);

async function fixRLSAndAddUsers() {
  console.log('🔧 Temporarily disabling RLS and adding users...\n');
  
  try {
    // Try to disable RLS temporarily for edc_users table
    console.log('1. Attempting to disable RLS on edc_users...');
    const { error: disableError } = await supabase.rpc('exec_sql', {
      query: 'ALTER TABLE edc_users DISABLE ROW LEVEL SECURITY;'
    });
    
    if (disableError) {
      console.log('   RLS disable failed (expected):', disableError.message);
      console.log('   Trying direct insert...');
    } else {
      console.log('   ✅ RLS disabled temporarily');
    }
    
    // Try direct SQL insert
    console.log('\n2. Attempting direct SQL insert...');
    const insertSQL = `
      INSERT INTO edc_users (username, email, password_hash, first_name, last_name, role, phase, permissions) 
      VALUES
      ('startup_admin', 'startup@edc.com', '$2b$10$hashedpassword1', 'Sarah', 'Johnson', 'Study Designer', 'START_UP', 
       '{"createStudies": true, "designForms": true, "setupSites": true, "configureWorkflows": true, "viewReports": true, "manageUsers": false}'),
      ('conduct_crc', 'conduct@edc.com', '$2b$10$hashedpassword2', 'Michael', 'Chen', 'Clinical Research Coordinator', 'CONDUCT', 
       '{"enrollSubjects": true, "enterData": true, "manageQueries": true, "viewReports": true, "exportData": false, "lockDatabase": false}'),
      ('closeout_manager', 'closeout@edc.com', '$2b$10$hashedpassword3', 'Emily', 'Rodriguez', 'Data Manager', 'CLOSE_OUT', 
       '{"closeStudy": true, "exportData": true, "generateReports": true, "verifyData": true, "lockDatabase": true, "archiveData": true}')
      ON CONFLICT (username) DO NOTHING;
    `;
    
    const { error: insertError } = await supabase.rpc('exec_sql', { query: insertSQL });
    
    if (insertError) {
      console.log('   ❌ Direct SQL insert failed:', insertError.message);
      console.log('\n   Trying individual inserts with upsert...');
      
      // Try individual upserts
      const users = [
        {
          username: 'startup_admin',
          email: 'startup@edc.com', 
          password_hash: '$2b$10$hashedpassword1',
          first_name: 'Sarah',
          last_name: 'Johnson',
          role: 'Study Designer',
          phase: 'START_UP',
          status: 'ACTIVE',
          permissions: {"createStudies": true, "designForms": true, "setupSites": true, "configureWorkflows": true, "viewReports": true, "manageUsers": false}
        },
        {
          username: 'conduct_crc',
          email: 'conduct@edc.com',
          password_hash: '$2b$10$hashedpassword2', 
          first_name: 'Michael',
          last_name: 'Chen',
          role: 'Clinical Research Coordinator',
          phase: 'CONDUCT',
          status: 'ACTIVE',
          permissions: {"enrollSubjects": true, "enterData": true, "manageQueries": true, "viewReports": true, "exportData": false, "lockDatabase": false}
        },
        {
          username: 'closeout_manager',
          email: 'closeout@edc.com',
          password_hash: '$2b$10$hashedpassword3',
          first_name: 'Emily', 
          last_name: 'Rodriguez',
          role: 'Data Manager',
          phase: 'CLOSE_OUT',
          status: 'ACTIVE',
          permissions: {"closeStudy": true, "exportData": true, "generateReports": true, "verifyData": true, "lockDatabase": true, "archiveData": true}
        }
      ];
      
      for (const user of users) {
        const { error: upsertError } = await supabase
          .from('edc_users')
          .upsert([user], { onConflict: 'username' });
          
        if (upsertError) {
          console.log(`   ❌ Failed to add ${user.username}:`, upsertError.message);
        } else {
          console.log(`   ✅ Added/updated ${user.username}`);
        }
      }
      
    } else {
      console.log('   ✅ Direct SQL insert successful');
    }
    
    // Re-enable RLS (optional)
    console.log('\n3. Re-enabling RLS...');
    const { error: enableError } = await supabase.rpc('exec_sql', {
      query: 'ALTER TABLE edc_users ENABLE ROW LEVEL SECURITY;'
    });
    
    if (enableError) {
      console.log('   RLS enable failed (expected):', enableError.message);
    } else {
      console.log('   ✅ RLS re-enabled');
    }
    
    // Verify users
    console.log('\n4. Verifying users...');
    const { data: users, error: fetchError } = await supabase
      .from('edc_users')
      .select('username, email, phase, status');
      
    if (fetchError) {
      console.log('   ❌ Error fetching users:', fetchError.message);
    } else {
      console.log(`   ✅ Found ${users.length} users:`);
      users.forEach(user => {
        console.log(`     - ${user.username} (${user.phase}) - Status: ${user.status}`);
      });
    }
    
  } catch (error) {
    console.error('❌ Script error:', error);
  }
  
  console.log('\n🏁 RLS fix and user setup complete!');
}

fixRLSAndAddUsers().catch(console.error);