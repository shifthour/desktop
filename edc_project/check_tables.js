const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = 'https://eflvzsfgoelonfclzrjy.supabase.co';
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVmbHZ6c2Znb2Vsb25mY2x6cmp5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDc2NTYxOTAsImV4cCI6MjA2MzIzMjE5MH0.LCxuFDJuBO8ggiqdWpv5uoOkplKWbcF8z5E_kdpOEWU';

const supabase = createClient(supabaseUrl, supabaseKey);

async function checkAllTables() {
  console.log('🔍 Checking all EDC tables...\n');
  
  const tables = [
    'edc_users',
    'edc_studies', 
    'edc_sites',
    'edc_form_templates',
    'edc_subjects',
    'edc_study_metrics',
    'edc_data_verification',
    'edc_closure_activities'
  ];
  
  for (const table of tables) {
    try {
      const { data, error, count } = await supabase
        .from(table)
        .select('*', { count: 'exact', head: true });
        
      if (error) {
        console.log(`❌ ${table}: ${error.message}`);
      } else {
        console.log(`✅ ${table}: ${count} rows`);
        
        // If it's users table, show sample data
        if (table === 'edc_users' && count > 0) {
          const { data: users } = await supabase
            .from(table)
            .select('username, email, phase, status')
            .limit(3);
          console.log('   Sample users:', users);
        }
      }
    } catch (err) {
      console.log(`❌ ${table}: ${err.message}`);
    }
  }
  
  console.log('\n🏁 Table check complete!');
}

checkAllTables().catch(console.error);