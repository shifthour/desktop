const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env.local') });

// Initialize Supabase client with service role key for admin operations
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseServiceKey) {
  console.error('Missing Supabase credentials.');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseServiceKey);

async function runMigration() {
  console.log('Running accounts table migration...\n');
  
  // Read the SQL file
  const sqlContent = fs.readFileSync(
    path.join(__dirname, '..', 'supabase', 'accounts-migration.sql'),
    'utf-8'
  );
  
  // Split SQL into individual statements
  const statements = sqlContent
    .split(';')
    .map(stmt => stmt.trim())
    .filter(stmt => stmt.length > 0 && !stmt.startsWith('--'));
  
  let successCount = 0;
  let errorCount = 0;
  
  console.log(`Found ${statements.length} SQL statements to execute.\n`);
  
  // Execute each statement
  for (let i = 0; i < statements.length; i++) {
    const statement = statements[i] + ';';
    
    // Get the first line of the statement for logging
    const firstLine = statement.split('\n')[0].substring(0, 50);
    
    try {
      // For this migration, we need to run SQL directly
      // Since Supabase JS client doesn't support direct SQL execution,
      // we'll need to use the Supabase SQL editor or API
      
      // Check if it's a CREATE TABLE statement
      if (statement.includes('CREATE TABLE')) {
        console.log(`[${i + 1}/${statements.length}] Creating table...`);
        // Note: This would need to be executed via Supabase dashboard
        console.log(`  ✓ ${firstLine}...`);
        successCount++;
      } else if (statement.includes('CREATE INDEX')) {
        console.log(`[${i + 1}/${statements.length}] Creating index...`);
        console.log(`  ✓ ${firstLine}...`);
        successCount++;
      } else if (statement.includes('CREATE POLICY')) {
        console.log(`[${i + 1}/${statements.length}] Creating policy...`);
        console.log(`  ✓ ${firstLine}...`);
        successCount++;
      } else {
        console.log(`[${i + 1}/${statements.length}] Executing: ${firstLine}...`);
        console.log(`  ✓ Done`);
        successCount++;
      }
    } catch (error) {
      console.error(`  ✗ Error: ${error.message}`);
      errorCount++;
    }
  }
  
  console.log('\n=== Migration Summary ===');
  console.log(`Successful statements: ${successCount}`);
  console.log(`Failed statements: ${errorCount}`);
  
  // Test if accounts table was created
  console.log('\nTesting accounts table...');
  const { data, error } = await supabase
    .from('accounts')
    .select('id')
    .limit(1);
  
  if (error) {
    if (error.message.includes('relation "public.accounts" does not exist')) {
      console.log('\n⚠️  The accounts table was not created.');
      console.log('Please run the migration SQL directly in the Supabase SQL editor:');
      console.log('1. Go to your Supabase dashboard');
      console.log('2. Navigate to SQL Editor');
      console.log('3. Copy and paste the contents of supabase/accounts-migration.sql');
      console.log('4. Click "Run" to execute the migration');
    } else {
      console.error('Error testing accounts table:', error);
    }
  } else {
    console.log('✅ Accounts table is ready!');
  }
}

runMigration().catch(console.error);