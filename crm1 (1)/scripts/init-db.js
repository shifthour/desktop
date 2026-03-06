#!/usr/bin/env node

const { createClient } = require('@supabase/supabase-js')
const fs = require('fs')
const path = require('path')

// Read environment variables
require('dotenv').config({ path: '.env.local' })

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY

if (!supabaseUrl || !supabaseServiceKey) {
  console.error('Missing Supabase credentials in .env.local')
  process.exit(1)
}

const supabase = createClient(supabaseUrl, supabaseServiceKey, {
  auth: {
    autoRefreshToken: false,
    persistSession: false
  }
})

async function initializeDatabase() {
  console.log('🚀 Initializing database schema...')

  try {
    // Read and execute schema SQL
    const schemaPath = path.join(__dirname, '..', 'supabase', 'schema.sql')
    const schemaSql = fs.readFileSync(schemaPath, 'utf8')
    
    // Note: Supabase doesn't support running complex SQL via the JS client
    // You'll need to run this manually in the Supabase SQL editor
    console.log('📝 Database schema is ready to be executed.')
    console.log('Please copy the contents of supabase/schema.sql and run it in your Supabase SQL editor.')
    console.log('Dashboard URL: https://supabase.com/dashboard/project/eflvzsfgoelonfclzrjy')
    
    // Test connection
    const { data, error } = await supabase
      .from('companies')
      .select('count')
      .limit(1)

    if (error) {
      console.log('⚠️  Tables not yet created. Please run the schema.sql in Supabase dashboard.')
    } else {
      console.log('✅ Database connection successful!')
    }

  } catch (error) {
    console.error('❌ Database initialization failed:', error)
  }
}

async function createSampleData() {
  console.log('📊 Creating sample data...')

  try {
    // Create a sample company for testing
    const { data: company, error: companyError } = await supabase
      .from('companies')
      .upsert({
        name: 'Acme Instruments',
        domain: 'acme-instruments',
        max_users: 10,
        subscription_status: 'active'
      })
      .select()
      .single()

    if (companyError) {
      console.error('Error creating sample company:', companyError)
      return
    }

    console.log('✅ Sample company created:', company.name)

    // Get company admin role
    const { data: adminRole, error: roleError } = await supabase
      .from('user_roles')
      .select('*')
      .eq('name', 'company_admin')
      .single()

    if (roleError) {
      console.error('Error fetching admin role:', roleError)
      return
    }

    // Create a sample admin user
    const { data: user, error: userError } = await supabase
      .from('users')
      .upsert({
        company_id: company.id,
        email: 'admin@acme-instruments.com',
        full_name: 'John Admin',
        role_id: adminRole.id,
        is_admin: true,
        is_active: true
      })
      .select()

    if (userError) {
      console.error('Error creating sample user:', userError)
      return
    }

    console.log('✅ Sample admin user created:', user[0].email)
    console.log('')
    console.log('🎉 Sample data created successfully!')
    console.log('You can now log in with: admin@acme-instruments.com')

  } catch (error) {
    console.error('❌ Sample data creation failed:', error)
  }
}

async function main() {
  console.log('🔧 LabGig CRM Database Setup')
  console.log('============================')
  
  await initializeDatabase()
  
  const createSample = process.argv.includes('--with-sample')
  if (createSample) {
    await createSampleData()
  }
  
  console.log('')
  console.log('📚 Next steps:')
  console.log('1. Copy supabase/schema.sql content to Supabase SQL editor')
  console.log('2. Run the SQL to create tables and roles')
  console.log('3. Start your Next.js development server: npm run dev')
  console.log('4. Visit http://localhost:3000/admin to access admin panel')
}

main().catch(console.error)