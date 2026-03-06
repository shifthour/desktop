import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

if (!supabaseUrl || !supabaseServiceKey) {
  console.error('Missing Supabase environment variables')
}

const supabase = createClient(supabaseUrl, supabaseServiceKey)

export async function GET(request: NextRequest) {
  try {
    console.log('Accounts API GET request received')
    console.log('Supabase URL:', supabaseUrl ? 'Set' : 'Missing')
    console.log('Supabase Key:', supabaseServiceKey ? 'Set' : 'Missing')
    
    const { searchParams } = new URL(request.url)
    const companyId = searchParams.get('companyId')
    const search = searchParams.get('search')
    const industry = searchParams.get('industry')
    const city = searchParams.get('city')
    const page = parseInt(searchParams.get('page') || '1')
    const limit = parseInt(searchParams.get('limit') || '50')
    const offset = (page - 1) * limit
    
    // Build query with user join to get owner name
    let query = supabase
      .from('accounts')
      .select(`
        *,
        owner:users!accounts_owner_id_fkey(id, full_name, email)
      `, { count: 'exact' })
      .order('created_at', { ascending: false })
      .range(offset, offset + limit - 1)
    
    // Apply filters
    if (search) {
      query = query.or(`account_name.ilike.%${search}%,billing_city.ilike.%${search}%,website.ilike.%${search}%`)
    }
    
    if (industry) {
      query = query.eq('industry', industry)
    }
    
    if (city) {
      query = query.eq('billing_city', city)
    }
    
    const { data, error, count } = await query

    console.log('Query executed. Error:', error, 'Data count:', data?.length, 'Total count:', count)

    // Debug: Log first account to see owner data
    if (data && data.length > 0) {
      console.log('Sample account data:', JSON.stringify(data[0], null, 2))
    }

    if (error) {
      console.error('Error fetching accounts:', error)
      return NextResponse.json({ 
        error: error.message,
        accounts: [],
        total: 0
      }, { status: 500 })
    }
    
    return NextResponse.json({
      accounts: data || [],
      total: count || 0,
      page,
      totalPages: Math.ceil((count || 0) / limit)
    })
    
  } catch (error: any) {
    console.error('GET accounts error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const { companyId, userId, ...accountData } = body

    console.log('=== POST /api/accounts - RAW DATA ===')
    console.log('Account Name:', accountData.account_name)
    console.log('Account Type:', accountData.account_type)
    console.log('acct_industry:', accountData.acct_industry)
    console.log('acct_sub_industry:', accountData.acct_sub_industry)
    console.log('industries array:', accountData.industries)
    console.log('Full accountData keys:', Object.keys(accountData))

    if (!companyId) {
      return NextResponse.json({ error: 'Company ID is required' }, { status: 400 })
    }

    if (!accountData.account_name) {
      return NextResponse.json({ error: 'Account name is required' }, { status: 400 })
    }

    // Fetch enabled field configurations for this company
    const { data: fieldConfigs, error: configError } = await supabase
      .from('account_field_configurations')
      .select('field_name, is_enabled')
      .eq('company_id', companyId)
      .eq('is_enabled', true)

    if (configError) {
      console.error('Error fetching field configurations:', configError)
      return NextResponse.json({
        error: 'Failed to load field configurations',
        details: configError.message
      }, { status: 500 })
    }

    // Get list of enabled field names
    const enabledFieldNames = new Set(
      (fieldConfigs || []).map(config => config.field_name)
    )

    // Clean and filter the account data
    // Only include fields that are enabled in the configuration
    const cleanedAccountData: any = {}

    // Check if this is a distributor with industries
    const isDistributor = accountData.account_type === 'Distributor' && accountData.industries && Array.isArray(accountData.industries) && accountData.industries.length > 0

    console.log('Processing account:', accountData.account_name, 'Type:', accountData.account_type, 'Is Distributor with industries:', isDistributor)

    Object.keys(accountData).forEach(key => {
      // Skip acct_industry and acct_sub_industry for distributors with industries
      if (isDistributor && (key === 'acct_industry' || key === 'acct_sub_industry')) {
        console.log('Skipping field for distributor:', key)
        return
      }

      // Always include 'industries' field for distributor accounts (JSONB field, not in field configs)
      // Only process fields that are in the enabled configuration OR special fields like 'industries'
      if (enabledFieldNames.has(key) || key === 'industries') {
        const value = accountData[key]
        // For industries field, keep array as-is (even if empty)
        if (key === 'industries') {
          cleanedAccountData[key] = Array.isArray(value) ? value : []
        }
        // Convert empty strings to null, keep other values as-is
        else if (value === '' || value === undefined) {
          cleanedAccountData[key] = null
        } else if (typeof value === 'string' && value.trim() === '') {
          cleanedAccountData[key] = null
        } else {
          cleanedAccountData[key] = value
        }
      }
    })

    // Check for duplicate (same name AND city) - only if city is provided and enabled
    if (cleanedAccountData.billing_city) {
      const { data: existing } = await supabase
        .from('accounts')
        .select('id')
        .eq('company_id', companyId)
        .eq('account_name', cleanedAccountData.account_name)
        .eq('billing_city', cleanedAccountData.billing_city)
        .single()

      if (existing) {
        return NextResponse.json({
          error: `An account with name "${cleanedAccountData.account_name}" already exists in ${cleanedAccountData.billing_city}`
        }, { status: 400 })
      }
    }

    // Build insert data with only enabled fields
    const insertData: any = {
      company_id: companyId,
      ...cleanedAccountData
    }

    // For distributors, ensure acct_industry and acct_sub_industry are NULL
    if (cleanedAccountData.account_type === 'Distributor' && cleanedAccountData.industries) {
      insertData.acct_industry = null
      insertData.acct_sub_industry = null
    }

    // Only add owner_id and created_by if those fields are enabled
    if (enabledFieldNames.has('owner_id')) {
      insertData.owner_id = userId || null
    }
    if (enabledFieldNames.has('created_by')) {
      insertData.created_by = userId || null
    }

    // Create account
    const { data, error } = await supabase
      .from('accounts')
      .insert(insertData)
      .select()
      .single()

    if (error) {
      console.error('Error creating account:', error)
      console.error('Error details:', JSON.stringify(error, null, 2))
      console.error('Insert data:', JSON.stringify(insertData, null, 2))
      return NextResponse.json({
        error: error.message,
        details: error.details || 'No additional details',
        hint: error.hint || 'Check the data format and required fields'
      }, { status: 500 })
    }

    return NextResponse.json(data, { status: 201 })

  } catch (error: any) {
    console.error('POST account error:', error)
    return NextResponse.json({
      error: error.message || 'Internal server error',
      details: 'An unexpected error occurred while creating the account'
    }, { status: 500 })
  }
}

export async function PUT(request: NextRequest) {
  try {
    const body = await request.json()
    const { id, companyId, ...updates } = body

    if (!id) {
      return NextResponse.json({ error: 'Account ID is required' }, { status: 400 })
    }

    // If companyId is provided, fetch enabled field configurations
    let enabledFieldNames: Set<string> | null = null
    if (companyId) {
      const { data: fieldConfigs, error: configError } = await supabase
        .from('account_field_configurations')
        .select('field_name, is_enabled')
        .eq('company_id', companyId)
        .eq('is_enabled', true)

      if (!configError && fieldConfigs) {
        enabledFieldNames = new Set(
          fieldConfigs.map(config => config.field_name)
        )
      }
    }

    // Clean the update data - convert empty strings to null
    const cleanedUpdates: any = {}
    Object.keys(updates).forEach(key => {
      // Always include 'industries' field for distributor accounts (JSONB field, not in field configs)
      // If we have field configurations, only process enabled fields OR special fields like 'industries'
      if (enabledFieldNames && !enabledFieldNames.has(key) && key !== 'industries') {
        return // Skip this field if it's not enabled
      }

      const value = updates[key]
      // For industries field, keep array as-is (even if empty)
      if (key === 'industries') {
        cleanedUpdates[key] = Array.isArray(value) ? value : []
      }
      // Convert empty strings to null, keep other values as-is
      else if (value === '' || value === undefined) {
        cleanedUpdates[key] = null
      } else if (typeof value === 'string' && value.trim() === '') {
        cleanedUpdates[key] = null
      } else {
        cleanedUpdates[key] = value
      }
    })

    const { data, error } = await supabase
      .from('accounts')
      .update(cleanedUpdates)
      .eq('id', id)
      .select()
      .single()

    if (error) {
      console.error('Error updating account:', error)
      console.error('Error details:', JSON.stringify(error, null, 2))
      console.error('Update data:', JSON.stringify(cleanedUpdates, null, 2))

      return NextResponse.json({
        error: error.message || 'Failed to update account',
        details: error.details || `Error code: ${error.code || 'UNKNOWN'}`,
        hint: error.hint || 'Check the account field configuration to ensure all fields are properly set up',
        code: error.code
      }, { status: 500 })
    }

    return NextResponse.json(data)

  } catch (error: any) {
    console.error('PUT account error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

export async function DELETE(request: NextRequest) {
  try {
    console.log('=== DELETE /api/accounts - REQUEST RECEIVED ===')
    const { searchParams } = new URL(request.url)
    const id = searchParams.get('id')
    const companyId = searchParams.get('companyId')

    console.log('Account ID:', id)
    console.log('Company ID:', companyId)

    if (!id) {
      console.log('Error: Account ID is missing')
      return NextResponse.json({ error: 'Account ID is required' }, { status: 400 })
    }

    console.log('Attempting to delete account:', id)

    const { error } = await supabase
      .from('accounts')
      .delete()
      .eq('id', id)

    if (error) {
      console.error('Error deleting account from database:', error)
      console.error('Error details:', JSON.stringify(error, null, 2))
      return NextResponse.json({ error: error.message }, { status: 500 })
    }

    console.log('Account deleted successfully:', id)
    return NextResponse.json({ success: true })

  } catch (error: any) {
    console.error('DELETE account error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}