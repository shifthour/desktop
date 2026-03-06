import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

const supabase = createClient(supabaseUrl, supabaseServiceKey)

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams
    const companyId = searchParams.get('companyId')
    const accountId = searchParams.get('accountId')

    // Fetch contacts
    let query = supabase
      .from('contacts')
      .select('*', { count: 'exact' })
      .order('created_at', { ascending: false })

    // Apply filters
    if (companyId) {
      query = query.eq('company_id', companyId)
    }

    if (accountId) {
      query = query.eq('account_id', accountId)
    }

    const { data, error, count } = await query

    if (error) {
      console.error('Error fetching contacts:', error)
      return NextResponse.json({ error: error.message }, { status: 500 })
    }

    // Fetch account details for each contact
    const contactsWithAccounts = await Promise.all(
      (data || []).map(async (contact) => {
        if (contact.account_id) {
          const { data: accountData } = await supabase
            .from('accounts')
            .select('account_name, customer_segment, account_type, acct_industry, acct_sub_industry, billing_city, billing_state, billing_country, main_phone, primary_email, website')
            .eq('id', contact.account_id)
            .single()

          console.log(`Contact ${contact.first_name} ${contact.last_name} - Account data:`, {
            account_id: contact.account_id,
            acct_industry: accountData?.acct_industry,
            acct_sub_industry: accountData?.acct_sub_industry
          })

          return {
            ...contact,
            account: accountData
          }
        }
        console.log(`Contact ${contact.first_name} ${contact.last_name} - No account_id`)
        return contact
      })
    )

    console.log(`Returning ${contactsWithAccounts.length} contacts with account data`)

    return NextResponse.json({
      contacts: contactsWithAccounts || [],
      total: count || 0
    })
  } catch (error: any) {
    console.error('GET contacts error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const { companyId, userId, accountId, ...contactData } = body

    if (!companyId) {
      return NextResponse.json({ error: 'Company ID is required' }, { status: 400 })
    }

    // Fetch enabled field configurations for this company
    const { data: fieldConfigs, error: configError } = await supabase
      .from('contact_field_configurations')
      .select('field_name, is_enabled, is_mandatory')
      .eq('company_id', companyId)
      .eq('is_enabled', true)

    if (configError) {
      console.error('Error fetching field configurations:', configError)
      return NextResponse.json({
        error: 'Failed to load field configurations',
        details: configError.message
      }, { status: 500 })
    }

    // Get list of enabled field names and mandatory fields
    const enabledFieldNames = new Set(
      (fieldConfigs || []).map(config => config.field_name)
    )
    const mandatoryFields = (fieldConfigs || [])
      .filter(config => config.is_mandatory)
      .map(config => config.field_name)

    // Validate mandatory fields
    for (const field of mandatoryFields) {
      if (!contactData[field]) {
        return NextResponse.json({ error: `${field} is required` }, { status: 400 })
      }
    }

    // Get account name if account_id is provided
    let companyName = contactData.company_name
    if (accountId) {
      const { data: accountData } = await supabase
        .from('accounts')
        .select('account_name')
        .eq('id', accountId)
        .single()

      if (accountData) {
        companyName = accountData.account_name
      }
    }

    // Clean and filter the contact data
    // Only include fields that are enabled in the configuration
    const cleanedContactData: any = {}
    Object.keys(contactData).forEach(key => {
      // Only process fields that are in the enabled configuration
      if (enabledFieldNames.has(key)) {
        const value = contactData[key]
        // Convert empty strings to null, keep other values as-is
        if (value === '' || value === undefined) {
          cleanedContactData[key] = null
        } else if (typeof value === 'string' && value.trim() === '') {
          cleanedContactData[key] = null
        } else {
          cleanedContactData[key] = value
        }
      }
    })

    // Build insert data with only enabled fields
    const insertData: any = {
      company_id: companyId,
      ...cleanedContactData
    }

    // Always add account_id when provided (system field, not user-configurable)
    insertData.account_id = accountId || null

    // Only add owner_id if that field is enabled
    if (enabledFieldNames.has('owner_id')) {
      insertData.owner_id = userId || null
    }
    // Only add created_by if that field is enabled
    if (enabledFieldNames.has('created_by')) {
      insertData.created_by = userId || null
    }
    // Only add company_name if that field is enabled
    if (enabledFieldNames.has('company_name') && companyName) {
      insertData.company_name = companyName
    }

    const { data, error } = await supabase
      .from('contacts')
      .insert(insertData)
      .select()
      .single()

    if (error) {
      console.error('Error creating contact:', error)
      console.error('Error details:', JSON.stringify(error, null, 2))
      console.error('Insert data:', JSON.stringify(insertData, null, 2))

      // Check for duplicate email error
      if (error.code === '23505' && error.message.includes('contacts_email_company_unique')) {
        const email = insertData.email_primary || 'this email'
        return NextResponse.json({
          error: `Email already exists`,
          message: `A contact with the email "${email}" already exists in your system. Please use a different email address.`
        }, { status: 400 })
      }

      // Check for other unique constraint violations
      if (error.code === '23505') {
        return NextResponse.json({
          error: 'Duplicate entry',
          message: 'A contact with this information already exists. Please check and try again.'
        }, { status: 400 })
      }

      return NextResponse.json({
        error: error.message || 'Failed to create contact',
        details: error.details || `Error code: ${error.code || 'UNKNOWN'}`,
        hint: error.hint || 'Check the contact field configuration to ensure all fields are properly set up',
        code: error.code
      }, { status: 500 })
    }

    return NextResponse.json(data, { status: 201 })
  } catch (error: any) {
    console.error('POST contact error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

export async function PUT(request: NextRequest) {
  try {
    const body = await request.json()
    const { id, companyId, accountId, ...contactData } = body

    if (!id) {
      return NextResponse.json({ error: 'Contact ID is required' }, { status: 400 })
    }

    // If companyId is provided, fetch enabled field configurations
    let enabledFieldNames: Set<string> | null = null
    if (companyId) {
      const { data: fieldConfigs, error: configError } = await supabase
        .from('contact_field_configurations')
        .select('field_name, is_enabled')
        .eq('company_id', companyId)
        .eq('is_enabled', true)

      if (!configError && fieldConfigs) {
        enabledFieldNames = new Set(
          fieldConfigs.map(config => config.field_name)
        )
      }
    }

    // Get account name if account_id is provided
    let companyName = contactData.company_name
    if (accountId) {
      const { data: accountData } = await supabase
        .from('accounts')
        .select('account_name')
        .eq('id', accountId)
        .single()

      if (accountData) {
        companyName = accountData.account_name
      }
    }

    // Clean the update data - convert empty strings to null
    const cleanedContactData: any = {}
    Object.keys(contactData).forEach(key => {
      // If we have field configurations, only process enabled fields
      if (enabledFieldNames && !enabledFieldNames.has(key)) {
        return // Skip this field if it's not enabled
      }

      const value = contactData[key]
      // Convert empty strings to null, keep other values as-is
      if (value === '' || value === undefined) {
        cleanedContactData[key] = null
      } else if (typeof value === 'string' && value.trim() === '') {
        cleanedContactData[key] = null
      } else {
        cleanedContactData[key] = value
      }
    })

    // Build update data
    const updateData: any = {
      ...cleanedContactData
    }

    // Always add account_id when provided (system field, not user-configurable)
    updateData.account_id = accountId || null

    // Only add company_name if field is enabled
    if ((!enabledFieldNames || enabledFieldNames.has('company_name')) && companyName) {
      updateData.company_name = companyName
    }

    const { data, error } = await supabase
      .from('contacts')
      .update(updateData)
      .eq('id', id)
      .select()
      .single()

    if (error) {
      console.error('Error updating contact:', error)
      console.error('Error details:', JSON.stringify(error, null, 2))
      console.error('Update data:', JSON.stringify(updateData, null, 2))

      // Check for duplicate email error
      if (error.code === '23505' && error.message.includes('contacts_email_company_unique')) {
        const email = updateData.email_primary || 'this email'
        return NextResponse.json({
          error: `Email already exists`,
          message: `A contact with the email "${email}" already exists in your system. Please use a different email address.`
        }, { status: 400 })
      }

      // Check for other unique constraint violations
      if (error.code === '23505') {
        return NextResponse.json({
          error: 'Duplicate entry',
          message: 'A contact with this information already exists. Please check and try again.'
        }, { status: 400 })
      }

      return NextResponse.json({
        error: error.message || 'Failed to update contact',
        details: error.details || `Error code: ${error.code || 'UNKNOWN'}`,
        hint: error.hint || 'Check the contact field configuration to ensure all fields are properly set up',
        code: error.code
      }, { status: 500 })
    }

    return NextResponse.json(data)
  } catch (error: any) {
    console.error('PUT contact error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

export async function DELETE(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams
    const id = searchParams.get('id')

    if (!id) {
      return NextResponse.json({ error: 'Contact ID is required' }, { status: 400 })
    }

    const { error } = await supabase
      .from('contacts')
      .delete()
      .eq('id', id)

    if (error) {
      console.error('Error deleting contact:', error)
      return NextResponse.json({ error: error.message }, { status: 500 })
    }

    return NextResponse.json({ message: 'Contact deleted successfully' })
  } catch (error: any) {
    console.error('DELETE contact error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
