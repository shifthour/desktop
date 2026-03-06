import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

const supabase = createClient(supabaseUrl, supabaseServiceKey)

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const companyId = searchParams.get('companyId') || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'

    // First, let's check the table structure
    const { data: tableInfo, error: tableError } = await supabase
      .rpc('get_table_columns', { table_name: 'complaints' })
      .single()

    if (tableError) {
      console.log('Table structure check failed, trying simple select:', tableError)
    } else {
      console.log('Complaints table structure:', tableInfo)
    }

    // Try a simple select first to see what columns exist
    const { data: sampleData, error: sampleError } = await supabase
      .from('complaints')
      .select('*')
      .limit(1)

    if (sampleError) {
      console.error('Error with simple select:', sampleError)
    } else {
      console.log('Sample complaint data structure:', sampleData)
    }

    let query = supabase
      .from('complaints')
      .select(`
        *,
        installation:installations(customer_name, product_name, installation_address),
        amc_contract:amc_contracts(amc_number, customer_name)
      `)
      .eq('company_id', companyId)
      .order('created_at', { ascending: false })

    const { data: complaints, error } = await query

    if (error) {
      console.error('Error fetching complaints:', error)
      return NextResponse.json(
        { error: 'Failed to fetch complaints' },
        { status: 500 }
      )
    }

    return NextResponse.json({ complaints })
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    
    console.log('=== COMPLAINT CREATION DEBUG ===')
    console.log('Received body:', body)
    console.log('Body keys:', Object.keys(body))
    
    const companyId = body.companyId || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
    const userId = 'f41e509a-4c92-4baa-bca2-ab1d3410e465' // Default user ID

    // Remove companyId from body and map fields to match database schema
    const { 
      companyId: _, 
      customer_name,
      complaint_description,
      complaint_title,
      contact_person,
      customer_phone,
      customer_email,
      complaint_type,
      warranty_status,
      product_name,
      product_model,
      serial_number,
      severity,
      priority,
      status,
      assigned_to,
      installation_id,
      amc_contract_id,
      source_reference,
      ...otherFields 
    } = body
    
    // Map complaint type to database values
    let mappedComplaintType = 'No Warranty/AMC' // default
    if (warranty_status === 'Under Warranty') {
      mappedComplaintType = 'Under Warranty'
    } else if (amc_contract_id) {
      mappedComplaintType = 'Under AMC'
    }
    
    const complaintData = {
      // Map form fields to database column names
      account_name: customer_name || '',
      description: complaint_description || '',
      subject: complaint_title || '',
      contact_person: contact_person || '',
      contact_phone: customer_phone || '',
      contact_email: customer_email || '',
      complaint_type: mappedComplaintType,
      
      // Include other fields that match, excluding fields that don't exist in DB
      installation_id: installation_id || null,
      amc_contract_id: amc_contract_id || null,
      source_reference: source_reference || null,
      product_name: product_name || 'Unknown Product', // Ensure product_name is never null
      serial_number: serial_number || '',
      model_number: product_model || null,
      severity: severity || 'Medium',
      priority: priority || 'Medium',
      status: status === 'Open' ? 'New' : (status || 'New'),
      assigned_to: assigned_to || null,
      
      // System fields
      company_id: companyId,
      created_by: userId,
      updated_by: userId,
      
      // Generate complaint number and date
      complaint_date: new Date().toISOString().split('T')[0],
      complaint_number: `COMP-${Date.now()}`
    }
    
    console.log('Complaint data to insert:', complaintData)
    console.log('Complaint data keys:', Object.keys(complaintData))

    const { data: complaint, error } = await supabase
      .from('complaints')
      .insert([complaintData])
      .select()
      .single()

    if (error) {
      console.error('Error creating complaint:', error)
      return NextResponse.json(
        { error: 'Failed to create complaint' },
        { status: 500 }
      )
    }

    return NextResponse.json({ complaint })
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}

export async function PUT(request: NextRequest) {
  try {
    const body = await request.json()
    const { id, ...updateData } = body
    const userId = 'f41e509a-4c92-4baa-bca2-ab1d3410e465' // Default user ID

    const complaintData = {
      ...updateData,
      updated_by: userId
    }

    const { data: complaint, error } = await supabase
      .from('complaints')
      .update(complaintData)
      .eq('id', id)
      .select()
      .single()

    if (error) {
      console.error('Error updating complaint:', error)
      return NextResponse.json(
        { error: 'Failed to update complaint' },
        { status: 500 }
      )
    }

    return NextResponse.json({ complaint })
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}

export async function DELETE(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const id = searchParams.get('id')

    if (!id) {
      return NextResponse.json(
        { error: 'Complaint ID is required' },
        { status: 400 }
      )
    }

    const { error } = await supabase
      .from('complaints')
      .delete()
      .eq('id', id)

    if (error) {
      console.error('Error deleting complaint:', error)
      return NextResponse.json(
        { error: 'Failed to delete complaint' },
        { status: 500 }
      )
    }

    return NextResponse.json({ success: true })
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}