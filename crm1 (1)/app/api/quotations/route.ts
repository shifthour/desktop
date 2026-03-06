import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

const supabase = createClient(supabaseUrl, supabaseServiceKey)

export async function GET(request: NextRequest) {
  try {
    // Get companyId from URL params
    const { searchParams } = new URL(request.url)
    const companyId = searchParams.get('companyId') || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
    
    let query = supabase
      .from('quotations')
      .select('*')
      .eq('company_id', companyId)
      .order('created_at', { ascending: false })

    const { data: quotations, error } = await query

    if (error) {
      console.error('Error fetching quotations:', error)
      return NextResponse.json(
        { error: 'Failed to fetch quotations' },
        { status: 500 }
      )
    }

    console.log(`Fetched ${quotations?.length || 0} quotations for company ${companyId}`)
    if (quotations && quotations.length > 0) {
      console.log('Latest quotation:', {
        id: quotations[0].id,
        quote_number: quotations[0].quote_number,
        customer_name: quotations[0].customer_name,
        created_at: quotations[0].created_at
      })
    }

    return NextResponse.json({ quotations })
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
    console.log('POST /api/quotations - received body keys:', Object.keys(body))
    console.log('POST /api/quotations - body sample:', JSON.stringify(body, null, 2).substring(0, 500))
    
    // Filter out any fields that don't belong in the quotations table
    const allowedFields = [
      'quote_number', 'quote_date', 'valid_until', 'customer_name', 'contact_person',
      'customer_email', 'customer_phone', 'billing_address', 'shipping_address',
      'products_quoted', 'total_amount', 'subtotal_amount', 'tax_amount', 'discount_amount',
      'currency', 'status', 'assigned_to', 'priority', 'reference_number', 'subject',
      'notes', 'terms_conditions', 'payment_terms', 'delivery_terms', 'tax_type', 'line_items'
    ]
    
    const filteredData = {}
    allowedFields.forEach(field => {
      if (body[field] !== undefined) {
        filteredData[field] = body[field]
      }
    })
    
    const companyId = body.companyId || body.company_id || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'

    const quotationData = {
      ...filteredData,
      company_id: companyId,
      created_by: 'System',
      updated_by: 'System'
    }

    console.log('POST /api/quotations - sending to Supabase:', JSON.stringify(quotationData, null, 2).substring(0, 500))

    const { data: quotation, error } = await supabase
      .from('quotations')
      .insert([quotationData])
      .select()
      .single()

    if (error) {
      console.error('Error creating quotation:', error)
      console.error('Failed quotation data keys:', Object.keys(quotationData))
      return NextResponse.json(
        { error: 'Failed to create quotation', details: error.message },
        { status: 500 }
      )
    }

    console.log('Successfully created quotation:', quotation.quote_number || quotation.id)
    return NextResponse.json({ quotation })
  } catch (error) {
    console.error('Unexpected error in POST /api/quotations:', error)
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

    const quotationData = {
      ...updateData,
      updated_by: 'System'
    }

    const { data: quotation, error } = await supabase
      .from('quotations')
      .update(quotationData)
      .eq('id', id)
      .select()
      .single()

    if (error) {
      console.error('Error updating quotation:', error)
      return NextResponse.json(
        { error: 'Failed to update quotation' },
        { status: 500 }
      )
    }

    return NextResponse.json({ quotation })
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
        { error: 'Quotation ID is required' },
        { status: 400 }
      )
    }

    const { error } = await supabase
      .from('quotations')
      .delete()
      .eq('id', id)

    if (error) {
      console.error('Error deleting quotation:', error)
      return NextResponse.json(
        { error: 'Failed to delete quotation' },
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