import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

const supabase = createClient(supabaseUrl, supabaseServiceKey)

export async function GET(request: NextRequest) {
  try {
    let query = supabase
      .from('invoices')
      .select('*')
      .order('created_at', { ascending: false })

    const { data: invoices, error } = await query

    if (error) {
      console.error('Error fetching invoices:', error)
      return NextResponse.json(
        { error: 'Failed to fetch invoices' },
        { status: 500 }
      )
    }

    return NextResponse.json({ invoices })
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
    
    console.log("=== API RECEIVED DATA ===")
    console.log("Body:", body)
    console.log("Body keys:", Object.keys(body))
    console.log("accountName in body?", 'accountName' in body)
    
    const companyId = body.companyId || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'

    // Extract and process the data properly
    const { accountName, items, totals, ...cleanBody } = body
    
    // Convert items array to a simple string representation for the database
    const itemsDescription = items && items.length > 0 
      ? items.map((item: any) => item.product || item.description).filter(Boolean).join(', ')
      : 'Invoice Items'
    
    const invoiceData = {
      ...cleanBody,
      items: itemsDescription, // Store as simple string
      subtotal_amount: totals?.subtotal || 0,
      tax_amount: totals?.totalTax || 0,
      discount_amount: totals?.totalDiscount || 0,
      company_id: companyId,
      created_by: 'f41e509a-4c92-4baa-bca2-ab1d3410e465', // Default system user ID
      updated_by: 'f41e509a-4c92-4baa-bca2-ab1d3410e465'
    }
    
    console.log("=== CLEANED INVOICE DATA ===")
    console.log("invoiceData:", invoiceData)
    console.log("invoiceData keys:", Object.keys(invoiceData))
    console.log("accountName in invoiceData?", 'accountName' in invoiceData)

    const { data: invoice, error } = await supabase
      .from('invoices')
      .insert([invoiceData])
      .select()
      .single()

    if (error) {
      console.error('Error creating invoice:', error)
      return NextResponse.json(
        { error: 'Failed to create invoice' },
        { status: 500 }
      )
    }

    return NextResponse.json({ invoice })
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

    const invoiceData = {
      ...updateData,
      updated_by: 'f41e509a-4c92-4baa-bca2-ab1d3410e465'
    }

    const { data: invoice, error } = await supabase
      .from('invoices')
      .update(invoiceData)
      .eq('id', id)
      .select()
      .single()

    if (error) {
      console.error('Error updating invoice:', error)
      return NextResponse.json(
        { error: 'Failed to update invoice' },
        { status: 500 }
      )
    }

    return NextResponse.json({ invoice })
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
        { error: 'Invoice ID is required' },
        { status: 400 }
      )
    }

    const { error } = await supabase
      .from('invoices')
      .delete()
      .eq('id', id)

    if (error) {
      console.error('Error deleting invoice:', error)
      return NextResponse.json(
        { error: 'Failed to delete invoice' },
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