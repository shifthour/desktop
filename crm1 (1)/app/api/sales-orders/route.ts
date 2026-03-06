import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

const supabase = createClient(supabaseUrl, supabaseServiceKey)

export async function GET(request: NextRequest) {
  try {
    let query = supabase
      .from('sales_orders')
      .select('*')
      .order('created_at', { ascending: false })

    const { data: salesOrders, error } = await query

    if (error) {
      console.error('Error fetching sales orders:', error)
      return NextResponse.json(
        { error: 'Failed to fetch sales orders' },
        { status: 500 }
      )
    }

    return NextResponse.json({ salesOrders })
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
    
    const companyId = body.companyId || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
    
    // Remove fields that shouldn't be sent to database
    const { created_by, updated_by, ...cleanBody } = body

    const salesOrderData = {
      ...cleanBody,
      company_id: companyId
    }

    const { data: salesOrder, error } = await supabase
      .from('sales_orders')
      .insert([salesOrderData])
      .select()
      .single()

    if (error) {
      console.error('Error creating sales order:', error)
      return NextResponse.json(
        { error: 'Failed to create sales order' },
        { status: 500 }
      )
    }

    return NextResponse.json({ salesOrder })
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
    const { id, created_by, updated_by, ...updateData } = body

    const salesOrderData = {
      ...updateData
    }

    const { data: salesOrder, error } = await supabase
      .from('sales_orders')
      .update(salesOrderData)
      .eq('id', id)
      .select()
      .single()

    if (error) {
      console.error('Error updating sales order:', error)
      return NextResponse.json(
        { error: 'Failed to update sales order' },
        { status: 500 }
      )
    }

    return NextResponse.json({ salesOrder })
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
        { error: 'Sales order ID is required' },
        { status: 400 }
      )
    }

    const { error } = await supabase
      .from('sales_orders')
      .delete()
      .eq('id', id)

    if (error) {
      console.error('Error deleting sales order:', error)
      return NextResponse.json(
        { error: 'Failed to delete sales order' },
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