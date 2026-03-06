import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

const supabase = createClient(supabaseUrl, supabaseServiceKey)

export async function GET(request: NextRequest) {
  try {
    // Get ALL deals with related data in a single query (optimized)
    let query = supabase
      .from('deals')
      .select(`
        *,
        deal_products:deal_products(
          id,
          product_id,
          product_name,
          quantity,
          price_per_unit,
          total_amount,
          notes
        )
      `)
      .order('created_at', { ascending: false })

    const { data: deals, error } = await query

    if (error) {
      console.error('Error fetching deals:', error)
      return NextResponse.json(
        { error: 'Failed to fetch deals' },
        { status: 500 }
      )
    }

    return NextResponse.json({ deals })
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
    const { selected_products, ...dealData } = body
    
    // Use default company ID if not provided
    const companyId = body.companyId || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'

    const dealToInsert = {
      ...dealData,
      company_id: companyId,
      created_by: 'System',
      updated_by: 'System'
    }

    // Remove selected_products from the main deal data and any undefined/null fields
    Object.keys(dealToInsert).forEach(key => {
      if (dealToInsert[key] === undefined || dealToInsert[key] === '' || key === 'selected_products') {
        if (key === 'selected_products') {
          delete dealToInsert[key]
        } else {
          dealToInsert[key] = null
        }
      }
    })

    const { data: deal, error } = await supabase
      .from('deals')
      .insert([dealToInsert])
      .select()
      .single()

    if (error) {
      console.error('Error creating deal:', error)
      console.error('Deal data that failed:', dealToInsert)
      console.error('Full error details:', JSON.stringify(error, null, 2))
      return NextResponse.json(
        {
          error: 'Failed to create deal',
          details: error.message || error.toString(),
          hint: error.hint,
          code: error.code
        },
        { status: 500 }
      )
    }

    // Insert selected products into deal_products table
    if (selected_products && selected_products.length > 0) {
      console.log('Inserting deal products:', selected_products)
      const dealProducts = selected_products.map((product: any) => ({
        deal_id: deal.id,
        product_id: product.product_id,
        product_name: product.product_name,
        quantity: product.quantity,
        price_per_unit: product.price_per_unit,
        notes: product.notes || null
      }))

      const { error: productsError } = await supabase
        .from('deal_products')
        .insert(dealProducts)

      if (productsError) {
        console.error('Error inserting deal products:', productsError)
        console.error('Deal products data that failed:', dealProducts)
        console.error('Full error details:', JSON.stringify(productsError, null, 2))
        // Don't fail the entire operation, just log the error
      } else {
        console.log('Successfully inserted deal products:', dealProducts.length, 'products')
      }
    }

    return NextResponse.json({ deal })
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

    const dealData = {
      ...updateData,
      updated_by: 'System'
    }

    const { data: deal, error } = await supabase
      .from('deals')
      .update(dealData)
      .eq('id', id)
      .select()
      .single()

    if (error) {
      console.error('Error updating deal:', error)
      return NextResponse.json(
        { error: 'Failed to update deal' },
        { status: 500 }
      )
    }

    // Auto-create activity if next_followup_date is set
    if (deal.next_followup_date && deal.contact_person) {
      try {
        await supabase.from('activities').insert({
          company_id: deal.company_id,
          activity_type: deal.stage === 'Proposal' ? 'demo' : 'call',
          title: `Follow up ${deal.stage} stage with ${deal.contact_person}`,
          description: `${deal.stage} stage: ${deal.product} deal worth ₹${deal.value?.toLocaleString()}`,
          entity_type: 'deal',
          entity_id: deal.id,
          entity_name: deal.account_name,
          contact_name: deal.contact_person,
          contact_phone: deal.phone,
          contact_email: deal.email,
          scheduled_date: deal.next_followup_date,
          due_date: deal.next_followup_date,
          priority: deal.probability >= 80 ? 'high' : 'medium',
          assigned_to: deal.assigned_to,
          status: 'pending'
        })
      } catch (activityError) {
        console.error('Error auto-creating deal activity:', activityError)
      }
    }

    return NextResponse.json({ deal })
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
        { error: 'Deal ID is required' },
        { status: 400 }
      )
    }

    const { error } = await supabase
      .from('deals')
      .delete()
      .eq('id', id)

    if (error) {
      console.error('Error deleting deal:', error)
      return NextResponse.json(
        { error: 'Failed to delete deal' },
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