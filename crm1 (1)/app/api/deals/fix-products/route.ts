import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

const supabase = createClient(supabaseUrl, supabaseServiceKey)

export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const { deal_id, products } = body

    if (!deal_id || !products || !Array.isArray(products)) {
      return NextResponse.json(
        { error: 'deal_id and products array are required' },
        { status: 400 }
      )
    }

    console.log(`Manually fixing deal ${deal_id} with ${products.length} products`)

    // First, delete any existing products for this deal to avoid duplicates
    await supabase
      .from('deal_products')
      .delete()
      .eq('deal_id', deal_id)

    // Insert the new products
    const dealProducts = products.map((product: any) => ({
      deal_id: deal_id,
      product_id: product.product_id || null,
      product_name: product.product_name,
      quantity: product.quantity,
      price_per_unit: product.price_per_unit,
      notes: product.notes || null
    }))

    const { error } = await supabase
      .from('deal_products')
      .insert(dealProducts)

    if (error) {
      console.error('Error manually fixing deal products:', error)
      return NextResponse.json(
        { error: 'Failed to insert deal products', details: error.message },
        { status: 500 }
      )
    }

    console.log(`Successfully fixed deal ${deal_id} with ${dealProducts.length} products`)

    return NextResponse.json({ 
      success: true, 
      message: `Fixed deal ${deal_id} with ${dealProducts.length} products` 
    })
  } catch (error) {
    console.error('Unexpected error in fix-products:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}