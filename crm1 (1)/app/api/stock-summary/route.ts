import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || ''
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || ''

const supabase = createClient(supabaseUrl, supabaseKey)

// GET - Fetch stock summary with alerts
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams
    const companyId = searchParams.get('companyId')
    const stockStatus = searchParams.get('stock_status') // 'critical', 'low', 'adequate'

    // Fetch ALL products directly from products table
    let query = supabase
      .from('products')
      .select('id, product_name, product_reference_no, category, stock_quantity, min_stock_level, reorder_level, price, bin_location')
      .order('product_name', { ascending: true })

    if (companyId) {
      query = query.eq('company_id', companyId)
    }

    const { data: products, error } = await query

    if (error) {
      console.error('Error fetching products:', error)
      return NextResponse.json({ error: 'Failed to fetch products' }, { status: 500 })
    }

    // Calculate stock status for each product
    const summary = products?.map(product => {
      const stockQty = product.stock_quantity || 0
      const minLevel = product.min_stock_level || 0
      const reorderLevel = product.reorder_level || 0

      let stock_status = 'adequate'
      if (stockQty === 0) {
        stock_status = 'critical'
      } else if (minLevel > 0 && stockQty <= minLevel) {
        stock_status = 'critical'
      } else if (reorderLevel > 0 && stockQty <= reorderLevel) {
        stock_status = 'low'
      }

      return {
        product_id: product.id,
        product_name: product.product_name,
        product_reference_no: product.product_reference_no,
        category: product.category,
        stock_quantity: stockQty,
        min_stock_level: minLevel,
        reorder_level: reorderLevel,
        stock_status,
        price: product.price || 0,
        bin_location: product.bin_location || null
      }
    }) || []

    // Filter by stock status if requested
    const filteredSummary = stockStatus
      ? summary.filter(s => s.stock_status === stockStatus)
      : summary

    // Calculate statistics
    const stats = {
      total_products: summary.length,
      critical_stock: summary.filter(s => s.stock_status === 'critical').length,
      low_stock: summary.filter(s => s.stock_status === 'low').length,
      adequate_stock: summary.filter(s => s.stock_status === 'adequate').length,
      total_stock_value: summary.reduce((acc, s) => acc + (s.stock_quantity * s.price), 0)
    }

    return NextResponse.json({
      summary: filteredSummary,
      stats
    })
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}
