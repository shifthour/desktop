import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || ''
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || ''

const supabase = createClient(supabaseUrl, supabaseKey)

// GET - Fetch stock transactions (audit trail)
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams
    const companyId = searchParams.get('companyId')
    const productId = searchParams.get('productId')
    const transactionType = searchParams.get('transaction_type')
    const limit = searchParams.get('limit') || '100'

    let query = supabase
      .from('stock_transactions')
      .select(`
        *,
        product:products(id, product_name, product_reference_no, category),
        performed_by_user:users(id, full_name, email)
      `)
      .order('transaction_date', { ascending: false })
      .limit(parseInt(limit))

    if (companyId) {
      query = query.eq('company_id', companyId)
    }

    if (productId) {
      query = query.eq('product_id', productId)
    }

    if (transactionType) {
      query = query.eq('transaction_type', transactionType)
    }

    const { data: transactions, error } = await query

    if (error) {
      console.error('Error fetching stock transactions:', error)
      return NextResponse.json({ error: 'Failed to fetch stock transactions' }, { status: 500 })
    }

    return NextResponse.json(transactions || [])
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}
