import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
)

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id: dealId } = await params

    if (!dealId) {
      return NextResponse.json({ error: 'Deal ID is required' }, { status: 400 })
    }

    // Get products for this deal from deal_products junction table
    const { data: dealProducts, error } = await supabase
      .from('deal_products')
      .select('*')
      .eq('deal_id', dealId)
      .order('created_at', { ascending: true })

    if (error) {
      console.error('Error fetching deal products:', error)
      return NextResponse.json({ error: 'Failed to fetch deal products' }, { status: 500 })
    }

    return NextResponse.json(dealProducts || [])
  } catch (error) {
    console.error('Error in deal products GET:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}