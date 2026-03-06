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
    const { id: leadId } = await params

    if (!leadId) {
      return NextResponse.json({ error: 'Lead ID is required' }, { status: 400 })
    }

    // Get products for this lead from lead_products junction table
    const { data: leadProducts, error } = await supabase
      .from('lead_products')
      .select('*')
      .eq('lead_id', leadId)
      .order('created_at', { ascending: true })

    if (error) {
      console.error('Error fetching lead products:', error)
      return NextResponse.json({ error: 'Failed to fetch lead products' }, { status: 500 })
    }

    return NextResponse.json({ products: leadProducts || [] })
  } catch (error) {
    console.error('Error in lead products GET:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}