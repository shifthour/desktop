import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || ''
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || ''

const supabase = createClient(supabaseUrl, supabaseKey)

// POST - Approve stock entry
export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const { id, companyId, userId } = body

    if (!id || !companyId || !userId) {
      return NextResponse.json({
        error: 'Entry ID, Company ID, and User ID are required'
      }, { status: 400 })
    }

    // Check if entry exists and is in correct status
    const { data: entry, error: checkError } = await supabase
      .from('stock_entries')
      .select('*, stock_entry_items(*)')
      .eq('id', id)
      .eq('company_id', companyId)
      .single()

    if (checkError) {
      console.error('Error checking stock entry:', checkError)
      return NextResponse.json({ error: 'Failed to check stock entry' }, { status: 500 })
    }

    if (!entry) {
      return NextResponse.json({ error: 'Stock entry not found' }, { status: 404 })
    }

    if (entry.status === 'approved' || entry.status === 'completed') {
      return NextResponse.json({
        error: 'Stock entry is already approved'
      }, { status: 400 })
    }

    if (!entry.stock_entry_items || entry.stock_entry_items.length === 0) {
      return NextResponse.json({
        error: 'Cannot approve entry without items'
      }, { status: 400 })
    }

    // Validate stock availability for outward entries
    if (entry.entry_type === 'outward') {
      for (const item of entry.stock_entry_items) {
        const { data: product, error: prodError } = await supabase
          .from('products')
          .select('product_name, stock_quantity')
          .eq('id', item.product_id)
          .single()

        if (prodError || !product) {
          return NextResponse.json({
            error: `Product not found: ${item.product_id}`
          }, { status: 400 })
        }

        const currentStock = product.stock_quantity || 0
        if (item.quantity > currentStock) {
          return NextResponse.json({
            error: `Insufficient stock for ${product.product_name}. Available: ${currentStock}, Requested: ${item.quantity}`
          }, { status: 400 })
        }
      }
    }

    // Update entry status to approved
    const { data: updatedEntry, error: updateError } = await supabase
      .from('stock_entries')
      .update({
        status: 'approved',
        approved_by: userId,
        approved_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      })
      .eq('id', id)
      .eq('company_id', companyId)
      .select()
      .single()

    if (updateError) {
      console.error('Error approving stock entry:', updateError)
      return NextResponse.json({ error: 'Failed to approve stock entry' }, { status: 500 })
    }

    // The trigger will automatically update stock quantities and create transactions

    return NextResponse.json({
      message: 'Stock entry approved successfully',
      entry: updatedEntry
    })
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}
