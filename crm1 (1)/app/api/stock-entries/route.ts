import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || ''
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || ''

const supabase = createClient(supabaseUrl, supabaseKey)

// GET - Fetch all stock entries
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams
    const entryType = searchParams.get('entry_type')
    const status = searchParams.get('status')
    const companyId = searchParams.get('companyId')

    let query = supabase
      .from('stock_entries')
      .select(`
        *,
        created_by_user:users!stock_entries_created_by_fkey(id, full_name, email),
        approved_by_user:users!stock_entries_approved_by_fkey(id, full_name, email),
        stock_entry_items(
          *,
          product:products(id, product_name, product_reference_no, category, stock_quantity)
        )
      `)
      .order('created_at', { ascending: false })

    if (companyId) {
      query = query.eq('company_id', companyId)
    }

    if (entryType) {
      query = query.eq('entry_type', entryType)
    }

    if (status) {
      query = query.eq('status', status)
    }

    const { data: entries, error } = await query

    if (error) {
      console.error('Error fetching stock entries:', error)
      return NextResponse.json({ error: 'Failed to fetch stock entries' }, { status: 500 })
    }

    return NextResponse.json(entries || [])
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

// POST - Create new stock entry
export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const { companyId, userId, items, ...entryData } = body

    if (!companyId || !userId) {
      return NextResponse.json({ error: 'Company ID and User ID are required' }, { status: 400 })
    }

    if (!items || items.length === 0) {
      return NextResponse.json({ error: 'At least one item is required' }, { status: 400 })
    }

    // Generate entry number
    const { data: entryNumber, error: entryNumberError } = await supabase
      .rpc('generate_entry_number', {
        p_entry_type: entryData.entry_type,
        p_company_id: companyId
      })

    if (entryNumberError) {
      console.error('Error generating entry number:', entryNumberError)
      return NextResponse.json({ error: 'Failed to generate entry number' }, { status: 500 })
    }

    // Create stock entry
    const stockEntryToInsert = {
      ...entryData,
      company_id: companyId,
      entry_number: entryNumber,
      created_by: userId,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    }

    const { data: stockEntry, error: entryError } = await supabase
      .from('stock_entries')
      .insert([stockEntryToInsert])
      .select()
      .single()

    if (entryError) {
      console.error('Error creating stock entry:', entryError)
      return NextResponse.json({ error: 'Failed to create stock entry', details: entryError }, { status: 500 })
    }

    // Insert stock entry items
    const itemsToInsert = items.map((item: any) => ({
      ...item,
      stock_entry_id: stockEntry.id,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    }))

    const { data: insertedItems, error: itemsError } = await supabase
      .from('stock_entry_items')
      .insert(itemsToInsert)
      .select()

    if (itemsError) {
      console.error('Error creating stock entry items:', itemsError)
      // Rollback: delete the stock entry
      await supabase.from('stock_entries').delete().eq('id', stockEntry.id)
      return NextResponse.json({ error: 'Failed to create stock entry items' }, { status: 500 })
    }

    return NextResponse.json({
      ...stockEntry,
      stock_entry_items: insertedItems
    })
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

// PUT - Update stock entry
export async function PUT(request: NextRequest) {
  try {
    const body = await request.json()
    const { id, companyId, userId, items, ...entryData } = body

    if (!id || !companyId) {
      return NextResponse.json({ error: 'Entry ID and Company ID are required' }, { status: 400 })
    }

    const entryToUpdate = {
      ...entryData,
      updated_by: userId,
      updated_at: new Date().toISOString()
    }

    const { data: stockEntry, error: entryError } = await supabase
      .from('stock_entries')
      .update(entryToUpdate)
      .eq('id', id)
      .eq('company_id', companyId)
      .select()
      .single()

    if (entryError) {
      console.error('Error updating stock entry:', entryError)
      return NextResponse.json({ error: 'Failed to update stock entry' }, { status: 500 })
    }

    // Update items if provided
    if (items && items.length > 0) {
      // Delete existing items
      await supabase
        .from('stock_entry_items')
        .delete()
        .eq('stock_entry_id', id)

      // Insert new items
      const itemsToInsert = items.map((item: any) => ({
        ...item,
        stock_entry_id: id,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      }))

      const { data: insertedItems, error: itemsError } = await supabase
        .from('stock_entry_items')
        .insert(itemsToInsert)
        .select()

      if (itemsError) {
        console.error('Error updating stock entry items:', itemsError)
        return NextResponse.json({ error: 'Failed to update stock entry items' }, { status: 500 })
      }

      return NextResponse.json({
        ...stockEntry,
        stock_entry_items: insertedItems
      })
    }

    return NextResponse.json(stockEntry)
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

// DELETE - Delete stock entry
export async function DELETE(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams
    const id = searchParams.get('id')
    const companyId = searchParams.get('companyId')

    if (!id || !companyId) {
      return NextResponse.json({ error: 'Entry ID and Company ID are required' }, { status: 400 })
    }

    // Check if entry is approved (should not delete approved entries)
    const { data: entry, error: checkError } = await supabase
      .from('stock_entries')
      .select('status')
      .eq('id', id)
      .single()

    if (checkError) {
      console.error('Error checking stock entry:', checkError)
      return NextResponse.json({ error: 'Failed to check stock entry' }, { status: 500 })
    }

    if (entry.status === 'approved' || entry.status === 'completed') {
      return NextResponse.json({
        error: 'Cannot delete approved or completed stock entries'
      }, { status: 400 })
    }

    const { error } = await supabase
      .from('stock_entries')
      .delete()
      .eq('id', id)
      .eq('company_id', companyId)

    if (error) {
      console.error('Error deleting stock entry:', error)
      return NextResponse.json({ error: 'Failed to delete stock entry' }, { status: 500 })
    }

    return NextResponse.json({ message: 'Stock entry deleted successfully' })
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}
