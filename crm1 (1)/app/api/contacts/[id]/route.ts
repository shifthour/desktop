import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

const supabase = createClient(supabaseUrl, supabaseServiceKey)

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params

    const { data, error } = await supabase
      .from('contacts')
      .select('*')
      .eq('id', id)
      .single()

    if (error) {
      console.error('Error fetching contact:', error)
      return NextResponse.json({ error: error.message }, { status: 500 })
    }

    if (!data) {
      return NextResponse.json({ error: 'Contact not found' }, { status: 404 })
    }

    return NextResponse.json(data)
  } catch (error: any) {
    console.error('GET contact error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

export async function DELETE(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    console.log('=== DELETE /api/contacts/[id] - REQUEST RECEIVED ===')
    const { id } = await params
    console.log('Contact ID:', id)

    if (!id) {
      console.log('Error: Contact ID is missing')
      return NextResponse.json({ error: 'Contact ID is required' }, { status: 400 })
    }

    console.log('Attempting to delete contact:', id)

    const { error } = await supabase
      .from('contacts')
      .delete()
      .eq('id', id)

    if (error) {
      console.error('Error deleting contact from database:', error)
      console.error('Error details:', JSON.stringify(error, null, 2))
      return NextResponse.json({ error: error.message }, { status: 500 })
    }

    console.log('Contact deleted successfully:', id)
    return NextResponse.json({ success: true, message: 'Contact deleted successfully' })

  } catch (error: any) {
    console.error('DELETE contact error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
