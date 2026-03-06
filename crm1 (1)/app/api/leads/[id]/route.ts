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
    const { id } = await params

    if (!id) {
      return NextResponse.json({ error: 'Lead ID is required' }, { status: 400 })
    }

    const { data: lead, error } = await supabase
      .from('leads')
      .select('*')
      .eq('id', id)
      .single()

    if (error) {
      console.error('Error fetching lead:', error)
      return NextResponse.json({ error: 'Failed to fetch lead' }, { status: 500 })
    }

    if (!lead) {
      return NextResponse.json({ error: 'Lead not found' }, { status: 404 })
    }

    // Merge custom_fields back into the main lead object
    let leadResponse = lead
    if (lead.custom_fields && typeof lead.custom_fields === 'object') {
      const { custom_fields, ...restLead } = lead
      leadResponse = { ...restLead, ...custom_fields }
    }

    // Map database IDs to form field names for select_dependent compatibility
    // The DynamicLeadField component expects IDs for select_dependent fields
    if (leadResponse.account_id) {
      leadResponse.account = leadResponse.account_id
    }
    if (leadResponse.contact_id) {
      leadResponse.contact = leadResponse.contact_id
    }

    return NextResponse.json(leadResponse)
  } catch (error) {
    console.error('Error in lead GET:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}
