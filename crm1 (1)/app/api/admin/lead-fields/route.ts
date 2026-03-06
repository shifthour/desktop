import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

const supabase = createClient(supabaseUrl, supabaseServiceKey)

// GET - Fetch lead field configurations for a company
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const companyId = searchParams.get('companyId')

    if (!companyId) {
      return NextResponse.json({ error: 'Company ID is required' }, { status: 400 })
    }

    const { data, error } = await supabase
      .from('lead_field_configurations')
      .select('*')
      .eq('company_id', companyId)
      .order('field_section', { ascending: true })
      .order('display_order', { ascending: true })

    if (error) {
      console.error('Error fetching lead field configurations:', error)
      return NextResponse.json({ error: error.message }, { status: 500 })
    }

    return NextResponse.json(data || [])
  } catch (error: any) {
    console.error('GET lead fields error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

// PUT - Bulk update lead field configurations (e.g., enable/disable, reorder)
export async function PUT(request: NextRequest) {
  try {
    const body = await request.json()
    const { companyId, fieldConfigs } = body

    if (!companyId || !fieldConfigs || !Array.isArray(fieldConfigs)) {
      return NextResponse.json({ error: 'Company ID and field configurations array are required' }, { status: 400 })
    }

    // Update each field configuration
    const updates = fieldConfigs.map(async (config) => {
      const updateData: any = {
        is_enabled: config.is_enabled,
        display_order: config.display_order,
        field_label: config.field_label,
        placeholder: config.placeholder,
        help_text: config.help_text,
        updated_at: new Date().toISOString()
      }

      // Include is_mandatory if provided
      if (config.is_mandatory !== undefined) {
        updateData.is_mandatory = config.is_mandatory
      }

      const { data, error } = await supabase
        .from('lead_field_configurations')
        .update(updateData)
        .eq('company_id', companyId)
        .eq('field_name', config.field_name)
        .select()
        .single()

      if (error) {
        console.error(`Error updating field ${config.field_name}:`, error)
        throw error
      }

      return data
    })

    const results = await Promise.all(updates)

    return NextResponse.json({
      success: true,
      updated: results.length,
      data: results
    })
  } catch (error: any) {
    console.error('PUT lead fields error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
