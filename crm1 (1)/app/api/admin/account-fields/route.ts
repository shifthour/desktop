import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

const supabase = createClient(supabaseUrl, supabaseServiceKey)

// GET - Fetch field configurations for a company
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const companyId = searchParams.get('companyId')

    if (!companyId) {
      return NextResponse.json({ error: 'Company ID is required' }, { status: 400 })
    }

    const { data, error } = await supabase
      .from('account_field_configurations')
      .select('*')
      .eq('company_id', companyId)
      .order('field_section', { ascending: true })
      .order('display_order', { ascending: true })

    if (error) {
      console.error('Error fetching field configurations:', error)
      return NextResponse.json({ error: error.message }, { status: 500 })
    }

    return NextResponse.json(data || [])
  } catch (error: any) {
    console.error('GET account fields error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

// POST - Create or update field configuration
export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const { companyId, fieldConfig } = body

    if (!companyId || !fieldConfig) {
      return NextResponse.json({ error: 'Company ID and field configuration are required' }, { status: 400 })
    }

    // Upsert the field configuration
    const { data, error } = await supabase
      .from('account_field_configurations')
      .upsert({
        company_id: companyId,
        ...fieldConfig,
        updated_at: new Date().toISOString()
      }, {
        onConflict: 'company_id,field_name'
      })
      .select()
      .single()

    if (error) {
      console.error('Error saving field configuration:', error)
      return NextResponse.json({ error: error.message }, { status: 500 })
    }

    return NextResponse.json(data)
  } catch (error: any) {
    console.error('POST account field error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

// PUT - Bulk update field configurations (e.g., enable/disable, reorder)
export async function PUT(request: NextRequest) {
  try {
    const body = await request.json()
    const { companyId, fieldConfigs } = body

    if (!companyId || !fieldConfigs || !Array.isArray(fieldConfigs)) {
      return NextResponse.json({ error: 'Company ID and field configurations array are required' }, { status: 400 })
    }

    // Update each field configuration
    const updates = fieldConfigs.map(async (config) => {
      const { data, error } = await supabase
        .from('account_field_configurations')
        .update({
          is_enabled: config.is_enabled,
          display_order: config.display_order,
          field_label: config.field_label,
          placeholder: config.placeholder,
          help_text: config.help_text,
          updated_at: new Date().toISOString()
        })
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
    console.error('PUT account fields error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

// DELETE - Remove field configuration (soft delete by disabling)
export async function DELETE(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const companyId = searchParams.get('companyId')
    const fieldName = searchParams.get('fieldName')

    if (!companyId || !fieldName) {
      return NextResponse.json({ error: 'Company ID and field name are required' }, { status: 400 })
    }

    // Check if field is mandatory
    const { data: field } = await supabase
      .from('account_field_configurations')
      .select('is_mandatory')
      .eq('company_id', companyId)
      .eq('field_name', fieldName)
      .single()

    if (field?.is_mandatory) {
      return NextResponse.json({ error: 'Cannot delete mandatory field' }, { status: 400 })
    }

    // Soft delete by disabling
    const { error } = await supabase
      .from('account_field_configurations')
      .update({ is_enabled: false, updated_at: new Date().toISOString() })
      .eq('company_id', companyId)
      .eq('field_name', fieldName)

    if (error) {
      console.error('Error deleting field configuration:', error)
      return NextResponse.json({ error: error.message }, { status: 500 })
    }

    return NextResponse.json({ success: true })
  } catch (error: any) {
    console.error('DELETE account field error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
