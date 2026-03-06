import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * GET /api/pricing/rules
 * List all pricing rules, ordered by priority.
 * Query params: ?active=true to filter active only
 */
export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)
    const activeOnly = searchParams.get('active') === 'true'

    let query = supabase
      .from('jc_pricing_rules')
      .select('*')
      .order('priority', { ascending: true })

    if (activeOnly) {
      query = query.eq('is_active', true)
    }

    const { data: rules, error } = await query

    if (error) throw error

    return NextResponse.json({ rules: rules || [] })
  } catch (error: any) {
    console.error('[Pricing Rules] GET error:', error.message)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

/**
 * POST /api/pricing/rules
 * Create a new pricing rule.
 * Body: { rule_name, rule_type, description, condition, multiplier, priority, applies_to_routes }
 */
export async function POST(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const body = await request.json()

    const { rule_name, rule_type, description, condition, multiplier, priority, applies_to_routes } = body

    if (!rule_name || !rule_type || !condition || multiplier === undefined) {
      return NextResponse.json(
        { error: 'rule_name, rule_type, condition, and multiplier are required' },
        { status: 400 }
      )
    }

    // Safety: validate multiplier bounds
    if (multiplier < 0.50 || multiplier > 2.00) {
      return NextResponse.json(
        { error: 'multiplier must be between 0.50 and 2.00' },
        { status: 400 }
      )
    }

    const { data, error } = await supabase
      .from('jc_pricing_rules')
      .insert({
        rule_name,
        rule_type,
        description: description || null,
        condition,
        multiplier,
        priority: priority || 10,
        is_active: true,
        applies_to_routes: applies_to_routes || [],
      })
      .select()
      .single()

    if (error) throw error

    return NextResponse.json({ success: true, rule: data })
  } catch (error: any) {
    console.error('[Pricing Rules] POST error:', error.message)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

/**
 * PUT /api/pricing/rules
 * Update an existing pricing rule.
 * Body: { id, ...fields_to_update }
 */
export async function PUT(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const body = await request.json()

    const { id, ...updates } = body

    if (!id) {
      return NextResponse.json({ error: 'id is required' }, { status: 400 })
    }

    // Safety: validate multiplier if being updated
    if (updates.multiplier !== undefined && (updates.multiplier < 0.50 || updates.multiplier > 2.00)) {
      return NextResponse.json(
        { error: 'multiplier must be between 0.50 and 2.00' },
        { status: 400 }
      )
    }

    const { data, error } = await supabase
      .from('jc_pricing_rules')
      .update(updates)
      .eq('id', id)
      .select()
      .single()

    if (error) throw error

    return NextResponse.json({ success: true, rule: data })
  } catch (error: any) {
    console.error('[Pricing Rules] PUT error:', error.message)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

/**
 * DELETE /api/pricing/rules?id=xxx
 * Soft-delete a pricing rule (set is_active = false)
 */
export async function DELETE(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)
    const id = searchParams.get('id')

    if (!id) {
      return NextResponse.json({ error: 'id query param is required' }, { status: 400 })
    }

    const { data, error } = await supabase
      .from('jc_pricing_rules')
      .update({ is_active: false })
      .eq('id', id)
      .select()
      .single()

    if (error) throw error

    return NextResponse.json({ success: true, rule: data })
  } catch (error: any) {
    console.error('[Pricing Rules] DELETE error:', error.message)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
