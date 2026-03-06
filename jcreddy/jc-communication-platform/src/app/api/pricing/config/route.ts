import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * GET /api/pricing/config
 * Fetch all active pricing configuration entries
 */
export async function GET() {
  try {
    const supabase = createServerClient()

    const { data: configs, error } = await supabase
      .from('jc_pricing_config')
      .select('*')
      .eq('is_active', true)
      .order('config_key', { ascending: true })

    if (error) throw error

    // Build a key-value map for easy access
    const configMap: Record<string, any> = {}
    for (const c of configs || []) {
      configMap[c.config_key] = c.config_value
    }

    return NextResponse.json({
      configs: configs || [],
      configMap,
    })
  } catch (error: any) {
    console.error('[Pricing Config] GET error:', error.message)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

/**
 * PUT /api/pricing/config
 * Update a config entry by config_key
 * Body: { config_key: string, config_value: object }
 */
export async function PUT(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const body = await request.json()

    const { config_key, config_value } = body

    if (!config_key || !config_value) {
      return NextResponse.json(
        { error: 'config_key and config_value are required' },
        { status: 400 }
      )
    }

    const { data, error } = await supabase
      .from('jc_pricing_config')
      .update({ config_value })
      .eq('config_key', config_key)
      .select()
      .single()

    if (error) throw error

    return NextResponse.json({ success: true, config: data })
  } catch (error: any) {
    console.error('[Pricing Config] PUT error:', error.message)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
