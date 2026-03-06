import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)
    const search = searchParams.get('search') || ''
    const countryId = searchParams.get('countryId') || ''

    let query = supabase
      .from('jc_states')
      .select(`
        *,
        jc_countries (id, country_name, country_code)
      `, { count: 'exact' })
      .eq('is_active', true)
      .order('state_name', { ascending: true })

    if (search) {
      query = query.or(`state_name.ilike.%${search}%,state_code.ilike.%${search}%`)
    }

    if (countryId) {
      query = query.eq('country_id', countryId)
    }

    const { data, error, count } = await query

    if (error) throw error

    return NextResponse.json({ states: data || [], total: count || 0 })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

export async function POST(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const body = await request.json()

    if (!body.state_code || !body.state_name || !body.country_id) {
      return NextResponse.json({ error: 'State code, name and country are required' }, { status: 400 })
    }

    const { data, error } = await supabase
      .from('jc_states')
      .insert({
        country_id: body.country_id,
        state_code: body.state_code.toUpperCase(),
        state_name: body.state_name,
      })
      .select()
      .single()

    if (error) throw error

    return NextResponse.json({ state: data, message: 'State created successfully' })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
