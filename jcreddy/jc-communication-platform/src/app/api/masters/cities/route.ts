import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)
    const search = searchParams.get('search') || ''
    const stateId = searchParams.get('stateId') || ''
    const page = parseInt(searchParams.get('page') || '1')
    const limit = parseInt(searchParams.get('limit') || '50')

    let query = supabase
      .from('jc_cities')
      .select(`
        *,
        jc_states (id, state_name, state_code, jc_countries (id, country_name))
      `, { count: 'exact' })
      .eq('is_active', true)
      .order('city_name', { ascending: true })

    if (search) {
      query = query.or(`city_name.ilike.%${search}%,city_code.ilike.%${search}%`)
    }

    if (stateId) {
      query = query.eq('state_id', stateId)
    }

    const offset = (page - 1) * limit
    query = query.range(offset, offset + limit - 1)

    const { data, error, count } = await query

    if (error) throw error

    return NextResponse.json({
      cities: data || [],
      total: count || 0,
      pagination: {
        page,
        limit,
        total: count || 0,
        totalPages: Math.ceil((count || 0) / limit),
      }
    })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

export async function POST(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const body = await request.json()

    if (!body.city_name || !body.state_id) {
      return NextResponse.json({ error: 'City name and state are required' }, { status: 400 })
    }

    const { data, error } = await supabase
      .from('jc_cities')
      .insert({
        state_id: body.state_id,
        city_code: body.city_code || null,
        city_name: body.city_name,
        is_metro: body.is_metro || false,
        latitude: body.latitude || null,
        longitude: body.longitude || null,
      })
      .select()
      .single()

    if (error) throw error

    return NextResponse.json({ city: data, message: 'City created successfully' })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
