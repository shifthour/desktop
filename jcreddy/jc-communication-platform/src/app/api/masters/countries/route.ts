import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)
    const search = searchParams.get('search') || ''

    let query = supabase
      .from('jc_countries')
      .select('*', { count: 'exact' })
      .eq('is_active', true)
      .order('country_name', { ascending: true })

    if (search) {
      query = query.or(`country_name.ilike.%${search}%,country_code.ilike.%${search}%`)
    }

    const { data, error, count } = await query

    if (error) throw error

    return NextResponse.json({ countries: data || [], total: count || 0 })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

export async function POST(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const body = await request.json()

    if (!body.country_code || !body.country_name) {
      return NextResponse.json({ error: 'Country code and name are required' }, { status: 400 })
    }

    const { data, error } = await supabase
      .from('jc_countries')
      .insert({
        country_code: body.country_code.toUpperCase(),
        country_name: body.country_name,
        phone_code: body.phone_code || null,
        currency_code: body.currency_code || null,
      })
      .select()
      .single()

    if (error) throw error

    return NextResponse.json({ country: data, message: 'Country created successfully' })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
