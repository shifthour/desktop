import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

export async function PUT(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params
    const supabase = createServerClient()
    const body = await request.json()

    const { data, error } = await supabase
      .from('jc_cities')
      .update({
        state_id: body.state_id,
        city_code: body.city_code,
        city_name: body.city_name,
        is_metro: body.is_metro,
        latitude: body.latitude,
        longitude: body.longitude,
        updated_at: new Date().toISOString(),
      })
      .eq('id', id)
      .select()
      .single()

    if (error) throw error

    return NextResponse.json({ city: data, message: 'City updated successfully' })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

export async function DELETE(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params
    const supabase = createServerClient()

    const { error } = await supabase
      .from('jc_cities')
      .update({ is_active: false })
      .eq('id', id)

    if (error) throw error

    return NextResponse.json({ message: 'City deleted successfully' })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
