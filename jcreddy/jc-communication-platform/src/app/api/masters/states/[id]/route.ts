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
      .from('jc_states')
      .update({
        country_id: body.country_id,
        state_code: body.state_code?.toUpperCase(),
        state_name: body.state_name,
        updated_at: new Date().toISOString(),
      })
      .eq('id', id)
      .select()
      .single()

    if (error) throw error

    return NextResponse.json({ state: data, message: 'State updated successfully' })
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
      .from('jc_states')
      .update({ is_active: false })
      .eq('id', id)

    if (error) throw error

    return NextResponse.json({ message: 'State deleted successfully' })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
