import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * Notifications API - List and manage notifications
 */
export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)

    const page = parseInt(searchParams.get('page') || '1')
    const limit = parseInt(searchParams.get('limit') || '20')
    const status = searchParams.get('status') || ''
    const type = searchParams.get('type') || ''
    const channel = searchParams.get('channel') || ''

    const offset = (page - 1) * limit

    let query = supabase
      .from('jc_notifications')
      .select(`
        *,
        jc_passengers!jc_notifications_passenger_id_fkey (
          full_name,
          mobile
        ),
        jc_bookings!jc_notifications_booking_id_fkey (
          travel_operator_pnr,
          origin,
          destination,
          travel_date
        )
      `, { count: 'exact' })

    if (status && status !== 'all') {
      query = query.eq('status', status)
    }

    if (type && type !== 'all') {
      query = query.eq('notification_type', type)
    }

    if (channel && channel !== 'all') {
      query = query.eq('channel', channel)
    }

    query = query
      .order('created_at', { ascending: false })
      .range(offset, offset + limit - 1)

    const { data: notifications, count, error } = await query

    if (error) {
      throw error
    }

    return NextResponse.json({
      notifications,
      pagination: {
        page,
        limit,
        total: count || 0,
        totalPages: Math.ceil((count || 0) / limit),
      },
    })
  } catch (error: any) {
    console.error('Notifications API error:', error)
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    )
  }
}

/**
 * Resend a notification
 */
export async function POST(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const body = await request.json()
    const { notification_id, action } = body

    if (action === 'resend') {
      // Reset notification for resending
      const { data, error } = await (supabase
        .from('jc_notifications') as any)
        .update({
          status: 'pending',
          scheduled_at: new Date().toISOString(),
          retry_count: 0,
          error_message: null,
          error_code: null,
        })
        .eq('id', notification_id)
        .select()
        .single()

      if (error) throw error

      return NextResponse.json({
        status: 'success',
        message: 'Notification queued for resend',
        notification: data,
      })
    }

    return NextResponse.json(
      { error: 'Invalid action' },
      { status: 400 }
    )
  } catch (error: any) {
    console.error('Notification action error:', error)
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    )
  }
}
