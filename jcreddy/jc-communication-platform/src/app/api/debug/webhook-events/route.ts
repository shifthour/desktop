import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * Debug endpoint to check recent webhook events
 * GET /api/debug/webhook-events?type=release&limit=20&full=true
 */
export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)

    const eventType = searchParams.get('type') || ''
    const limit = parseInt(searchParams.get('limit') || '20')
    const showFull = searchParams.get('full') === 'true'

    // Get recent webhook events
    let query = supabase
      .from('jc_webhook_events')
      .select('id, event_type, payload, processed, processing_error, created_at')
      .order('created_at', { ascending: false })
      .limit(limit)

    if (eventType) {
      query = query.ilike('event_type', `%${eventType}%`)
    }

    const { data: events, error } = await query

    if (error) {
      return NextResponse.json({ error: error.message }, { status: 500 })
    }

    // Also get recent released bookings with passengers
    const { data: releasedBookings } = await supabase
      .from('jc_bookings')
      .select(`
        id, travel_operator_pnr, booking_status, booking_source, created_at, updated_at,
        jc_passengers (id, full_name, mobile, seat_number)
      `)
      .eq('booking_status', 'released')
      .order('updated_at', { ascending: false })
      .limit(10)

    // Check what requestTypes are coming in
    const requestTypes = events?.map(e => {
      const baseInfo = {
        id: e.id,
        event_type: e.event_type,
        requestType: e.payload?.requestType || e.payload?.request_type || 'N/A',
        pnr: e.payload?.travel_operator_pnr || e.payload?.pnr_number || e.payload?.ticket_number || 'N/A',
        processed: e.processed,
        error: e.processing_error,
        created_at: e.created_at,
      }

      // If full=true, include full payload
      if (showFull) {
        return {
          ...baseInfo,
          payload: e.payload,
        }
      }
      return baseInfo
    })

    return NextResponse.json({
      webhook_events: requestTypes,
      released_bookings: releasedBookings,
      summary: {
        total_events: events?.length || 0,
        release_events: events?.filter(e =>
          e.event_type?.includes('release') ||
          e.payload?.requestType?.toUpperCase()?.includes('RELEASE')
        ).length || 0,
        released_bookings_count: releasedBookings?.length || 0,
      }
    })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
