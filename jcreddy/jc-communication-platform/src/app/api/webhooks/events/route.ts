import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * Webhook Events API - List and monitor webhook events
 */
export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)

    const page = parseInt(searchParams.get('page') || '1')
    const limit = parseInt(searchParams.get('limit') || '50')
    const eventType = searchParams.get('type') || ''
    const processed = searchParams.get('processed') || ''
    const pnrSearch = searchParams.get('pnr') || ''

    const offset = (page - 1) * limit

    let query = supabase
      .from('jc_webhook_events')
      .select('*', { count: 'exact' })
      .eq('endpoint', '/api/webhooks/bitla/events') // Only show events from the main endpoint

    if (eventType && eventType !== 'all') {
      // Support multiple event types (comma-separated)
      const types = eventType.split(',').map(t => t.trim()).filter(Boolean)
      if (types.length === 1) {
        query = query.eq('event_type', types[0])
      } else if (types.length > 1) {
        query = query.in('event_type', types)
      }
    }

    if (processed === 'true') {
      query = query.eq('processed', true)
    } else if (processed === 'false') {
      query = query.eq('processed', false)
    }

    // Search by PNR in payload JSON
    // This searches across multiple possible PNR field names
    if (pnrSearch) {
      query = query.or(
        `payload->>travel_operator_pnr.ilike.%${pnrSearch}%,` +
        `payload->>pnr_number.ilike.%${pnrSearch}%,` +
        `payload->>ticket_number.ilike.%${pnrSearch}%,` +
        `payload->>pnr.ilike.%${pnrSearch}%,` +
        `payload->>ticketNumber.ilike.%${pnrSearch}%,` +
        `payload->>PNR.ilike.%${pnrSearch}%,` +
        `payload->>TicketNumber.ilike.%${pnrSearch}%`
      )
    }

    query = query
      .order('created_at', { ascending: false })
      .range(offset, offset + limit - 1)

    const { data: events, count, error } = await query

    if (error) {
      throw error
    }

    // Get event type counts (only from main endpoint)
    const { data: typeCounts } = await supabase
      .from('jc_webhook_events')
      .select('event_type')
      .eq('endpoint', '/api/webhooks/bitla/events')

    const eventTypeCounts: Record<string, number> = {}
    typeCounts?.forEach(e => {
      eventTypeCounts[e.event_type] = (eventTypeCounts[e.event_type] || 0) + 1
    })

    return NextResponse.json({
      events,
      eventTypeCounts,
      pagination: {
        page,
        limit,
        total: count || 0,
        totalPages: Math.ceil((count || 0) / limit),
      },
    })
  } catch (error: any) {
    console.error('Webhook events API error:', error)
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    )
  }
}

/**
 * Retry a failed webhook event
 */
export async function POST(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const body = await request.json()
    const { event_id } = body

    // Get the event
    const { data: event, error: fetchError } = await supabase
      .from('jc_webhook_events')
      .select('*')
      .eq('id', event_id)
      .single()

    if (fetchError || !event) {
      return NextResponse.json(
        { error: 'Event not found' },
        { status: 404 }
      )
    }

    // Mark as unprocessed to retry
    const { error: updateError } = await supabase
      .from('jc_webhook_events')
      .update({
        processed: false,
        processing_error: null,
        retry_count: event.retry_count + 1,
      })
      .eq('id', event_id)

    if (updateError) throw updateError

    return NextResponse.json({
      status: 'success',
      message: 'Event queued for retry',
    })
  } catch (error: any) {
    console.error('Webhook retry error:', error)
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    )
  }
}
