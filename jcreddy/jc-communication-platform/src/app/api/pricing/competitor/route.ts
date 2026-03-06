import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * GET /api/pricing/competitor
 * Fetch competitor fare comparisons with filters.
 *
 * Query params:
 *   ?status=pending          Filter by status (default: pending)
 *   ?date=2026-02-13         Filter by travel date
 *   ?route=Hyderabad|Bangalore  Filter by route (from|to)
 *   ?limit=50                Max results (default: 50)
 */
export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)

    const status = searchParams.get('status') || 'pending'
    const date = searchParams.get('date')
    const route = searchParams.get('route')
    const limit = parseInt(searchParams.get('limit') || '50', 10)

    // Build query
    let query = supabase
      .from('jc_fare_comparisons')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(limit)

    if (status && status !== 'all') {
      query = query.eq('status', status)
    }

    if (date) {
      query = query.eq('travel_date', date)
    }

    if (route) {
      const [from, to] = route.split('|')
      if (from) query = query.eq('route_from', from)
      if (to) query = query.eq('route_to', to)
    }

    const { data: comparisons, error } = await query

    if (error) {
      throw new Error(`Failed to fetch comparisons: ${error.message}`)
    }

    // Compute stats
    const pending = comparisons?.filter((c) => c.status === 'pending') || []
    const costlier = pending.filter((c) => (c.fare_difference || 0) > 0)
    const cheaper = pending.filter((c) => (c.fare_difference || 0) < 0)
    const avgDifference =
      costlier.length > 0
        ? Math.round((costlier.reduce((sum, c) => sum + (c.fare_difference || 0), 0) / costlier.length) * 100) / 100
        : 0

    const totalSavings = pending.reduce((sum, c) => {
      const diff = (c.mythri_min_fare || 0) - (c.recommended_mythri_fare || 0)
      return sum + Math.max(0, diff)
    }, 0)

    // Group comparisons by Mythri trip for card layout
    const grouped: Record<string, any> = {}
    for (const c of (comparisons || [])) {
      const key = `${c.mythri_service}|${c.travel_date}`
      if (!grouped[key]) {
        grouped[key] = {
          mythri_service: c.mythri_service,
          mythri_departure: c.mythri_departure,
          mythri_bus_type: c.mythri_bus_type,
          mythri_min_fare: c.mythri_min_fare,
          mythri_max_fare: c.mythri_max_fare,
          mythri_avg_fare: c.mythri_avg_fare,
          mythri_available_seats: c.mythri_available_seats,
          mythri_seat_type_fares: c.mythri_seat_type_fares || [],
          mythri_trip_id: c.mythri_trip_id || '',
          route_from: c.route_from,
          route_to: c.route_to,
          travel_date: c.travel_date,
          competitors: [],
        }
      }
      grouped[key].competitors.push({
        operator: c.competitor_operator || 'vikram',
        service: c.vikram_service,
        departure: c.vikram_departure,
        bus_type: c.vikram_bus_type,
        min_fare: c.vikram_min_fare,
        max_fare: c.vikram_max_fare,
        avg_fare: c.vikram_avg_fare,
        available_seats: c.vikram_available_seats,
        seat_type_fares: c.competitor_seat_type_fares || [],
        fare_difference: c.fare_difference,
        recommended_mythri_fare: c.recommended_mythri_fare,
        undercut_amount: c.undercut_amount,
        savings_percent: c.savings_percent,
        seat_type_comparison: c.seat_type_comparison,
        trip_id: c.competitor_trip_id || '',
        id: c.id,
        status: c.status,
      })
    }
    // Get routes list for filter dropdown (needed before unmatched trips check)
    const { data: routes } = await supabase
      .from('jc_competitor_route_config')
      .select('route_name, route_from, route_to')
      .eq('is_active', true)
      .order('route_name')

    // Also include Mythri trips that have no competitor match
    // so ALL Mythri services appear even without nearby competitors
    // Only include trips for active routes
    const activeRouteKeys = new Set(
      (routes || []).map((r: any) => `${r.route_from}|${r.route_to}`)
    )

    const datesToFetch = date
      ? [date]
      : Array.from(new Set((comparisons || []).map((c: any) => c.travel_date)))

    if (datesToFetch.length > 0) {
      let mythriQuery = supabase
        .from('jc_competitor_fares')
        .select('*')
        .eq('operator', 'mythri')
        .in('travel_date', datesToFetch)
        .order('scraped_at', { ascending: false })
        .limit(100)

      if (route) {
        const [from, to] = route.split('|')
        if (from) mythriQuery = mythriQuery.eq('route_from', from)
        if (to) mythriQuery = mythriQuery.eq('route_to', to)
      }

      const { data: allMythriTrips } = await mythriQuery
      const existingKeys = new Set(Object.keys(grouped))
      const seenMythri = new Set<string>()

      for (const trip of (allMythriTrips || [])) {
        const key = `${trip.service_number}|${trip.travel_date}`
        if (seenMythri.has(key) || existingKeys.has(key)) continue
        // Skip trips for inactive routes
        if (!activeRouteKeys.has(`${trip.route_from}|${trip.route_to}`)) continue
        seenMythri.add(key)

        grouped[key] = {
          mythri_service: trip.service_number,
          mythri_departure: trip.departure_time,
          mythri_bus_type: trip.bus_type,
          mythri_min_fare: trip.min_fare,
          mythri_max_fare: trip.max_fare,
          mythri_avg_fare: trip.avg_fare,
          mythri_available_seats: trip.available_seats,
          mythri_seat_type_fares: trip.fare_by_seat_type || [],
          mythri_trip_id: trip.trip_identifier || '',
          route_from: trip.route_from,
          route_to: trip.route_to,
          travel_date: trip.travel_date,
          competitors: [],
        }
      }
    }

    const groupedComparisons = Object.values(grouped)

    // Get last scrape time
    const { data: lastSync } = await supabase
      .from('jc_sync_jobs')
      .select('completed_at, details')
      .eq('sync_type', 'competitor_scrape')
      .order('completed_at', { ascending: false })
      .limit(1)
      .single()

    return NextResponse.json({
      success: true,
      comparisons: comparisons || [],
      grouped_comparisons: groupedComparisons,
      stats: {
        total_pending: pending.length,
        costlier_count: costlier.length,
        cheaper_count: cheaper.length,
        avg_fare_difference: avgDifference,
        total_potential_savings: Math.round(totalSavings),
        routes_monitored: routes?.length || 0,
      },
      routes: routes || [],
      last_scrape: lastSync?.completed_at || null,
      last_scrape_details: lastSync?.details || null,
    })
  } catch (error: any) {
    console.error('[Competitor API] Error:', error.message)
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    )
  }
}

/**
 * PATCH /api/pricing/competitor
 * Update comparison status (apply/dismiss)
 *
 * Body: { id: string, action: 'apply' | 'dismiss', notes?: string }
 */
export async function PATCH(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const body = await request.json()
    const { id, action, notes } = body

    if (!id || !action) {
      return NextResponse.json({ error: 'id and action are required' }, { status: 400 })
    }

    const statusMap: Record<string, string> = {
      apply: 'applied',
      dismiss: 'dismissed',
    }

    const newStatus = statusMap[action]
    if (!newStatus) {
      return NextResponse.json({ error: 'Invalid action. Use "apply" or "dismiss".' }, { status: 400 })
    }

    const { error } = await supabase
      .from('jc_fare_comparisons')
      .update({
        status: newStatus,
        reviewed_at: new Date().toISOString(),
        review_notes: notes || null,
      })
      .eq('id', id)

    if (error) {
      throw new Error(`Failed to update: ${error.message}`)
    }

    return NextResponse.json({ success: true, status: newStatus })
  } catch (error: any) {
    console.error('[Competitor API] PATCH error:', error.message)
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    )
  }
}
