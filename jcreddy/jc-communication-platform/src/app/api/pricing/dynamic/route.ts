import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'
import {
  generateDynamicPrice,
  detectDirection,
  getEventForDate,
  isInLongWeekend,
  isVolvoOrPremium,
  FLOOR_FARE,
  DAILY_MINIMUM,
  SERVICES_PER_DAY,
  SEATS_PER_SERVICE,
  TARGET_FILL_RATE,
  type DynamicPricingInput,
  type CompetitorFareInput,
} from '@/lib/dynamic-pricing-v2'

/**
 * GET /api/pricing/dynamic
 * Generates AI pricing recommendations using SCRAPED competitor fare data.
 * Uses jc_competitor_fares as the primary source (all 3 operators: mythri, vikram, vkaveri).
 *
 * Query params:
 *   ?route=From|To     Filter by route (optional, default: all)
 *   ?days=30           Number of days to look ahead (default: 30)
 */
export async function GET(request: NextRequest) {
  const startTime = Date.now()

  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)

    const route = searchParams.get('route')
    const days = Math.min(parseInt(searchParams.get('days') || '30', 10), 60)

    // Parse route filter
    let routeFrom: string | null = null
    let routeTo: string | null = null
    if (route) {
      const parts = route.split('|')
      routeFrom = parts[0] || null
      routeTo = parts[1] || null
    }

    // Date range: today → today+days (IST)
    const now = new Date()
    const istOffset = 5.5 * 60 * 60 * 1000
    const istNow = new Date(now.getTime() + istOffset)
    const today = istNow.toISOString().split('T')[0]
    const endDate = new Date(istNow.getTime() + days * 24 * 60 * 60 * 1000)
    const end = endDate.toISOString().split('T')[0]

    console.log(`[Dynamic Pricing] Generating from scraped data for ${today} → ${end}`)

    // ── 1. Fetch ALL scraped fares (mythri + competitors) ────────────────
    let fareQuery = supabase
      .from('jc_competitor_fares')
      .select('id, operator, route_from, route_to, travel_date, service_number, bus_type, departure_time, arrival_time, total_seats, available_seats, min_fare, max_fare, avg_fare, fare_by_seat_type, scraped_at, trip_identifier')
      .gte('travel_date', today)
      .lte('travel_date', end)
      .order('travel_date', { ascending: true })
      .order('operator', { ascending: true })

    if (routeFrom) fareQuery = fareQuery.ilike('route_from', `%${routeFrom}%`)
    if (routeTo) fareQuery = fareQuery.ilike('route_to', `%${routeTo}%`)

    const { data: allFares, error: fareError } = await fareQuery

    if (fareError) throw new Error(`Failed to fetch fares: ${fareError.message}`)

    const fares = allFares || []
    console.log(`[Dynamic Pricing] Fetched ${fares.length} scraped fares`)

    if (fares.length === 0) {
      return NextResponse.json({
        success: true,
        summary: {
          total_trips: 0,
          avg_recommended_fare: 0,
          total_revenue_current: 0,
          total_revenue_recommended: 0,
          revenue_delta: 0,
          trips_needing_increase: 0,
          trips_needing_decrease: 0,
          trips_no_change: 0,
          floor_fare: FLOOR_FARE,
          daily_minimum: DAILY_MINIMUM,
          services_per_day: SERVICES_PER_DAY,
          seats_per_service: SEATS_PER_SERVICE,
          target_fill_rate: TARGET_FILL_RATE,
        },
        recommendations: [],
        daily_totals: [],
        message: 'No scraped fare data found. Run the competitor scraper first.',
        latency_ms: Date.now() - startTime,
        timestamp: new Date().toISOString(),
      })
    }

    // ── 2. Separate Mythri fares from competitor fares ────────────────────
    const mythriFares = fares.filter((f: any) => f.operator === 'mythri')
    const competitorFares = fares.filter((f: any) => f.operator !== 'mythri')

    // Group competitor fares by date+route for easy lookup
    // Include bus_type for bus-class-aware pricing (Volvo vs regular)
    const competitorByKey = new Map<string, CompetitorFareInput[]>()
    for (const c of competitorFares) {
      const key = `${c.travel_date}|${c.route_from}|${c.route_to}`
      if (!competitorByKey.has(key)) competitorByKey.set(key, [])
      // Aggregate per operator (keep best fare per operator per date)
      const existing = competitorByKey.get(key)!
      const existingOp = existing.find(e => e.operator === c.operator)
      if (!existingOp) {
        existing.push({
          operator: c.operator,
          min_fare: c.min_fare || 0,
          avg_fare: c.avg_fare || 0,
          available_seats: c.available_seats || 0,
          bus_type: c.bus_type || '',
          is_volvo: isVolvoOrPremium(c.bus_type || ''),
        })
      } else {
        // Keep the lower min_fare
        if (c.min_fare && c.min_fare < existingOp.min_fare) {
          existingOp.min_fare = c.min_fare
        }
      }
    }

    // ── 3. Deduplicate Mythri fares (keep latest scrape per service per date+route) ──
    const mythriDedup = new Map<string, any>()
    for (const f of mythriFares) {
      const key = `${f.travel_date}|${f.route_from}|${f.route_to}|${f.service_number || 'default'}`
      const scraped = f.scraped_at ? new Date(f.scraped_at).getTime() : 0
      const existing = mythriDedup.get(key)
      if (!existing || scraped > (existing.scraped_at ? new Date(existing.scraped_at).getTime() : 0)) {
        mythriDedup.set(key, f)
      }
    }
    const dedupedMythriFares = Array.from(mythriDedup.values())
    console.log(`[Dynamic Pricing] Deduped Mythri: ${mythriFares.length} → ${dedupedMythriFares.length} unique services`)

    // ── 4. Generate recommendations for each Mythri trip ─────────────────
    const recommendations: any[] = []

    for (const fare of dedupedMythriFares) {
      const travelDate = fare.travel_date
      // Use noon IST so UTC conversion stays on same calendar day (midnight IST = prev day in UTC)
      const dateObj = new Date(travelDate + 'T12:00:00+05:30')
      const dayOfWeek = dateObj.getUTCDay()
      const direction = detectDirection(fare.route_from, fare.route_to)

      // Get Mythri's current fare
      const baseFare = fare.min_fare || fare.avg_fare || FLOOR_FARE
      const totalSeats = fare.total_seats || SEATS_PER_SERVICE
      const availableSeats = fare.available_seats || totalSeats
      const bookedSeats = Math.max(0, totalSeats - availableSeats)
      const occupancy = totalSeats > 0 ? (bookedSeats / totalSeats) * 100 : 0

      // Calculate hours to departure
      let hoursToDepart = 168 // default to 7 days
      if (fare.departure_time) {
        const depTime = new Date(fare.departure_time).getTime()
        hoursToDepart = Math.max(0, (depTime - Date.now()) / (1000 * 60 * 60))
      } else {
        // Estimate from date
        const depEstimate = new Date(travelDate + 'T20:00:00+05:30').getTime() // assume 8 PM departure
        hoursToDepart = Math.max(0, (depEstimate - Date.now()) / (1000 * 60 * 60))
      }

      // Get competitor fares for this date + route
      const key = `${travelDate}|${fare.route_from}|${fare.route_to}`
      const compFares = competitorByKey.get(key) || []

      // Event data
      const event = getEventForDate(travelDate)
      const longWeekend = isInLongWeekend(travelDate)

      // Build pricing input (bus_type for class-aware competitor comparison)
      const input: DynamicPricingInput = {
        travel_date: travelDate,
        direction,
        service_number: fare.service_number || '',
        departure_time: fare.departure_time ? new Date(fare.departure_time).toTimeString().substring(0, 5) : '',
        bus_type: fare.bus_type || '',
        is_volvo: isVolvoOrPremium(fare.bus_type || ''),
        base_fare: baseFare,
        current_occupancy: occupancy,
        booked_seats: bookedSeats,
        total_seats: totalSeats,
        available_seats: availableSeats,
        bookings_last_1h: 0, // not available from scrape
        bookings_last_6h: 0,
        bookings_last_24h: 0,
        cancellations_last_24h: 0,
        hours_to_departure: hoursToDepart,
        competitor_fares: compFares,
        day_of_week: dayOfWeek,
        is_event: !!event,
        event_name: event?.name,
        event_impact: event?.impact,
        is_long_weekend: !!longWeekend,
      }

      // Generate recommendation
      const result = generateDynamicPrice(input)

      const changePct = baseFare > 0
        ? ((result.recommended_fare - baseFare) / baseFare) * 100
        : 0

      const cheapestComp = compFares.length > 0
        ? Math.min(...compFares.map(c => c.min_fare).filter(f => f > 0))
        : null
      const avgComp = compFares.length > 0
        ? Math.round(compFares.reduce((s, c) => s + c.avg_fare, 0) / compFares.length)
        : null

      const dirLabel = direction === 'BLR_WGL' ? 'BLR → WGL' : 'WGL → BLR'
      const depTimeStr = fare.departure_time ? new Date(fare.departure_time).toTimeString().substring(0, 5) : '--'

      // Build a clear recommendation reason
      const activeFactors = result.factors.filter(f => f.adjustment !== 0)
      const topFactor = activeFactors.sort((a, b) => Math.abs(b.adjustment * b.weight) - Math.abs(a.adjustment * a.weight))[0]
      let action: 'RAISE' | 'LOWER' | 'HOLD' = 'HOLD'
      if (changePct > 1) action = 'RAISE'
      else if (changePct < -1) action = 'LOWER'

      let reason = ''
      if (action === 'RAISE' && topFactor) {
        reason = topFactor.explanation
      } else if (action === 'LOWER' && topFactor) {
        reason = topFactor.explanation
      } else {
        reason = 'Current fare is optimal for this date.'
      }

      recommendations.push({
        trip_id: fare.id,
        travel_date: travelDate,
        route_from: fare.route_from,
        route_to: fare.route_to,
        route_label: `${fare.route_from} → ${fare.route_to}`,
        service_number: fare.service_number || '',
        bus_type: fare.bus_type || '',
        departure_time: depTimeStr,
        current_fare: baseFare,
        mythri_min: fare.min_fare,
        mythri_max: fare.max_fare,
        mythri_avg: fare.avg_fare,
        mythri_seats: totalSeats,
        mythri_available: availableSeats,
        recommended_fare: result.recommended_fare,
        change_percent: Math.round(changePct * 100) / 100,
        action,
        reason,
        demand_score: result.demand_score,
        urgency: result.urgency,
        factors: result.factors,
        competitor_details: compFares,
        occupancy_percent: Math.round(occupancy * 100) / 100,
        available_seats: availableSeats,
        booked_seats: bookedSeats,
        total_seats: totalSeats,
        revenue_current: result.revenue_at_current,
        revenue_recommended: result.revenue_at_recommended,
        revenue_delta: result.revenue_delta,
        confidence: result.confidence,
        event_name: event?.name || null,
        event_emoji: event?.emoji || null,
        event_impact: event?.impact || null,
        is_long_weekend: !!longWeekend,
        long_weekend_name: longWeekend?.name || null,
        hours_to_departure: Math.round(hoursToDepart * 10) / 10,
      })
    }

    // ── 5. Build summary ─────────────────────────────────────────────────
    const totalTrips = recommendations.length
    const avgRecFare = totalTrips > 0
      ? Math.round(recommendations.reduce((s: number, r: any) => s + r.recommended_fare, 0) / totalTrips)
      : 0
    const totalRevCurrent = recommendations.reduce((s: number, r: any) => s + r.revenue_current, 0)
    const totalRevRecommended = recommendations.reduce((s: number, r: any) => s + r.revenue_recommended, 0)
    const tripsUp = recommendations.filter((r: any) => r.change_percent > 0).length
    const tripsDown = recommendations.filter((r: any) => r.change_percent < 0).length
    const tripsNoChange = recommendations.filter((r: any) => r.change_percent === 0).length

    // ── 6. Build daily totals ────────────────────────────────────────────
    const dailyMap = new Map<string, {
      date: string
      trips: number
      total_current: number
      total_recommended: number
      delta: number
      event_name: string | null
      event_emoji: string | null
      is_long_weekend: boolean
    }>()

    for (const rec of recommendations) {
      const key = rec.travel_date
      if (!dailyMap.has(key)) {
        dailyMap.set(key, {
          date: rec.travel_date,
          trips: 0,
          total_current: 0,
          total_recommended: 0,
          delta: 0,
          event_name: rec.event_name,
          event_emoji: rec.event_emoji,
          is_long_weekend: rec.is_long_weekend,
        })
      }
      const d = dailyMap.get(key)!
      d.trips++
      d.total_current += rec.revenue_current
      d.total_recommended += rec.revenue_recommended
      d.delta += rec.revenue_delta
    }

    const dailyTotals = Array.from(dailyMap.values()).sort(
      (a, b) => a.date.localeCompare(b.date)
    )

    // ── 7. Build grouped fares by date (all 3 operators) ───────────────
    // Group ALL fares (mythri + competitors) by date+route for card display
    // Deduplicate by service_number within each operator (keep latest scrape)
    const faresByDate = new Map<string, any>()
    for (const f of fares) {
      const key = `${f.travel_date}|${f.route_from}|${f.route_to}`
      if (!faresByDate.has(key)) {
        const evt = getEventForDate(f.travel_date)
        const lw = isInLongWeekend(f.travel_date)
        faresByDate.set(key, {
          date: f.travel_date,
          route_from: f.route_from,
          route_to: f.route_to,
          route_label: `${f.route_from} → ${f.route_to}`,
          event_name: evt?.name || null,
          event_emoji: evt?.emoji || null,
          event_impact: evt?.impact || null,
          is_long_weekend: !!lw,
          long_weekend_name: lw?.name || null,
          operators: {} as Record<string, any>,
        })
      }
      const group = faresByDate.get(key)!
      if (!group.operators[f.operator]) {
        group.operators[f.operator] = {
          operator: f.operator,
          _serviceMap: new Map<string, any>(), // temp: dedup by service_number
          services: [],
        }
      }
      const opGroup = group.operators[f.operator]
      const svcKey = f.service_number || `unknown-${opGroup._serviceMap.size}`
      // Keep only latest scrape per service (fares are ordered by travel_date, operator already)
      const existing = opGroup._serviceMap.get(svcKey)
      const scraped = f.scraped_at ? new Date(f.scraped_at).getTime() : 0
      if (!existing || scraped > (existing._scraped || 0)) {
        opGroup._serviceMap.set(svcKey, {
          _scraped: scraped,
          service_number: f.service_number,
          bus_type: f.bus_type,
          departure_time: f.departure_time ? new Date(f.departure_time).toTimeString().substring(0, 5) : null,
          min_fare: f.min_fare,
          max_fare: f.max_fare,
          avg_fare: f.avg_fare,
          total_seats: f.total_seats,
          available_seats: f.available_seats,
          trip_identifier: f.trip_identifier || '',
          fare_by_seat_type: f.fare_by_seat_type || [],
        })
      }
    }

    // Convert _serviceMap to services array and clean up temp fields
    for (const group of Array.from(faresByDate.values())) {
      for (const opKey of Object.keys(group.operators)) {
        const op = group.operators[opKey]
        op.services = Array.from(op._serviceMap.values()).map((s: any) => {
          const { _scraped, ...rest } = s
          return rest
        })
        delete op._serviceMap
      }
    }

    const dateGroups = Array.from(faresByDate.values()).sort(
      (a, b) => a.date.localeCompare(b.date) || a.route_label.localeCompare(b.route_label)
    )

    const latency = Date.now() - startTime
    console.log(`[Dynamic Pricing] Generated ${totalTrips} recommendations from ${dedupedMythriFares.length} Mythri services (deduped from ${mythriFares.length}) + ${competitorFares.length} competitor fares in ${latency}ms`)

    return NextResponse.json({
      success: true,
      summary: {
        total_trips: totalTrips,
        avg_recommended_fare: avgRecFare,
        total_revenue_current: Math.round(totalRevCurrent),
        total_revenue_recommended: Math.round(totalRevRecommended),
        revenue_delta: Math.round(totalRevRecommended - totalRevCurrent),
        trips_needing_increase: tripsUp,
        trips_needing_decrease: tripsDown,
        trips_no_change: tripsNoChange,
        floor_fare: FLOOR_FARE,
        daily_minimum: DAILY_MINIMUM,
        services_per_day: SERVICES_PER_DAY,
        seats_per_service: SEATS_PER_SERVICE,
        target_fill_rate: TARGET_FILL_RATE,
        mythri_fares_count: mythriFares.length,
        competitor_fares_count: competitorFares.length,
      },
      recommendations,
      daily_totals: dailyTotals,
      date_groups: dateGroups,
      latency_ms: latency,
      timestamp: new Date().toISOString(),
    })
  } catch (error: any) {
    console.error('[Dynamic Pricing] Error:', error.message)
    return NextResponse.json(
      { success: false, error: error.message, timestamp: new Date().toISOString() },
      { status: 500 }
    )
  }
}
