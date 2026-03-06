import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

// Allow 60 second timeout for cron
export const maxDuration = 60

const CRON_SECRET = process.env.CRON_SECRET || 'mythri-cron-secret-2024'

/**
 * GET /api/cron/demand-snapshot
 * Captures demand snapshots for all upcoming trips (next 3 days).
 * Designed to run every 30 minutes via cron-job.org.
 *
 * Required Header: x-cron-secret: <your-secret>
 */
export async function GET(request: NextRequest) {
  const startTime = Date.now()

  try {
    // Validate cron secret
    const cronSecret = request.headers.get('x-cron-secret')
    if (cronSecret !== CRON_SECRET) {
      console.log('[Demand Snapshot] Unauthorized request - invalid secret')
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const supabase = createServerClient()

    // Get IST date
    const now = new Date()
    const istDate = now.toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' })
    const istNow = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }))

    console.log(`[Demand Snapshot] Starting snapshot for IST date: ${istDate}`)

    // Create sync job record
    const { data: syncJob } = await supabase
      .from('jc_sync_jobs')
      .insert({
        sync_type: 'demand_snapshot',
        target_date: istDate,
        status: 'running',
        started_at: new Date().toISOString(),
        metadata: { trigger: 'cron', mode: 'demand_snapshot' },
      })
      .select('id')
      .single()

    // Calculate date range: today + next 3 days
    const endDate = new Date(istNow)
    endDate.setDate(endDate.getDate() + 3)
    const endDateStr = endDate.toISOString().split('T')[0]

    // Fetch all upcoming trips in the date range
    const { data: trips, error: tripsError } = await supabase
      .from('jc_trips')
      .select(`
        id,
        route_id,
        service_number,
        travel_date,
        departure_time,
        departure_datetime,
        total_seats,
        available_seats,
        booked_seats,
        blocked_seats,
        trip_status,
        jc_routes (
          id,
          base_fare,
          service_number
        )
      `)
      .gte('travel_date', istDate)
      .lte('travel_date', endDateStr)
      .in('trip_status', ['scheduled', 'departed', 'in_transit'])
      .range(0, 999)

    if (tripsError) {
      throw new Error(`Failed to fetch trips: ${tripsError.message}`)
    }

    if (!trips || trips.length === 0) {
      console.log('[Demand Snapshot] No upcoming trips found')
      if (syncJob?.id) {
        await supabase.from('jc_sync_jobs').update({
          status: 'completed',
          completed_at: new Date().toISOString(),
          records_fetched: 0,
          records_created: 0,
          details: { message: 'No upcoming trips found' },
        }).eq('id', syncJob.id)
      }
      return NextResponse.json({
        success: true,
        message: 'No upcoming trips found',
        snapshots_created: 0,
      })
    }

    console.log(`[Demand Snapshot] Found ${trips.length} upcoming trips`)

    // Fetch all trip IDs for batch queries
    const tripIds = trips.map((t: any) => t.id)

    // Batch query: booking velocity for all trips at once
    const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000).toISOString()
    const sixHoursAgo = new Date(now.getTime() - 6 * 60 * 60 * 1000).toISOString()
    const twentyFourHoursAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000).toISOString()

    // Fetch recent bookings for velocity calculation
    const { data: recentBookings } = await supabase
      .from('jc_bookings')
      .select('trip_id, created_at')
      .in('trip_id', tripIds)
      .eq('booking_status', 'confirmed')
      .gte('created_at', twentyFourHoursAgo)
      .range(0, 9999)

    // Fetch recent cancellations
    const { data: recentCancellations } = await supabase
      .from('jc_cancellations')
      .select('booking_id')
      .gte('cancelled_at', twentyFourHoursAgo)
      .range(0, 9999)

    // Fetch cancellation booking IDs to trip mapping
    const cancellationBookingIds = recentCancellations?.map((c: any) => c.booking_id).filter(Boolean) || []
    let cancellationTripMap: Record<string, number> = {}
    if (cancellationBookingIds.length > 0) {
      const { data: cancelBookings } = await supabase
        .from('jc_bookings')
        .select('trip_id')
        .in('id', cancellationBookingIds)
        .in('trip_id', tripIds)
        .range(0, 9999)

      if (cancelBookings) {
        for (const b of cancelBookings) {
          cancellationTripMap[b.trip_id] = (cancellationTripMap[b.trip_id] || 0) + 1
        }
      }
    }

    // Fetch fare stats per trip (from passengers)
    const { data: fareStats } = await supabase
      .from('jc_passengers')
      .select('trip_id, fare')
      .in('trip_id', tripIds)
      .eq('is_cancelled', false)
      .not('fare', 'is', null)
      .range(0, 49999)

    // Fetch revenue per trip (from bookings)
    const { data: revenueData } = await supabase
      .from('jc_bookings')
      .select('trip_id, total_fare')
      .in('trip_id', tripIds)
      .eq('booking_status', 'confirmed')
      .range(0, 9999)

    // Build lookup maps
    const bookingVelocityMap: Record<string, { last1h: number; last6h: number; last24h: number }> = {}
    for (const b of recentBookings || []) {
      if (!bookingVelocityMap[b.trip_id]) {
        bookingVelocityMap[b.trip_id] = { last1h: 0, last6h: 0, last24h: 0 }
      }
      const createdAt = new Date(b.created_at).getTime()
      bookingVelocityMap[b.trip_id].last24h++
      if (createdAt >= new Date(sixHoursAgo).getTime()) bookingVelocityMap[b.trip_id].last6h++
      if (createdAt >= new Date(oneHourAgo).getTime()) bookingVelocityMap[b.trip_id].last1h++
    }

    const fareStatsMap: Record<string, { avg: number; min: number; max: number }> = {}
    for (const p of fareStats || []) {
      if (!fareStatsMap[p.trip_id]) {
        fareStatsMap[p.trip_id] = { avg: 0, min: Infinity, max: 0 }
      }
      const fare = p.fare || 0
      fareStatsMap[p.trip_id].min = Math.min(fareStatsMap[p.trip_id].min, fare)
      fareStatsMap[p.trip_id].max = Math.max(fareStatsMap[p.trip_id].max, fare)
    }
    // Calculate averages
    const fareCounts: Record<string, number> = {}
    const fareSums: Record<string, number> = {}
    for (const p of fareStats || []) {
      const fare = p.fare || 0
      fareCounts[p.trip_id] = (fareCounts[p.trip_id] || 0) + 1
      fareSums[p.trip_id] = (fareSums[p.trip_id] || 0) + fare
    }
    for (const tripId of Object.keys(fareStatsMap)) {
      fareStatsMap[tripId].avg = fareCounts[tripId] > 0
        ? Math.round((fareSums[tripId] / fareCounts[tripId]) * 100) / 100
        : 0
      if (fareStatsMap[tripId].min === Infinity) fareStatsMap[tripId].min = 0
    }

    const revenueMap: Record<string, number> = {}
    for (const r of revenueData || []) {
      revenueMap[r.trip_id] = (revenueMap[r.trip_id] || 0) + (r.total_fare || 0)
    }

    // Build actual booked seats count from passengers (more reliable than jc_trips.booked_seats)
    const actualBookedSeatsMap: Record<string, number> = {}
    for (const tripId of Object.keys(fareCounts)) {
      actualBookedSeatsMap[tripId] = fareCounts[tripId]
    }

    // Build snapshots
    const snapshots: any[] = []

    for (const trip of trips) {
      const totalSeats = trip.total_seats || 40
      // Use passenger count as primary source for booked seats (more reliable)
      // Fall back to jc_trips.booked_seats only if no passenger data
      const bookedSeatsFromPassengers = actualBookedSeatsMap[trip.id] || 0
      const bookedSeats = bookedSeatsFromPassengers > 0 ? bookedSeatsFromPassengers : (trip.booked_seats || 0)
      const blockedSeats = trip.blocked_seats || 0
      const availableSeats = Math.max(0, totalSeats - bookedSeats - blockedSeats)
      const occupancyPercent = totalSeats > 0
        ? Math.round((bookedSeats / totalSeats) * 10000) / 100
        : 0

      // Calculate hours to departure
      let hoursToDeparture = 0
      if (trip.departure_datetime) {
        const departureTime = new Date(trip.departure_datetime).getTime()
        hoursToDeparture = Math.max(0, (departureTime - now.getTime()) / (1000 * 60 * 60))
      } else if (trip.travel_date && trip.departure_time) {
        const depStr = `${trip.travel_date}T${trip.departure_time}+05:30`
        const departureTime = new Date(depStr).getTime()
        hoursToDeparture = Math.max(0, (departureTime - now.getTime()) / (1000 * 60 * 60))
      }
      hoursToDeparture = Math.round(hoursToDeparture * 100) / 100

      // Day of week (0=Sunday, 6=Saturday)
      const travelDateObj = new Date(trip.travel_date + 'T00:00:00+05:30')
      const dayOfWeek = travelDateObj.getDay()
      const isWeekend = dayOfWeek === 0 || dayOfWeek === 5 || dayOfWeek === 6

      const velocity = bookingVelocityMap[trip.id] || { last1h: 0, last6h: 0, last24h: 0 }
      const fares = fareStatsMap[trip.id] || { avg: 0, min: 0, max: 0 }
      const routeBaseFare = (trip as any).jc_routes?.base_fare || 0

      snapshots.push({
        trip_id: trip.id,
        route_id: trip.route_id,
        service_number: trip.service_number,
        travel_date: trip.travel_date,
        departure_time: trip.departure_time,
        snapshot_at: new Date().toISOString(),
        total_seats: totalSeats,
        available_seats: availableSeats,
        booked_seats: bookedSeats,
        blocked_seats: blockedSeats,
        occupancy_percent: occupancyPercent,
        bookings_last_1h: velocity.last1h,
        bookings_last_6h: velocity.last6h,
        bookings_last_24h: velocity.last24h,
        cancellations_last_24h: cancellationTripMap[trip.id] || 0,
        hours_to_departure: hoursToDeparture,
        day_of_week: dayOfWeek,
        is_weekend: isWeekend,
        is_holiday: false,
        avg_fare_booked: fares.avg || null,
        min_fare_booked: fares.min || null,
        max_fare_booked: fares.max || null,
        route_base_fare: routeBaseFare,
        total_revenue_so_far: revenueMap[trip.id] || 0,
      })
    }

    // Batch insert snapshots
    let createdCount = 0
    let errorCount = 0

    // Insert in batches of 50
    for (let i = 0; i < snapshots.length; i += 50) {
      const batch = snapshots.slice(i, i + 50)
      const { error: insertError } = await supabase
        .from('jc_demand_snapshots')
        .insert(batch)

      if (insertError) {
        console.error(`[Demand Snapshot] Batch insert error:`, insertError.message)
        errorCount += batch.length
      } else {
        createdCount += batch.length
      }
    }

    const latency = Date.now() - startTime

    console.log(`[Demand Snapshot] Completed: ${createdCount} snapshots created, ${errorCount} errors in ${latency}ms`)

    // Update sync job
    if (syncJob?.id) {
      await supabase.from('jc_sync_jobs').update({
        status: errorCount > 0 ? 'completed_with_errors' : 'completed',
        completed_at: new Date().toISOString(),
        records_fetched: trips.length,
        records_created: createdCount,
        records_failed: errorCount,
        details: {
          trips_found: trips.length,
          snapshots_created: createdCount,
          errors: errorCount,
          latency_ms: latency,
          date_range: { from: istDate, to: endDateStr },
        },
      }).eq('id', syncJob.id)
    }

    return NextResponse.json({
      success: true,
      message: `Created ${createdCount} demand snapshots`,
      snapshots_created: createdCount,
      trips_analyzed: trips.length,
      errors: errorCount,
      latency_ms: latency,
      date_range: { from: istDate, to: endDateStr },
      sync_job_id: syncJob?.id,
      timestamp: new Date().toISOString(),
    })
  } catch (error: any) {
    console.error('[Demand Snapshot] Error:', error.message)
    return NextResponse.json(
      { success: false, error: error.message, timestamp: new Date().toISOString() },
      { status: 500 }
    )
  }
}

/**
 * POST /api/cron/demand-snapshot
 * Alternative POST endpoint (some cron services prefer POST)
 */
export async function POST(request: NextRequest) {
  return GET(request)
}
