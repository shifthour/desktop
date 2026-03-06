import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * Trips API - List and manage trips with real-time booking data
 * Uses jc_trips table as primary source, enriched with booking stats
 */
export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)

    const page = parseInt(searchParams.get('page') || '1')
    const limit = parseInt(searchParams.get('limit') || '50')
    const date = searchParams.get('date') || ''
    const status = searchParams.get('status') || ''

    // Get date filter (use local date)
    let dateFilter = date
    if (!dateFilter) {
      const now = new Date()
      dateFilter = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`
    }

    // First, try to get trips from jc_trips table with ALL details
    // NOTE: Supabase default limit is 1000, we need to explicitly set higher limit
    const { data: dbTrips, error: tripsError } = await supabase
      .from('jc_trips')
      .select(`
        id,
        route_id,
        travel_date,
        service_number,
        reservation_id,
        bus_type,
        departure_time,
        arrival_time,
        total_seats,
        available_seats,
        blocked_seats,
        booked_seats,
        phone_booking_count,
        blocked_seat_numbers,
        phone_booking_seat_numbers,
        coach_num,
        coach_id,
        coach_mobile_number,
        captain1_name,
        captain1_phone,
        captain2_name,
        captain2_phone,
        attendant_name,
        attendant_phone,
        helpline_number,
        origin,
        destination,
        route_duration,
        chart_operated_by,
        pickup_van_details,
        trip_status,
        last_synced_at
      `)
      .eq('travel_date', dateFilter)
      .order('departure_time', { ascending: true })
      .range(0, 9999)

    if (tripsError) {
      console.error('Error fetching trips:', tripsError)
    }

    // Fetch all routes to join manually (more reliable than foreign key join)
    const { data: allRoutes } = await supabase
      .from('jc_routes')
      .select('id, bitla_route_id, route_name, origin, destination, service_number, distance_km, base_fare')
      .range(0, 9999)

    // Build routes lookup maps
    const routesById: Record<string, any> = {}
    const routesByBitlaId: Record<number, any> = {}
    const routesByServiceNumber: Record<string, any> = {}

    allRoutes?.forEach((r: any) => {
      if (r.id) routesById[r.id] = r
      if (r.bitla_route_id) routesByBitlaId[r.bitla_route_id] = r
      if (r.service_number) routesByServiceNumber[r.service_number] = r
    })

    // Get all bookings for this date to calculate real-time stats
    // Also fetch captain details from bookings as fallback if trips don't have them
    // NOTE: Supabase default limit is 1000, we need to explicitly set higher limit
    const { data: bookings } = await supabase
      .from('jc_bookings')
      .select(`
        id,
        service_number,
        total_fare,
        booking_status,
        coach_num,
        captain1_details,
        captain2_details,
        bitla_route_id,
        jc_passengers (
          id,
          is_cancelled,
          is_boarded
        )
      `)
      .eq('travel_date', dateFilter)
      .range(0, 49999)

    // Helper function to parse captain details string (format: "Name - Phone")
    const parseCaptainDetails = (details: string | null | undefined) => {
      if (!details) return { name: null, phone: null }
      const parts = details.split(' - ')
      return {
        name: parts[0]?.trim() || null,
        phone: parts[1]?.trim() || null,
      }
    }

    // Build booking stats by service_number AND bitla_route_id
    // This allows matching trips by either key
    const bookingStatsMap: Record<string, any> = {}

    // Helper to get or create stats entry
    const getOrCreateStats = (key: string, booking: any) => {
      if (!bookingStatsMap[key]) {
        const captain1 = parseCaptainDetails(booking.captain1_details)
        const captain2 = parseCaptainDetails(booking.captain2_details)
        bookingStatsMap[key] = {
          booking_count: 0,
          booked_seats: 0,
          cancelled_seats: 0,
          boarded_count: 0,
          total_revenue: 0,
          coach_num: booking.coach_num || null,
          captain1_name: captain1.name,
          captain1_phone: captain1.phone,
          captain2_name: captain2.name,
          captain2_phone: captain2.phone,
          captain1_details_raw: booking.captain1_details,
          captain2_details_raw: booking.captain2_details,
        }
      }
      return bookingStatsMap[key]
    }

    bookings?.forEach((booking: any) => {
      const sn = booking.service_number
      const bitlaRouteId = booking.bitla_route_id
      if (!sn && !bitlaRouteId) return

      // Create/update stats for service_number key
      let stats = null
      if (sn) {
        stats = getOrCreateStats(sn, booking)
      }

      // Also create/update stats for bitla_route_id key (if different)
      if (bitlaRouteId && String(bitlaRouteId) !== sn) {
        const bitlaStats = getOrCreateStats(String(bitlaRouteId), booking)
        // Point to same object so updates apply to both
        if (stats && stats !== bitlaStats) {
          // Merge stats - use the one with more data
          bookingStatsMap[String(bitlaRouteId)] = stats
        } else if (!stats) {
          stats = bitlaStats
        }
      }

      if (!stats) return

      // Update stats (this will update for all keys pointing to this object)
      stats.booking_count += 1

      const passengers = booking.jc_passengers || []
      passengers.forEach((p: any) => {
        if (p.is_cancelled) {
          stats.cancelled_seats += 1
        } else {
          stats.booked_seats += 1
          if (p.is_boarded) {
            stats.boarded_count += 1
          }
        }
      })

      if (booking.booking_status === 'confirmed') {
        stats.total_revenue += booking.total_fare || 0
      }
    })

    let trips: any[] = []

    // Track which service_numbers we've already added from jc_trips
    const addedServiceNumbers = new Set<string>()

    // First, add trips from jc_trips table (if any)
    if (dbTrips && dbTrips.length > 0) {
      dbTrips.forEach((trip: any) => {
        // Find the route using multiple lookup strategies
        let route = null

        // 1. Try by route_id (UUID) if available
        if (trip.route_id && routesById[trip.route_id]) {
          route = routesById[trip.route_id]
        }
        // 2. Try by reservation_id (which is often the bitla_route_id)
        else if (trip.reservation_id && routesByBitlaId[trip.reservation_id]) {
          route = routesByBitlaId[trip.reservation_id]
        }
        // 3. Try by service_number
        else if (trip.service_number && routesByServiceNumber[trip.service_number]) {
          route = routesByServiceNumber[trip.service_number]
        }

        const sn = trip.service_number || route?.service_number || route?.bitla_route_id
        const bitlaRouteId = route?.bitla_route_id
        if (sn) addedServiceNumbers.add(String(sn))

        // Try to find booking stats by multiple keys: service_number, bitla_route_id
        const stats = bookingStatsMap[sn]
          || bookingStatsMap[String(bitlaRouteId)]
          || bookingStatsMap[trip.service_number]
          || {
            booking_count: 0,
            booked_seats: trip.booked_seats || 0,
            cancelled_seats: 0,
            boarded_count: 0,
            total_revenue: 0,
          }

        // Use values from jc_trips directly - use nullish coalescing (??) to handle 0 correctly
        const totalSeats = trip.total_seats ?? 40
        const bookedSeats = trip.booked_seats ?? stats.booked_seats ?? 0
        const blockedSeats = trip.blocked_seats ?? 0
        const phoneBookingCount = trip.phone_booking_count ?? 0
        // Available seats: prefer stored value, only calculate if null/undefined
        const availableSeats = trip.available_seats ?? (totalSeats - bookedSeats - blockedSeats)

        trips.push({
          id: trip.id,
          service_number: sn,
          travel_date: trip.travel_date,
          departure_time: trip.departure_time || '--:--',
          arrival_time: trip.arrival_time || '--:--',
          bus_type: trip.bus_type || 'Sleeper A/C',

          // Seat details from jc_trips
          total_seats: totalSeats,
          booked_seats: bookedSeats,
          available_seats: availableSeats,
          blocked_seats: blockedSeats,
          phone_booking_count: phoneBookingCount,
          blocked_seat_numbers: trip.blocked_seat_numbers,
          phone_booking_seat_numbers: trip.phone_booking_seat_numbers,

          // Booking stats (from real-time calculation)
          cancelled_seats: stats.cancelled_seats,
          boarded_count: stats.boarded_count,
          booking_count: stats.booking_count,
          total_revenue: stats.total_revenue,
          avg_fare: bookedSeats > 0 ? Math.round(stats.total_revenue / bookedSeats) : 0,
          occupancy_percent: totalSeats > 0 ? Math.round((bookedSeats / totalSeats) * 100) : 0,

          // Coach/Bus details - fallback to booking data if trip doesn't have it
          coach_num: trip.coach_num || stats.coach_num,
          coach_id: trip.coach_id,
          coach_mobile_number: trip.coach_mobile_number,
          vehicle_number: trip.coach_num || stats.coach_num, // Alias for UI

          // Crew details - fallback to parsed booking data if trip doesn't have it
          captain1_name: trip.captain1_name || stats.captain1_name,
          captain1_phone: trip.captain1_phone || stats.captain1_phone,
          captain2_name: trip.captain2_name || stats.captain2_name,
          captain2_phone: trip.captain2_phone || stats.captain2_phone,
          attendant_name: trip.attendant_name,
          attendant_phone: trip.attendant_phone,
          helpline_number: trip.helpline_number,
          // Raw captain details for debugging/display
          captain1_details_raw: stats.captain1_details_raw,
          captain2_details_raw: stats.captain2_details_raw,

          // Route details from trip
          origin: trip.origin || route?.origin,
          destination: trip.destination || route?.destination,
          route_duration: trip.route_duration,

          // Other details
          chart_operated_by: trip.chart_operated_by,
          pickup_van_details: trip.pickup_van_details,
          trip_status: trip.trip_status || 'scheduled',
          last_synced_at: trip.last_synced_at,

          // Route relation
          jc_routes: route ? {
            id: route.id,
            bitla_route_id: route.bitla_route_id,
            origin: route.origin,
            destination: route.destination,
            route_name: route.route_name,
            service_number: route.service_number || route.bitla_route_id,
            distance_km: route.distance_km || 0,
            base_fare: route.base_fare || 0,
          } : null,
        })
      })
    }

    // NOTE: We only show trips that exist in jc_trips table
    // Bookings without a corresponding trip record are NOT shown here
    // (They would need to be synced via pull-data first)

    // Filter out trips with 0 bookings - only show trips that have passengers
    trips = trips.filter((trip: any) => trip.booked_seats > 0)

    // Sort by route ID (bitla_route_id), then by departure_time
    trips.sort((a, b) => {
      // Primary sort: by bitla_route_id (numeric)
      const routeIdA = a.jc_routes?.bitla_route_id || 0
      const routeIdB = b.jc_routes?.bitla_route_id || 0
      if (routeIdA !== routeIdB) {
        return routeIdA - routeIdB
      }
      // Secondary sort: by departure_time
      return (a.departure_time || '').localeCompare(b.departure_time || '')
    })

    // Apply status filter if provided
    if (status && status !== 'all') {
      trips = trips.filter((t: any) => t.trip_status === status)
    }

    // Paginate
    const offset = (page - 1) * limit
    const paginatedTrips = trips.slice(offset, offset + limit)

    return NextResponse.json({
      trips: paginatedTrips,
      pagination: {
        page,
        limit,
        total: trips.length,
        totalPages: Math.ceil(trips.length / limit),
      },
      debug: {
        jc_trips_count: dbTrips?.length || 0,
        date_filter: dateFilter,
      },
    })
  } catch (error: any) {
    console.error('Trips API error:', error)
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    )
  }
}
