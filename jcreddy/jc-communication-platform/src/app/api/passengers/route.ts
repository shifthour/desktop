import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * Passengers API - List and search passengers
 */
export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)

    const page = parseInt(searchParams.get('page') || '1')
    const limit = parseInt(searchParams.get('limit') || '20')
    const search = searchParams.get('search') || ''
    const status = searchParams.get('status') || ''
    const routeId = searchParams.get('routeId') || ''
    const routeName = searchParams.get('routeName') || ''
    const dateFrom = searchParams.get('dateFrom') || ''
    const dateTo = searchParams.get('dateTo') || ''

    const offset = (page - 1) * limit

    // Build booking filter combining date and route filters
    // Date filter should use booking's travel_date, not passenger's created_at
    let filteredBookingIds: string[] | null = null

    if (dateFrom || dateTo || routeId) {
      // Start building booking query
      let bookingQuery = supabase.from('jc_bookings').select('id, bitla_route_id')

      // Apply date filter on booking's travel_date
      if (dateFrom) {
        bookingQuery = bookingQuery.gte('travel_date', dateFrom)
      }
      if (dateTo) {
        bookingQuery = bookingQuery.lte('travel_date', dateTo)
      }

      // Apply route filter if provided
      if (routeId) {
        // Get route's bitla_route_id and service_number
        const { data: route } = await supabase
          .from('jc_routes')
          .select('bitla_route_id, service_number')
          .eq('id', routeId)
          .single()

        const bitlaRouteId = route?.bitla_route_id || null
        const serviceNumber = route?.service_number || null

        console.log(`[Passengers API] Route filter - routeId: ${routeId}, bitlaRouteId: ${bitlaRouteId}, serviceNumber: ${serviceNumber}`)

        // Add route filter to booking query
        if (bitlaRouteId) {
          bookingQuery = bookingQuery.eq('bitla_route_id', bitlaRouteId)
        } else if (serviceNumber) {
          bookingQuery = bookingQuery.eq('service_number', serviceNumber)
        }
      }

      // Execute the booking query
      const { data: matchingBookings } = await bookingQuery.range(0, 9999)

      console.log(`[Passengers API] Found ${matchingBookings?.length || 0} bookings matching filters`)

      if (matchingBookings && matchingBookings.length > 0) {
        filteredBookingIds = matchingBookings.map((b: any) => b.id)
      } else {
        // No bookings match the filters
        return NextResponse.json({
          passengers: [],
          stats: { total: 0, boarded: 0, pending: 0, cancelled: 0 },
          pagination: { page, limit, total: 0, totalPages: 0 },
        })
      }
    }

    // Build query with all related data
    let query = supabase
      .from('jc_passengers')
      .select(`
        id,
        pnr_number,
        full_name,
        mobile,
        email,
        seat_number,
        fare,
        is_boarded,
        is_cancelled,
        is_confirmed,
        booking_id,
        trip_id
      `, { count: 'exact' })

    // Apply booking filter (includes date and route filtering via booking IDs)
    if (filteredBookingIds) {
      query = query.in('booking_id', filteredBookingIds.slice(0, 5000))
    }

    // Note: Date filtering is now done via booking's travel_date, not passenger's created_at

    // Apply search filter
    if (search) {
      query = query.or(`full_name.ilike.%${search}%,mobile.ilike.%${search}%,pnr_number.ilike.%${search}%,email.ilike.%${search}%`)
    }

    // Apply status filter
    if (status === 'confirmed') {
      query = query.eq('is_confirmed', true).eq('is_cancelled', false)
    } else if (status === 'boarded') {
      query = query.eq('is_boarded', true)
    } else if (status === 'cancelled') {
      query = query.eq('is_cancelled', true)
    }

    // Apply sorting and pagination
    query = query
      .order('created_at', { ascending: false })
      .range(offset, offset + limit - 1)

    const { data: passengersRaw, count, error } = await query

    if (error) {
      throw error
    }

    // Fetch related booking, trip, and route data
    const bookingIds = passengersRaw
      ? Array.from(new Set(passengersRaw.map(p => p.booking_id).filter(Boolean)))
      : []
    const tripIds = passengersRaw
      ? Array.from(new Set(passengersRaw.map(p => p.trip_id).filter(Boolean)))
      : []

    // Fetch bookings with service_number (route_id)
    let bookingsMap: Record<string, any> = {}
    if (bookingIds.length > 0) {
      const { data: bookings } = await supabase
        .from('jc_bookings')
        .select('id, travel_operator_pnr, travel_date, origin, destination, boarding_point_name, dropoff_point_name, total_fare, booked_by, booking_status, trip_id, service_number, boarding_time')
        .in('id', bookingIds)

      if (bookings) {
        bookings.forEach(b => {
          bookingsMap[b.id] = b
        })
      }
    }

    // Fetch all passengers to build repeat customer detection
    // NOTE: Supabase default limit is 1000, we need to explicitly set higher limit
    const { data: allPassengers } = await supabase
      .from('jc_passengers')
      .select('mobile, booking_id')
      .not('mobile', 'is', null)
      .range(0, 49999)

    // Build a map of phone numbers to unique booking IDs and booked_by sources
    const phoneToBookingsMap = new Map<string, Set<string>>()
    const phoneToBookedByMap = new Map<string, Set<string>>()

    // We need to get booked_by for all bookings related to passengers
    // NOTE: Supabase default limit is 1000, we need to explicitly set higher limit
    const allBookingIds = allPassengers?.map(p => p.booking_id).filter(Boolean) || []
    let allBookingsData: Record<string, any> = {}
    if (allBookingIds.length > 0) {
      const uniqueBookingIds = Array.from(new Set(allBookingIds))
      const { data: allBookings } = await supabase
        .from('jc_bookings')
        .select('id, booked_by')
        .in('id', uniqueBookingIds.slice(0, 50000))
        .range(0, 49999)

      if (allBookings) {
        allBookings.forEach(b => {
          allBookingsData[b.id] = b
        })
      }
    }

    allPassengers?.forEach((p: any) => {
      if (p.mobile && !p.mobile.includes('x') && !p.mobile.includes('X') && p.booking_id) {
        const cleanPhone = p.mobile.replace(/\D/g, '')
        if (!phoneToBookingsMap.has(cleanPhone)) {
          phoneToBookingsMap.set(cleanPhone, new Set())
          phoneToBookedByMap.set(cleanPhone, new Set())
        }
        phoneToBookingsMap.get(cleanPhone)!.add(p.booking_id)

        // Add booked_by source
        const booking = allBookingsData[p.booking_id]
        if (booking?.booked_by) {
          // Clean up booked_by - remove text after "/"
          let bookedVia = booking.booked_by
          if (bookedVia.includes('/')) {
            bookedVia = bookedVia.split('/')[0].trim()
          }
          phoneToBookedByMap.get(cleanPhone)!.add(bookedVia)
        }
      }
    })

    // Helper to get booking count for a phone
    const getBookingCount = (cleanPhone: string): number => {
      return phoneToBookingsMap.get(cleanPhone)?.size || 0
    }

    // Helper to get all booking sources for a phone
    const getBookedBySources = (cleanPhone: string): string[] => {
      return Array.from(phoneToBookedByMap.get(cleanPhone) || [])
    }

    // Fetch trips with routes
    let tripsMap: Record<string, any> = {}
    let routesMap: Record<string, any> = {}
    if (tripIds.length > 0) {
      const { data: trips } = await supabase
        .from('jc_trips')
        .select('id, service_number, bus_type, departure_time, route_id')
        .in('id', tripIds)

      if (trips) {
        const routeIds = Array.from(new Set(trips.map(t => t.route_id).filter(Boolean)))

        // Fetch routes
        if (routeIds.length > 0) {
          const { data: routes } = await supabase
            .from('jc_routes')
            .select('id, route_name, origin, destination, service_number')
            .in('id', routeIds)

          if (routes) {
            routes.forEach(r => {
              routesMap[r.id] = r
            })
          }
        }

        trips.forEach(t => {
          tripsMap[t.id] = {
            ...t,
            route: t.route_id ? routesMap[t.route_id] : null
          }
        })
      }
    }

    // Map passengers with related data and repeat info
    const passengers = passengersRaw?.map(p => {
      const cleanPhone = p.mobile?.replace(/\D/g, '') || ''
      const repeatCount = getBookingCount(cleanPhone)
      const isRepeat = repeatCount > 1
      let bookedBySources = getBookedBySources(cleanPhone)

      // Fallback: if no sources found, use the current booking's booked_by
      const currentBooking = p.booking_id ? bookingsMap[p.booking_id] : null
      if (bookedBySources.length === 0 && currentBooking?.booked_by) {
        let bookedVia = currentBooking.booked_by
        if (bookedVia.includes('/')) {
          bookedVia = bookedVia.split('/')[0].trim()
        }
        bookedBySources = [bookedVia]
      }

      return {
        id: p.id,
        pnr_number: p.pnr_number,
        full_name: p.full_name,
        mobile: p.mobile,
        email: p.email,
        seat_number: p.seat_number,
        fare: p.fare,
        is_boarded: p.is_boarded,
        is_cancelled: p.is_cancelled,
        is_confirmed: p.is_confirmed,
        is_repeat: isRepeat,
        repeat_count: repeatCount,
        booked_by_sources: bookedBySources,
        booking: p.booking_id ? bookingsMap[p.booking_id] : null,
        trip: p.trip_id ? tripsMap[p.trip_id] : null,
        route: p.trip_id && tripsMap[p.trip_id]?.route ? tripsMap[p.trip_id].route : null,
      }
    }) || []

    // Build base query for stats - use count queries instead of fetching all data
    const buildStatsBaseQuery = () => {
      let q = supabase.from('jc_passengers').select('*', { count: 'exact', head: true })
      // Date filtering is done via filteredBookingIds (booking's travel_date)
      if (filteredBookingIds) {
        q = q.in('booking_id', filteredBookingIds.slice(0, 5000))
      }
      if (search) {
        q = q.or(`full_name.ilike.%${search}%,mobile.ilike.%${search}%,pnr_number.ilike.%${search}%,email.ilike.%${search}%`)
      }
      return q
    }

    // Get counts for each status using separate count queries (more reliable than fetching all data)
    const [totalResult, boardedResult, cancelledResult] = await Promise.all([
      // Total count (with status filter if applied)
      status === 'confirmed'
        ? buildStatsBaseQuery().eq('is_confirmed', true).eq('is_cancelled', false)
        : status === 'boarded'
        ? buildStatsBaseQuery().eq('is_boarded', true)
        : status === 'cancelled'
        ? buildStatsBaseQuery().eq('is_cancelled', true)
        : buildStatsBaseQuery(),
      // Boarded count
      buildStatsBaseQuery().eq('is_boarded', true),
      // Cancelled count
      buildStatsBaseQuery().eq('is_cancelled', true),
    ])

    const totalCount = totalResult.count || 0
    const boardedCount = boardedResult.count || 0
    const cancelledCount = cancelledResult.count || 0

    const stats = {
      total: totalCount,
      boarded: boardedCount,
      pending: totalCount - boardedCount - cancelledCount,
      cancelled: cancelledCount,
    }

    return NextResponse.json({
      passengers,
      stats,
      pagination: {
        page,
        limit,
        total: count || 0,
        totalPages: Math.ceil((count || 0) / limit),
      },
    })
  } catch (error: any) {
    console.error('Passengers API error:', error)
    console.error('Error stack:', error.stack)
    return NextResponse.json(
      { error: error.message || 'Internal server error', details: error.toString() },
      { status: 500 }
    )
  }
}

/**
 * Get single passenger by ID
 */
export async function POST(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const body = await request.json()
    const { passengerId } = body

    if (!passengerId) {
      return NextResponse.json(
        { error: 'Passenger ID required' },
        { status: 400 }
      )
    }

    const { data: passenger, error } = await supabase
      .from('jc_passengers')
      .select(`
        *,
        jc_bookings (
          *
        ),
        jc_notifications (
          id,
          notification_type,
          channel,
          status,
          sent_at,
          delivered_at,
          error_message
        )
      `)
      .eq('id', passengerId)
      .single()

    if (error) {
      throw error
    }

    return NextResponse.json({ passenger })
  } catch (error: any) {
    console.error('Get passenger error:', error)
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    )
  }
}
