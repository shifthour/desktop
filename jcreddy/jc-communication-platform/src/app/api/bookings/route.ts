import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * Bookings API - List and search bookings
 */
export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)

    const page = parseInt(searchParams.get('page') || '1')
    const limit = parseInt(searchParams.get('limit') || '20')
    const search = searchParams.get('search') || ''
    const status = searchParams.get('status') || ''
    const dateFrom = searchParams.get('dateFrom') || ''
    const dateTo = searchParams.get('dateTo') || ''
    const dateType = searchParams.get('dateType') || 'travel' // 'travel' or 'booked'
    const sortBy = searchParams.get('sortBy') || 'created_at'
    const sortOrder = searchParams.get('sortOrder') || 'desc'
    const passengerType = searchParams.get('passengerType') || ''

    const offset = (page - 1) * limit

    // Get route filters
    const routeId = searchParams.get('routeId') || ''
    const routeName = searchParams.get('routeName') || ''

    // Build query
    let query = supabase
      .from('jc_bookings')
      .select(`
        *,
        jc_passengers (
          id,
          full_name,
          mobile,
          seat_number,
          fare,
          is_boarded,
          is_cancelled
        ),
        jc_trips (
          id,
          route_id,
          jc_routes (
            id,
            bitla_route_id,
            route_name,
            service_number
          )
        ),
        jc_cancellations (
          id,
          cancelled_at,
          cancelled_by,
          cancellation_charge,
          refund_amount,
          original_fare,
          cancellation_reason,
          refund_status
        )
      `, { count: 'exact' })

    // Apply filters
    if (search) {
      query = query.or(`travel_operator_pnr.ilike.%${search}%,ticket_number.ilike.%${search}%,origin.ilike.%${search}%,destination.ilike.%${search}%`)
    }

    if (status && status !== 'all') {
      query = query.eq('booking_status', status)
    }

    // Apply date range filter based on dateType
    // travel = travel_date (journey date), booked = created_at (booking date)
    if (dateType === 'travel') {
      // travel_date is a date field (no time component)
      if (dateFrom) {
        query = query.gte('travel_date', dateFrom)
      }
      if (dateTo) {
        query = query.lte('travel_date', dateTo)
      }
    } else {
      // created_at is a timestamp field
      if (dateFrom) {
        query = query.gte('created_at', `${dateFrom}T00:00:00`)
      }
      if (dateTo) {
        query = query.lte('created_at', `${dateTo}T23:59:59`)
      }
    }

    // Apply route filter - filter by service_number only (not origin, as passengers can board from different cities)
    // routeId format can be "service_number|origin" or just "service_number"
    if (routeId) {
      if (routeId.includes('|')) {
        const [serviceNum] = routeId.split('|')
        query = query.eq('service_number', serviceNum)
        // Note: Removed origin filter as same route can have passengers from different boarding points
      } else {
        query = query.eq('service_number', routeId)
      }
    }

    // Legacy filter by internal route ID if needed
    const internalRouteId = searchParams.get('internalRouteId') || ''
    if (internalRouteId) {
      // Get trips for this route
      const { data: trips } = await supabase
        .from('jc_trips')
        .select('id')
        .eq('route_id', internalRouteId)

      if (trips && trips.length > 0) {
        const tripIds = trips.map(t => t.id)
        query = query.in('trip_id', tripIds)
      } else {
        // No trips for this route, return empty results
        return NextResponse.json({
          bookings: [],
          pagination: {
            page,
            limit,
            total: 0,
            totalPages: 0,
          },
        })
      }
    }

    // Apply sorting and pagination
    query = query
      .order(sortBy, { ascending: sortOrder === 'asc' })
      .range(offset, offset + limit - 1)

    const { data: bookings, count, error } = await query

    if (error) {
      throw error
    }

    // Debug: Log the booking statuses returned
    const statusCounts = bookings?.reduce((acc: any, b: any) => {
      acc[b.booking_status] = (acc[b.booking_status] || 0) + 1
      return acc
    }, {})
    console.log(`[Bookings API] Query returned ${bookings?.length} bookings, statuses:`, statusCounts)

    // Get all unique phone numbers from current bookings for repeat customer check
    const phoneNumbers = new Set<string>()
    const passengerNames = new Map<string, string>() // phone -> name mapping

    bookings?.forEach((booking: any) => {
      booking.jc_passengers?.forEach((p: any) => {
        if (p.mobile && !p.mobile.includes('x') && !p.mobile.includes('X')) {
          phoneNumbers.add(p.mobile)
          passengerNames.set(p.mobile, p.full_name)
        }
      })
    })

    // Fetch all passengers to check for repeat customers
    // NOTE: Supabase default limit is 1000, we need to explicitly set higher limit
    const { data: allPassengers } = await supabase
      .from('jc_passengers')
      .select('mobile, full_name, booking_id')
      .not('mobile', 'is', null)
      .range(0, 49999)

    // Build a map of phone numbers to UNIQUE booking IDs (for repeat detection)
    // This ensures we count unique bookings, not just passenger records
    const phoneToBookingsMap = new Map<string, Set<string>>()
    const phoneToNameMap = new Map<string, string>() // For tentative phone matching

    allPassengers?.forEach((p: any) => {
      if (p.mobile && !p.mobile.includes('x') && !p.mobile.includes('X') && p.booking_id) {
        const cleanPhone = p.mobile.replace(/\D/g, '')
        if (!phoneToBookingsMap.has(cleanPhone)) {
          phoneToBookingsMap.set(cleanPhone, new Set())
        }
        phoneToBookingsMap.get(cleanPhone)!.add(p.booking_id)
        phoneToNameMap.set(cleanPhone, p.full_name)
      }
    })

    // Helper to get booking count for a phone
    const getBookingCount = (cleanPhone: string): number => {
      return phoneToBookingsMap.get(cleanPhone)?.size || 0
    }

    // Helper function to check if masked phone matches any real phone
    const findTentativePhone = (maskedPhone: string, passengerName: string): string | null => {
      if (!maskedPhone || (!maskedPhone.includes('x') && !maskedPhone.includes('X'))) {
        return null
      }

      // Extract prefix (first 3 digits) and suffix (last 4 digits) from masked phone
      const cleanMasked = maskedPhone.replace(/\D/g, '')
      const prefix = cleanMasked.slice(0, 3)
      const suffix = cleanMasked.slice(-4)

      // Find matching phone number with same name
      const phones = Array.from(phoneToNameMap.keys())
      for (const phone of phones) {
        if (phone.startsWith(prefix) && phone.endsWith(suffix)) {
          const name = phoneToNameMap.get(phone)
          // Check if name matches (case-insensitive, trim spaces)
          const normalizedStoredName = name?.toLowerCase().trim()
          const normalizedPassengerName = passengerName?.toLowerCase().trim()
          if (normalizedStoredName === normalizedPassengerName) {
            return phone
          }
        }
      }
      return null
    }

    // Enrich bookings with repeat customer info
    const enrichedBookings = bookings?.map((booking: any) => {
      const enrichedPassengers = booking.jc_passengers?.map((passenger: any) => {
        const cleanPhone = passenger.mobile?.replace(/\D/g, '') || ''
        const isMasked = passenger.mobile?.includes('x') || passenger.mobile?.includes('X')

        let isRepeat = false
        let repeatCount = 0
        let tentativePhone: string | null = null

        if (isMasked) {
          // For masked phones (released tickets), try to find tentative match
          tentativePhone = findTentativePhone(passenger.mobile, passenger.full_name)
          if (tentativePhone) {
            repeatCount = getBookingCount(tentativePhone)
            isRepeat = repeatCount > 1
          }
        } else {
          // For regular phones, check if has more than one unique booking
          repeatCount = getBookingCount(cleanPhone)
          isRepeat = repeatCount > 1
        }

        return {
          ...passenger,
          is_repeat: isRepeat,
          repeat_count: repeatCount,
          tentative_phone: tentativePhone,
        }
      })

      return {
        ...booking,
        jc_passengers: enrichedPassengers,
      }
    })

    // Filter by passenger type (new/repeat) if specified
    let filteredBookings = enrichedBookings
    if (passengerType === 'new') {
      filteredBookings = enrichedBookings?.filter((booking: any) => {
        const primaryPassenger = booking.jc_passengers?.[0]
        return primaryPassenger && !primaryPassenger.is_repeat
      })
    } else if (passengerType === 'repeat') {
      filteredBookings = enrichedBookings?.filter((booking: any) => {
        const primaryPassenger = booking.jc_passengers?.[0]
        return primaryPassenger && primaryPassenger.is_repeat
      })
    }

    // Build base query for stats with same filters
    const buildStatsBaseQuery = () => {
      let q = supabase.from('jc_bookings').select('total_seats', { count: 'exact' })
      if (search) {
        q = q.or(`travel_operator_pnr.ilike.%${search}%,ticket_number.ilike.%${search}%,origin.ilike.%${search}%,destination.ilike.%${search}%`)
      }
      // Apply date filter based on dateType (same as main query)
      if (dateType === 'travel') {
        if (dateFrom) {
          q = q.gte('travel_date', dateFrom)
        }
        if (dateTo) {
          q = q.lte('travel_date', dateTo)
        }
      } else {
        if (dateFrom) {
          q = q.gte('created_at', `${dateFrom}T00:00:00`)
        }
        if (dateTo) {
          q = q.lte('created_at', `${dateTo}T23:59:59`)
        }
      }
      if (routeId) {
        if (routeId.includes('|')) {
          const [serviceNum] = routeId.split('|')
          q = q.eq('service_number', serviceNum)
        } else {
          q = q.eq('service_number', routeId)
        }
      }
      return q
    }

    // Get passenger/seat counts by joining bookings with passengers
    // This counts actual passengers (seats) not just booking records
    const buildPassengerCountQuery = (statusFilter?: string) => {
      let q = supabase
        .from('jc_bookings')
        .select('id, booking_status')
      if (search) {
        q = q.or(`travel_operator_pnr.ilike.%${search}%,ticket_number.ilike.%${search}%,origin.ilike.%${search}%,destination.ilike.%${search}%`)
      }
      if (dateType === 'travel') {
        if (dateFrom) q = q.gte('travel_date', dateFrom)
        if (dateTo) q = q.lte('travel_date', dateTo)
      } else {
        if (dateFrom) q = q.gte('created_at', `${dateFrom}T00:00:00`)
        if (dateTo) q = q.lte('created_at', `${dateTo}T23:59:59`)
      }
      if (routeId) {
        if (routeId.includes('|')) {
          const [serviceNum] = routeId.split('|')
          q = q.eq('service_number', serviceNum)
        } else {
          q = q.eq('service_number', routeId)
        }
      }
      if (statusFilter) q = q.eq('booking_status', statusFilter)
      return q.range(0, 99999)
    }

    // Get bookings for stats - apply status filter if selected
    // When user filters by a status, stats should only show that status data
    const { data: allFilteredBookings } = await buildPassengerCountQuery(status && status !== 'all' ? status : undefined)

    // Get booking IDs for passenger count
    const bookingIds = allFilteredBookings?.map((b: any) => b.id) || []

    // Count passengers for these bookings (shows seat counts, not booking counts)
    let passengerCounts = { total: 0, pending: 0, confirmed: 0, released: 0, cancelled: 0 }

    if (bookingIds.length > 0) {
      // Get all passengers for these bookings
      const { data: passengers } = await supabase
        .from('jc_passengers')
        .select('booking_id, is_cancelled')
        .in('booking_id', bookingIds)

      // Count passengers by booking status
      const bookingStatusMap = new Map(allFilteredBookings?.map((b: any) => [b.id, b.booking_status]) || [])

      passengers?.forEach((p: any) => {
        const bookingStatus = bookingStatusMap.get(p.booking_id)
        passengerCounts.total++
        if (bookingStatus === 'pending') passengerCounts.pending++
        else if (bookingStatus === 'confirmed') passengerCounts.confirmed++
        else if (bookingStatus === 'released') passengerCounts.released++
        else if (bookingStatus === 'cancelled') passengerCounts.cancelled++
      })
    }

    // Debug: Log filter parameters
    console.log(`[Bookings API] Filters - status: "${status}", dateFrom: "${dateFrom}", dateTo: "${dateTo}", routeId: "${routeId}"`)
    console.log(`[Bookings API] Passenger counts - total: ${passengerCounts.total}, confirmed: ${passengerCounts.confirmed}, released: ${passengerCounts.released}`)

    // Get revenue separately (need actual data for sum)
    let revenueQuery = supabase
      .from('jc_bookings')
      .select('total_fare')
      .eq('booking_status', 'confirmed')
    if (search) {
      revenueQuery = revenueQuery.or(`travel_operator_pnr.ilike.%${search}%,ticket_number.ilike.%${search}%,origin.ilike.%${search}%,destination.ilike.%${search}%`)
    }
    // Apply date filter based on dateType
    if (dateType === 'travel') {
      if (dateFrom) {
        revenueQuery = revenueQuery.gte('travel_date', dateFrom)
      }
      if (dateTo) {
        revenueQuery = revenueQuery.lte('travel_date', dateTo)
      }
    } else {
      if (dateFrom) {
        revenueQuery = revenueQuery.gte('created_at', `${dateFrom}T00:00:00`)
      }
      if (dateTo) {
        revenueQuery = revenueQuery.lte('created_at', `${dateTo}T23:59:59`)
      }
    }
    if (routeId) {
      if (routeId.includes('|')) {
        const [serviceNum] = routeId.split('|')
        revenueQuery = revenueQuery.eq('service_number', serviceNum)
      } else {
        revenueQuery = revenueQuery.eq('service_number', routeId)
      }
    }
    revenueQuery = revenueQuery.range(0, 99999)

    const { data: revenueData } = await revenueQuery
    const totalRevenue = revenueData?.reduce((sum: number, b: any) => sum + (b.total_fare || 0), 0) || 0

    // Calculate stats using passenger counts (actual seats)
    const stats = {
      total: passengerCounts.total,
      pending: passengerCounts.pending,
      confirmed: passengerCounts.confirmed,
      released: passengerCounts.released,
      cancelled: passengerCounts.cancelled,
      revenue: totalRevenue,
    }

    return NextResponse.json({
      bookings: filteredBookings,
      stats,
      pagination: {
        page,
        limit,
        total: passengerType ? (filteredBookings?.length || 0) : (count || 0),
        totalPages: passengerType ? Math.ceil((filteredBookings?.length || 0) / limit) : Math.ceil((count || 0) / limit),
      },
    })
  } catch (error: any) {
    console.error('Bookings API error:', error)
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    )
  }
}
