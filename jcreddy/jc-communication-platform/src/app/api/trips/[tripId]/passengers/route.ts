import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * GET /api/trips/[tripId]/passengers
 * Fetch all passengers for a specific trip, grouped by boarding point
 */
export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ tripId: string }> }
) {
  try {
    const { tripId } = await params
    const supabase = createServerClient()

    // Get the trip details first
    const { data: trip, error: tripError } = await supabase
      .from('jc_trips')
      .select('id, travel_date, service_number, origin, destination, departure_time')
      .eq('id', tripId)
      .single()

    if (tripError || !trip) {
      return NextResponse.json(
        { error: 'Trip not found' },
        { status: 404 }
      )
    }

    // Get all bookings for this trip (by trip_id or by travel_date + service_number)
    let bookingIds: string[] = []

    // First try by trip_id
    const { data: bookingsByTrip } = await supabase
      .from('jc_bookings')
      .select('id, boarding_point_name, boarding_time, boarding_address')
      .eq('trip_id', tripId)
      .eq('booking_status', 'confirmed')

    if (bookingsByTrip && bookingsByTrip.length > 0) {
      bookingIds = bookingsByTrip.map(b => b.id)
    } else {
      // Fallback: get bookings by travel_date + service_number
      const { data: bookingsByService } = await supabase
        .from('jc_bookings')
        .select('id, boarding_point_name, boarding_time, boarding_address')
        .eq('travel_date', trip.travel_date)
        .eq('service_number', trip.service_number)
        .eq('booking_status', 'confirmed')

      if (bookingsByService) {
        bookingIds = bookingsByService.map(b => b.id)
      }
    }

    if (bookingIds.length === 0) {
      return NextResponse.json({
        tripId,
        trip,
        boardingPoints: [],
        totalPassengers: 0,
      })
    }

    // Get all passengers for these bookings with booking details
    const { data: passengers, error: passengersError } = await supabase
      .from('jc_passengers')
      .select(`
        id,
        full_name,
        mobile,
        email,
        seat_number,
        pnr_number,
        fare,
        is_boarded,
        is_cancelled,
        booking_id,
        jc_bookings (
          id,
          boarding_point_name,
          boarding_time,
          boarding_address,
          origin,
          destination,
          travel_operator_pnr,
          booked_by
        )
      `)
      .in('booking_id', bookingIds)
      .eq('is_cancelled', false)
      .order('seat_number', { ascending: true })

    if (passengersError) {
      console.error('Error fetching passengers:', passengersError)
      return NextResponse.json(
        { error: 'Failed to fetch passengers' },
        { status: 500 }
      )
    }

    // Helper to extract time from boarding point name (e.g., "Location - 08:30 PM")
    const extractTimeFromName = (name: string): string | null => {
      // Match patterns like "08:30 PM", "8:30 PM", "20:30"
      const timeMatch = name.match(/(\d{1,2}:\d{2}\s*(?:AM|PM|am|pm)?)/i)
      if (timeMatch) {
        return timeMatch[1].trim()
      }
      return null
    }

    // Helper to convert 12h time to 24h for sorting (e.g., "08:30 PM" -> "20:30")
    const to24Hour = (timeStr: string | null): string | null => {
      if (!timeStr) return null
      const match = timeStr.match(/(\d{1,2}):(\d{2})\s*(AM|PM|am|pm)?/i)
      if (!match) return timeStr

      let hours = parseInt(match[1], 10)
      const mins = match[2]
      const period = match[3]?.toUpperCase()

      if (period === 'PM' && hours !== 12) {
        hours += 12
      } else if (period === 'AM' && hours === 12) {
        hours = 0
      }

      return `${hours.toString().padStart(2, '0')}:${mins}`
    }

    // Group passengers by boarding point
    const boardingPointsMap = new Map<string, {
      name: string
      time: string | null
      time24h: string | null
      address: string | null
      passengers: any[]
    }>()

    passengers?.forEach((passenger: any) => {
      const booking = passenger.jc_bookings
      const bpName = booking?.boarding_point_name || 'Unknown Boarding Point'
      // Try to get time from booking field first, then extract from name
      let bpTime = booking?.boarding_time || extractTimeFromName(bpName)
      const bpTime24h = to24Hour(bpTime)
      const bpAddress = booking?.boarding_address || null

      if (!boardingPointsMap.has(bpName)) {
        boardingPointsMap.set(bpName, {
          name: bpName,
          time: bpTime,
          time24h: bpTime24h,
          address: bpAddress,
          passengers: [],
        })
      }

      boardingPointsMap.get(bpName)!.passengers.push({
        id: passenger.id,
        full_name: passenger.full_name,
        mobile: passenger.mobile,
        email: passenger.email,
        seat_number: passenger.seat_number,
        pnr_number: passenger.pnr_number || booking?.travel_operator_pnr,
        fare: passenger.fare,
        is_boarded: passenger.is_boarded,
        booking_id: passenger.booking_id,
        origin: booking?.origin,
        destination: booking?.destination,
        booked_by: booking?.booked_by || 'Direct',
      })
    })

    // Helper function to convert 24h time string to minutes for proper sorting
    const timeToMinutes = (timeStr: string | null): number => {
      if (!timeStr) return 9999 // Put null times at the end
      try {
        // Handle "HH:MM" format (24-hour)
        const parts = timeStr.split(':')
        const hours = parseInt(parts[0], 10)
        const mins = parseInt(parts[1], 10)
        return hours * 60 + mins
      } catch {
        return 9999
      }
    }

    // Convert map to array and sort by boarding time (earliest first)
    const boardingPoints = Array.from(boardingPointsMap.values())
      .sort((a, b) => {
        // Use the 24h converted time for proper sorting
        const timeA = timeToMinutes(a.time24h)
        const timeB = timeToMinutes(b.time24h)
        return timeA - timeB
      })

    const totalPassengers = passengers?.length || 0

    return NextResponse.json({
      tripId,
      trip,
      boardingPoints,
      totalPassengers,
    })
  } catch (error: any) {
    console.error('Error in trip passengers API:', error)
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    )
  }
}
