import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * Debug endpoint to check trip data in database
 * GET /api/debug/trips?date=2025-12-18
 */
export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)
    const date = searchParams.get('date')

    if (!date) {
      return NextResponse.json({
        error: 'Please provide date parameter (YYYY-MM-DD format)',
        example: '/api/debug/trips?date=2025-12-18'
      }, { status: 400 })
    }

    // Get all trips for this date from jc_trips
    const { data: trips, error: tripsError } = await supabase
      .from('jc_trips')
      .select(`
        id,
        route_id,
        travel_date,
        service_number,
        reservation_id,
        departure_time,
        arrival_time,
        total_seats,
        available_seats,
        blocked_seats,
        booked_seats,
        phone_booking_count,
        blocked_seat_numbers,
        origin,
        destination,
        trip_status,
        last_synced_at,
        created_at
      `)
      .eq('travel_date', date)
      .order('service_number')

    if (tripsError) {
      return NextResponse.json({ error: tripsError.message }, { status: 500 })
    }

    // Get all bookings for this date
    const { data: bookings, error: bookingsError } = await supabase
      .from('jc_bookings')
      .select('id, service_number, travel_date, total_fare, booking_status, travel_operator_pnr')
      .eq('travel_date', date)
      .order('service_number')

    // Get distinct travel_dates in the database (last 30 days)
    const { data: distinctDates } = await supabase
      .from('jc_trips')
      .select('travel_date')
      .order('travel_date', { ascending: false })
      .limit(30)

    const uniqueDates = Array.from(new Set(distinctDates?.map(d => d.travel_date) || []))

    return NextResponse.json({
      query_date: date,
      trips_found: trips?.length || 0,
      bookings_found: bookings?.length || 0,
      trips: trips?.map(t => ({
        service_number: t.service_number,
        travel_date: t.travel_date,
        origin: t.origin,
        destination: t.destination,
        total_seats: t.total_seats,
        available_seats: t.available_seats,
        booked_seats: t.booked_seats,
        blocked_seats: t.blocked_seats,
        phone_booking_count: t.phone_booking_count,
        departure_time: t.departure_time,
        last_synced_at: t.last_synced_at,
      })),
      bookings_by_service: bookings?.reduce((acc: any, b) => {
        if (!acc[b.service_number]) acc[b.service_number] = []
        acc[b.service_number].push(b.travel_operator_pnr)
        return acc
      }, {}),
      available_dates_in_db: uniqueDates,
    })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
