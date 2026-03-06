import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * Debug endpoint to check booking data in database
 * GET /api/debug/bookings?date=2025-12-18
 */
export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)
    const date = searchParams.get('date')
    const serviceNumber = searchParams.get('service')

    // Get all bookings
    let query = supabase
      .from('jc_bookings')
      .select(`
        id,
        travel_operator_pnr,
        service_number,
        travel_date,
        origin,
        destination,
        total_fare,
        booking_status,
        created_at
      `)
      .order('created_at', { ascending: false })
      .limit(100)

    if (date) {
      query = query.eq('travel_date', date)
    }

    if (serviceNumber) {
      query = query.eq('service_number', serviceNumber)
    }

    const { data: bookings, error } = await query

    if (error) {
      return NextResponse.json({ error: error.message }, { status: 500 })
    }

    // Get distinct travel_dates
    const { data: distinctDates } = await supabase
      .from('jc_bookings')
      .select('travel_date')
      .order('travel_date', { ascending: false })
      .limit(30)

    const uniqueDates = Array.from(new Set(distinctDates?.map(d => d.travel_date) || []))

    // Get distinct service_numbers
    const { data: distinctServices } = await supabase
      .from('jc_bookings')
      .select('service_number')
      .limit(100)

    const uniqueServices = Array.from(new Set(distinctServices?.map(d => d.service_number) || []))

    // Group bookings by service_number for the given date
    const byService: Record<string, number> = {}
    bookings?.forEach(b => {
      const sn = b.service_number || 'null'
      byService[sn] = (byService[sn] || 0) + 1
    })

    return NextResponse.json({
      query_params: { date, serviceNumber },
      total_bookings_found: bookings?.length || 0,
      bookings_by_service: byService,
      sample_bookings: bookings?.slice(0, 10).map(b => ({
        id: b.id,
        pnr: b.travel_operator_pnr,
        service_number: b.service_number,
        travel_date: b.travel_date,
        origin: b.origin,
        destination: b.destination,
        fare: b.total_fare,
        status: b.booking_status,
      })),
      available_dates_in_db: uniqueDates,
      available_services_in_db: uniqueServices,
    })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
