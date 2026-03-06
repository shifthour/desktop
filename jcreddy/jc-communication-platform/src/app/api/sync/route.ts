import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'
import { createBitlaClient } from '@/lib/bitla-client'

// Route segment config for Vercel - allow 60 second timeout
export const maxDuration = 60

/**
 * API endpoint to trigger data sync from Bitla
 */
export async function POST(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const bitla = createBitlaClient()

    // Create sync job record
    const { data: syncJob } = await (supabase
      .from('jc_sync_jobs') as any)
      .insert({
        sync_type: 'full',
        status: 'running',
        started_at: new Date().toISOString(),
      })
      .select('id')
      .single()

    let recordsFetched = 0
    let recordsCreated = 0
    let recordsUpdated = 0
    let recordsFailed = 0

    try {
      // Fetch all data from Bitla
      const { routes, trips } = await bitla.syncAllData()

      recordsFetched = trips.length

      // Get operator
      const { data: operator } = await supabase
        .from('jc_operators')
        .select('id')
        .eq('operator_code', 'mythri')
        .single() as { data: { id: string } | null }

      // Sync routes
      for (const route of routes) {
        const { error } = await (supabase.from('jc_routes') as any).upsert(
          {
            operator_id: operator?.id,
            bitla_route_id: route.id,
            route_name: route.route_num,
            origin: 'TBD',
            destination: 'TBD',
            status: route.route_status.toLowerCase(),
            last_synced_at: new Date().toISOString(),
          },
          {
            onConflict: 'operator_id,bitla_route_id',
          }
        )

        if (error) {
          recordsFailed++
        } else {
          recordsUpdated++
        }
      }

      // Sync trips and passengers
      for (const tripData of trips) {
        try {
          // Find route
          const { data: route } = await (supabase
            .from('jc_routes') as any)
            .select('id')
            .eq('bitla_route_id', tripData.route_id)
            .single()

          if (!route) continue

          // Parse dates
          const travelDate = tripData.travel_date.split('-').reverse().join('-')

          // Upsert trip
          const { data: trip, error: tripError } = await (supabase
            .from('jc_trips') as any)
            .upsert(
              {
                route_id: route.id,
                travel_date: travelDate,
                reservation_id: tripData.reservation_id,
                service_number: tripData.route_num,
                bus_type: tripData.bus_type,
                departure_time: tripData.route_departure_time,
                total_seats: tripData.available_seats + (tripData.passenger_details?.length || 0),
                available_seats: tripData.available_seats,
                booked_seats: tripData.passenger_details?.length || 0,
                blocked_seats: tripData.blocked_count,
                captain1_name: extractName(tripData.captain1_details),
                captain1_phone: extractPhone(tripData.captain1_details),
                captain2_name: extractName(tripData.captain2_details),
                captain2_phone: extractPhone(tripData.captain2_details),
                helpline_number: tripData.customer_helpline_number,
                trip_status: 'scheduled',
                raw_data: tripData as any,
                last_synced_at: new Date().toISOString(),
              },
              {
                onConflict: 'route_id,travel_date,reservation_id',
              }
            )
            .select('id')
            .single()

          if (tripError) {
            recordsFailed++
            continue
          }

          // Update route with origin/destination
          await (supabase
            .from('jc_routes') as any)
            .update({
              origin: tripData.origin,
              destination: tripData.destination,
            })
            .eq('id', route.id)

          // Sync passengers
          for (const p of tripData.passenger_details || []) {
            try {
              // Create or get booking
              const boardingTime = parseBoardingTime(p.bording_date_time)

              const { data: booking } = await (supabase
                .from('jc_bookings') as any)
                .upsert(
                  {
                    trip_id: trip?.id,
                    operator_id: operator?.id,
                    ticket_number: p.pnr_number,
                    travel_operator_pnr: `${tripData.reservation_id}-${p.pnr_number}`,
                    service_number: tripData.route_num,
                    travel_date: travelDate,
                    origin: tripData.origin,
                    destination: tripData.destination,
                    boarding_point_name: p.boarding_at?.split(' - ')[0],
                    boarding_address: p.boarding_address,
                    boarding_landmark: p.boarding_landmark,
                    boarding_datetime: boardingTime,
                    boarding_latitude: p.bp_stage_latitude
                      ? parseFloat(p.bp_stage_latitude)
                      : null,
                    boarding_longitude: p.bp_stage_longitude
                      ? parseFloat(p.bp_stage_longitude)
                      : null,
                    dropoff_point_name: p.drop_off?.split(' - ')[0],
                    dropoff_latitude: p.dp_stage_latitude
                      ? parseFloat(p.dp_stage_latitude)
                      : null,
                    dropoff_longitude: p.dp_stage_longitude
                      ? parseFloat(p.dp_stage_longitude)
                      : null,
                    booked_by: p.booked_by,
                    booking_source: 'api_sync',
                    booking_status: p.is_ticket_confirm ? 'confirmed' : 'pending',
                  },
                  {
                    onConflict: 'travel_operator_pnr',
                  }
                )
                .select('id')
                .single()

              if (booking) {
                // Upsert passenger
                await (supabase.from('jc_passengers') as any).upsert(
                  {
                    booking_id: booking.id,
                    trip_id: trip?.id,
                    pnr_number: p.pnr_number,
                    title: p.title,
                    full_name: p.name,
                    age: p.age,
                    mobile: p.mobile,
                    email: p.email,
                    seat_number: p.seat_number,
                    is_primary: true,
                    is_confirmed: p.is_ticket_confirm,
                    is_boarded: p.is_boarded === 1,
                    wake_up_call_applicable: p.wake_up_call_applicable,
                    pre_boarding_applicable: p.pre_boarding_applicable,
                    welcome_call_applicable: p.welcome_call_applicable,
                    is_sms_allowed: p.is_trackingo_sms_allowed,
                    raw_data: p as any,
                  },
                  {
                    onConflict: 'booking_id,seat_number',
                  }
                )

                recordsCreated++
              }
            } catch (passengerError) {
              console.error('Error syncing passenger:', passengerError)
              recordsFailed++
            }
          }
        } catch (tripError) {
          console.error('Error syncing trip:', tripError)
          recordsFailed++
        }
      }

      // Update sync job
      if (syncJob?.id) {
        await (supabase
          .from('jc_sync_jobs') as any)
          .update({
            status: 'completed',
            completed_at: new Date().toISOString(),
            records_fetched: recordsFetched,
            records_created: recordsCreated,
            records_updated: recordsUpdated,
            records_failed: recordsFailed,
          })
          .eq('id', syncJob.id)
      }

      return NextResponse.json({
        status: 'success',
        sync_job_id: syncJob?.id,
        records_fetched: recordsFetched,
        records_created: recordsCreated,
        records_updated: recordsUpdated,
        records_failed: recordsFailed,
      })
    } catch (error: any) {
      // Update sync job with error
      if (syncJob?.id) {
        await (supabase
          .from('jc_sync_jobs') as any)
          .update({
            status: 'failed',
            completed_at: new Date().toISOString(),
            error_message: error.message,
          })
          .eq('id', syncJob.id)
      }

      throw error
    }
  } catch (error: any) {
    console.error('Sync error:', error)
    return NextResponse.json(
      { status: 'error', message: error.message },
      { status: 500 }
    )
  }
}

export async function GET() {
  const supabase = createServerClient()

  // Get latest sync jobs
  const { data: syncJobs } = await (supabase
    .from('jc_sync_jobs') as any)
    .select('*')
    .order('created_at', { ascending: false })
    .limit(10)

  return NextResponse.json({
    status: 'ok',
    recent_syncs: syncJobs,
  })
}

// Helper functions
function extractName(details: string): string | null {
  if (!details) return null
  const match = details.match(/^([^(]+)/)
  return match ? match[1].trim() : null
}

function extractPhone(details: string): string | null {
  if (!details) return null
  const match = details.match(/(\d{10})/)
  return match ? match[1] : null
}

function parseBoardingTime(dateTimeStr: string): string | null {
  if (!dateTimeStr) return null

  // Handle "2025-12-03 07:45 PM" format
  const match = dateTimeStr.match(
    /^(\d{4})-(\d{2})-(\d{2})\s+(\d{1,2}):(\d{2})\s*(AM|PM)/i
  )
  if (match) {
    const year = match[1]
    const month = match[2]
    const day = match[3]
    let hours = parseInt(match[4])
    const minutes = match[5]
    const period = match[6].toUpperCase()

    if (period === 'PM' && hours !== 12) hours += 12
    if (period === 'AM' && hours === 12) hours = 0

    return `${year}-${month}-${day}T${hours.toString().padStart(2, '0')}:${minutes}:00.000Z`
  }

  return null
}
