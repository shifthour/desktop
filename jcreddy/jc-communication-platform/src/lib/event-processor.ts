import { SupabaseClient } from '@supabase/supabase-js'

type Supabase = SupabaseClient

/**
 * Event Processor for Bitla Webhook Events
 * Handles data storage and triggers communication workflows
 */

// ═══════════════════════════════════════════════════════════════════════════════
// BOOKING EVENT PROCESSOR
// ═══════════════════════════════════════════════════════════════════════════════

// Bitla sends booking data as an array of passenger objects
// Each passenger object contains both booking and passenger info
interface BitlaPassengerPayload {
  age?: number
  gst?: number
  fare?: number
  name: string
  email?: string
  title?: string
  discount?: number
  drop_off?: string
  booked_by?: string
  booked_on?: string
  commission?: number
  net_amount?: number
  pnr_number: string
  boarding_at?: string
  origin_name?: string
  seat_number: string
  ticket_fare?: number
  travel_date: string
  coach_number?: string
  dropoff_time?: string
  phone_number: string
  boarding_time?: string
  offer_discount?: number
  additional_fare?: number
  destination_name?: string
  dropoff_landmark?: string
  boarding_landmark?: string
}

export async function processBookingEvent(
  supabase: Supabase,
  payload: any,
  operatorId?: string
) {
  // Handle both array format (from Bitla) and object format
  const passengers: BitlaPassengerPayload[] = Array.isArray(payload) ? payload : [payload]

  if (!passengers.length) {
    throw new Error('No passenger data in payload')
  }

  const firstPassenger = passengers[0]

  // Parse travel date
  const travelDate = parseDate(firstPassenger.travel_date)
  const boardingTime = parseTime(firstPassenger.boarding_time || '')
  const dropoffTime = parseTime(firstPassenger.dropoff_time || '')

  // Upsert booking
  const { data: booking, error: bookingError } = await supabase
    .from('jc_bookings')
    .upsert(
      {
        operator_id: operatorId,
        ticket_number: firstPassenger.pnr_number,
        travel_operator_pnr: firstPassenger.pnr_number,
        travel_date: travelDate,
        origin: firstPassenger.origin_name || '',
        destination: firstPassenger.destination_name || '',
        boarding_point_name: firstPassenger.boarding_at,
        dropoff_point_name: firstPassenger.drop_off,
        boarding_landmark: firstPassenger.boarding_landmark,
        boarding_time: boardingTime,
        dropoff_time: dropoffTime,
        total_seats: passengers.length,
        total_fare: firstPassenger.fare || firstPassenger.ticket_fare,
        base_fare: firstPassenger.fare,
        cgst: firstPassenger.gst ? firstPassenger.gst / 2 : null,
        sgst: firstPassenger.gst ? firstPassenger.gst / 2 : null,
        discount: firstPassenger.discount || firstPassenger.offer_discount || 0,
        booked_by: firstPassenger.booked_by,
        booking_source: 'webhook',
        booking_status: 'confirmed',
        raw_data: payload as any,
      },
      {
        onConflict: 'travel_operator_pnr',
      }
    )
    .select('id')
    .single()

  if (bookingError) {
    throw new Error(`Failed to upsert booking: ${bookingError.message}`)
  }

  // Upsert passengers
  for (let i = 0; i < passengers.length; i++) {
    const p = passengers[i]

    const { error: passengerError } = await supabase.from('jc_passengers').upsert(
      {
        booking_id: booking.id,
        pnr_number: p.pnr_number,
        title: p.title,
        full_name: p.name,
        age: p.age,
        mobile: p.phone_number,
        email: p.email,
        seat_number: p.seat_number,
        fare: p.fare || p.ticket_fare,
        is_primary: i === 0,
        is_confirmed: true,
        raw_data: p as any,
      },
      {
        onConflict: 'booking_id,seat_number',
      }
    )

    if (passengerError) {
      console.error(`Failed to upsert passenger: ${passengerError.message}`)
    }
  }

  // Schedule notifications for new booking
  await scheduleBookingNotifications(supabase, booking.id, operatorId)

  return booking.id
}

// ═══════════════════════════════════════════════════════════════════════════════
// CANCELLATION EVENT PROCESSOR
// ═══════════════════════════════════════════════════════════════════════════════

interface CancellationPayload {
  event_type: string
  ticket_number: string
  travel_operator_pnr: string
  seat_numbers: string[]
  cancellation_charge?: number
  refund_amount?: number
  cancelled_by?: string
  cancelled_on: string
  cancel_remark?: string
}

export async function processCancellationEvent(
  supabase: Supabase,
  payload: CancellationPayload,
  operatorId?: string
) {
  // Find the booking
  const { data: booking } = await supabase
    .from('jc_bookings')
    .select('id, total_seats, total_fare')
    .eq('travel_operator_pnr', payload.travel_operator_pnr)
    .single()

  if (!booking) {
    console.warn(`Booking not found for PNR: ${payload.travel_operator_pnr}`)
    return
  }

  // Determine cancellation type
  const cancelledSeats = payload.seat_numbers?.length || 0
  const totalSeats = booking.total_seats || 1
  const cancellationType = cancelledSeats >= totalSeats ? 'full' : 'partial'

  // Insert cancellation record
  const { data: cancellation, error: cancelError } = await supabase
    .from('jc_cancellations')
    .insert({
      booking_id: booking.id,
      travel_operator_pnr: payload.travel_operator_pnr,
      ticket_number: payload.ticket_number,
      cancellation_type: cancellationType,
      seat_numbers: payload.seat_numbers,
      original_fare: booking.total_fare,
      cancellation_charge: payload.cancellation_charge,
      refund_amount: payload.refund_amount,
      cancelled_by: payload.cancelled_by,
      cancellation_source: 'customer',
      cancellation_reason: payload.cancel_remark,
      cancelled_at: parseDateTime(payload.cancelled_on) || new Date().toISOString(),
      raw_data: payload as any,
    })
    .select('id')
    .single()

  if (cancelError) {
    throw new Error(`Failed to insert cancellation: ${cancelError.message}`)
  }

  // Update booking status
  await supabase
    .from('jc_bookings')
    .update({
      booking_status: cancellationType === 'full' ? 'cancelled' : 'partially_cancelled',
      cancellation_date: new Date().toISOString(),
      cancellation_reason: payload.cancel_remark,
      cancellation_charge: payload.cancellation_charge,
      refund_amount: payload.refund_amount,
    })
    .eq('id', booking.id)

  // Mark cancelled passengers
  if (payload.seat_numbers?.length) {
    for (const seatNumber of payload.seat_numbers) {
      await supabase
        .from('jc_passengers')
        .update({
          is_cancelled: true,
          cancelled_at: new Date().toISOString(),
          cancellation_charge: payload.cancellation_charge,
          refund_amount: payload.refund_amount,
        })
        .eq('booking_id', booking.id)
        .eq('seat_number', seatNumber)
    }
  }

  // Cancel pending notifications
  await supabase
    .from('jc_notifications')
    .update({ status: 'cancelled' })
    .eq('booking_id', booking.id)
    .eq('status', 'pending')

  // Send cancellation notification
  await scheduleCancellationNotification(supabase, booking.id, payload, operatorId)

  return cancellation.id
}

// ═══════════════════════════════════════════════════════════════════════════════
// BOARDING STATUS EVENT PROCESSOR
// ═══════════════════════════════════════════════════════════════════════════════

interface BoardingStatusPayload {
  event_type: string
  ticket_number: string
  service_number: string
  seat_number: string
  boarding_status: string
  status_updated_at: string
  latitude?: string
  longitude?: string
}

export async function processBoardingStatusEvent(
  supabase: Supabase,
  payload: BoardingStatusPayload,
  operatorId?: string
) {
  // Find booking and passenger
  const { data: booking } = await supabase
    .from('jc_bookings')
    .select('id')
    .eq('ticket_number', payload.ticket_number)
    .single()

  if (!booking) {
    console.warn(`Booking not found for ticket: ${payload.ticket_number}`)
    return
  }

  const { data: passenger } = await supabase
    .from('jc_passengers')
    .select('id')
    .eq('booking_id', booking.id)
    .eq('seat_number', payload.seat_number)
    .single()

  // Insert boarding status record
  const { error } = await supabase.from('jc_boarding_status').insert({
    booking_id: booking.id,
    passenger_id: passenger?.id,
    ticket_number: payload.ticket_number,
    service_number: payload.service_number,
    seat_number: payload.seat_number,
    boarding_status: payload.boarding_status.toLowerCase(),
    status_updated_at: parseDateTime(payload.status_updated_at) || new Date().toISOString(),
    latitude: payload.latitude ? parseFloat(payload.latitude) : null,
    longitude: payload.longitude ? parseFloat(payload.longitude) : null,
    raw_data: payload as any,
  })

  if (error) {
    throw new Error(`Failed to insert boarding status: ${error.message}`)
  }

  // Update passenger
  if (passenger) {
    const isBoarded = payload.boarding_status.toLowerCase() === 'boarded'
    await supabase
      .from('jc_passengers')
      .update({
        is_boarded: isBoarded,
        boarded_at: isBoarded ? new Date().toISOString() : null,
        boarding_location: payload.latitude
          ? { lat: parseFloat(payload.latitude), lng: parseFloat(payload.longitude || '0') }
          : null,
      })
      .eq('id', passenger.id)
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// VEHICLE ASSIGNMENT EVENT PROCESSOR
// ═══════════════════════════════════════════════════════════════════════════════

interface VehicleAssignmentPayload {
  event_type: string
  service_id: string
  service_number: string
  travel_date: string
  coach_number: string
  gps_id?: string
  gps_vendor?: string
  trip_status?: string
  crew_members?: Array<{
    name: string
    contact_number: string
    role: string
  }>
}

export async function processVehicleAssignmentEvent(
  supabase: Supabase,
  payload: VehicleAssignmentPayload,
  operatorId?: string
) {
  const travelDate = parseDate(payload.travel_date)

  // Find the trip
  const { data: trip } = await supabase
    .from('jc_trips')
    .select('id')
    .eq('service_number', payload.service_number)
    .eq('travel_date', travelDate)
    .single()

  if (!trip) {
    console.warn(`Trip not found for service ${payload.service_number} on ${travelDate}`)
    return
  }

  // Update trip with vehicle info
  const crewInfo = payload.crew_members || []
  const driver1 = crewInfo.find(c => c.role.toLowerCase().includes('driver'))
  const conductor = crewInfo.find(c => c.role.toLowerCase().includes('conductor'))

  await supabase
    .from('jc_trips')
    .update({
      captain1_name: driver1?.name,
      captain1_phone: driver1?.contact_number,
      conductor_name: conductor?.name,
      conductor_phone: conductor?.contact_number,
    })
    .eq('id', trip.id)

  // Insert vehicle assignment
  const { error } = await supabase.from('jc_vehicle_assignments').upsert(
    {
      trip_id: trip.id,
      service_id: payload.service_id,
      service_number: payload.service_number,
      travel_date: travelDate,
      coach_number: payload.coach_number,
      gps_id: payload.gps_id,
      gps_vendor: payload.gps_vendor,
      assignment_status: 'assigned',
      raw_data: payload as any,
    },
    {
      onConflict: 'trip_id',
    }
  )

  if (error) {
    throw new Error(`Failed to insert vehicle assignment: ${error.message}`)
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// SERVICE UPDATE EVENT PROCESSOR
// ═══════════════════════════════════════════════════════════════════════════════

// Bitla service-update payload contains comprehensive trip and booking data
interface BitlaServiceUpdatePayload {
  origin?: string
  destination?: string
  travels?: string  // Service name like "MYTHRI-20"
  bus_type?: string
  dep_time?: string
  duration?: string
  route_id?: string
  schedule_id?: string
  travel_date?: string
  journey_date?: string
  requestType?: string  // "BOOKING"
  seat_numbers?: string
  boarding_point_name?: string
  drop_off_name?: string
  travel_operator_pnr?: string
  passenger_details?: Array<{
    name: string
    age?: number
    email?: string
    gender?: string
    adult_fare?: number
    pnr_number: string
    seat_number: string
    phone_number: string
  }>
  boarding_point_details_dep_time?: string
  boarding_point_details_address?: string
  boarding_point_details_landmark?: string
  drop_off_point_details_time?: string
  drop_off_point_details_address?: string
  drop_off_point_details_landmark?: string
}

export async function processServiceUpdateEvent(
  supabase: Supabase,
  payload: any,
  operatorId?: string
) {
  const data = payload as BitlaServiceUpdatePayload
  const travelDate = parseDate(data.travel_date || data.journey_date || '')
  const serviceNumber = data.travels || ''

  // First, try to find or create a route
  let routeId: string | null = null
  if (data.route_id) {
    const { data: existingRoute } = await supabase
      .from('jc_routes')
      .select('id')
      .eq('bitla_route_id', parseInt(data.route_id))
      .single()

    if (existingRoute) {
      routeId = existingRoute.id
    } else {
      // Create route
      const { data: newRoute } = await supabase
        .from('jc_routes')
        .insert({
          operator_id: operatorId,
          bitla_route_id: parseInt(data.route_id),
          route_name: serviceNumber,
          origin: data.origin || '',
          destination: data.destination || '',
          coach_type: data.bus_type,
          status: 'active',
        })
        .select('id')
        .single()
      routeId = newRoute?.id || null
    }
  }

  // Create or update trip
  let tripId: string | null = null
  if (routeId && travelDate) {
    // Convert dep_time (seconds from midnight) to time string
    let departureTime: string | null = null
    if (data.dep_time) {
      const totalSeconds = parseInt(data.dep_time)
      const hours = Math.floor(totalSeconds / 3600)
      const minutes = Math.floor((totalSeconds % 3600) / 60)
      departureTime = `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:00`
    }

    const { data: existingTrip } = await supabase
      .from('jc_trips')
      .select('id')
      .eq('route_id', routeId)
      .eq('travel_date', travelDate)
      .single()

    if (existingTrip) {
      tripId = existingTrip.id
      // Update trip
      await supabase
        .from('jc_trips')
        .update({
          service_number: serviceNumber,
          bus_type: data.bus_type,
          departure_time: departureTime,
          reservation_id: data.schedule_id ? parseInt(data.schedule_id) : null,
        })
        .eq('id', tripId)
    } else {
      // Create trip
      const { data: newTrip } = await supabase
        .from('jc_trips')
        .insert({
          route_id: routeId,
          travel_date: travelDate,
          service_number: serviceNumber,
          bus_type: data.bus_type,
          departure_time: departureTime,
          reservation_id: data.schedule_id ? parseInt(data.schedule_id) : null,
          trip_status: 'scheduled',
        })
        .select('id')
        .single()
      tripId = newTrip?.id || null
    }
  }

  // If this is a booking request, also create booking and passenger records
  if (data.requestType === 'BOOKING' && data.passenger_details?.length) {
    const firstPassenger = data.passenger_details[0]

    // Convert boarding time
    let boardingTime: string | null = null
    if (data.boarding_point_details_dep_time) {
      const totalSeconds = parseInt(data.boarding_point_details_dep_time)
      const hours = Math.floor(totalSeconds / 3600)
      const minutes = Math.floor((totalSeconds % 3600) / 60)
      boardingTime = `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:00`
    }

    // Convert dropoff time
    let dropoffTime: string | null = null
    if (data.drop_off_point_details_time) {
      const totalSeconds = parseInt(data.drop_off_point_details_time)
      const hours = Math.floor(totalSeconds / 3600)
      const minutes = Math.floor((totalSeconds % 3600) / 60)
      dropoffTime = `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:00`
    }

    // Create booking
    const { data: booking } = await supabase
      .from('jc_bookings')
      .upsert({
        trip_id: tripId,
        operator_id: operatorId,
        ticket_number: data.travel_operator_pnr || firstPassenger.pnr_number,
        travel_operator_pnr: data.travel_operator_pnr || firstPassenger.pnr_number,
        service_number: serviceNumber,
        travel_date: travelDate,
        origin: data.origin,
        destination: data.destination,
        boarding_point_name: data.boarding_point_name,
        dropoff_point_name: data.drop_off_name,
        boarding_address: data.boarding_point_details_address,
        boarding_landmark: data.boarding_point_details_landmark,
        boarding_time: boardingTime,
        dropoff_time: dropoffTime,
        total_seats: data.passenger_details.length,
        total_fare: firstPassenger.adult_fare,
        booking_source: 'webhook',
        booking_status: 'confirmed',
        raw_data: payload as any,
      }, {
        onConflict: 'travel_operator_pnr',
      })
      .select('id')
      .single()

    // Create passengers
    if (booking) {
      for (let i = 0; i < data.passenger_details.length; i++) {
        const p = data.passenger_details[i]
        await supabase.from('jc_passengers').upsert({
          booking_id: booking.id,
          trip_id: tripId,
          pnr_number: p.pnr_number,
          title: p.gender,
          full_name: p.name,
          age: p.age,
          mobile: p.phone_number,
          email: p.email,
          seat_number: p.seat_number,
          fare: p.adult_fare,
          is_primary: i === 0,
          is_confirmed: true,
          raw_data: p as any,
        }, {
          onConflict: 'booking_id,seat_number',
        })
      }

      // Schedule notifications
      await scheduleBookingNotifications(supabase, booking.id, operatorId)
    }
  }

  return tripId
}

// ═══════════════════════════════════════════════════════════════════════════════
// NOTIFICATION SCHEDULING
// ═══════════════════════════════════════════════════════════════════════════════

async function scheduleBookingNotifications(
  supabase: Supabase,
  bookingId: string,
  operatorId?: string
) {
  // Get booking with passengers
  const { data: booking } = await supabase
    .from('jc_bookings')
    .select(
      `
      *,
      jc_passengers (*)
    `
    )
    .eq('id', bookingId)
    .single()

  if (!booking || !booking.jc_passengers?.length) return

  const passengers = booking.jc_passengers
  const boardingDatetime = booking.boarding_datetime
    ? new Date(booking.boarding_datetime)
    : null

  for (const passenger of passengers) {
    // 1. Booking Confirmation (immediate)
    await supabase.from('jc_notifications').insert({
      passenger_id: passenger.id,
      booking_id: bookingId,
      trip_id: booking.trip_id,
      operator_id: operatorId,
      notification_type: 'booking_confirmation',
      recipient_mobile: passenger.mobile,
      recipient_email: passenger.email,
      recipient_name: passenger.full_name,
      channel: 'whatsapp',
      priority: 1,
      scheduled_at: new Date().toISOString(),
      status: 'pending',
      message_variables: {
        passenger_name: passenger.full_name,
        origin: booking.origin,
        destination: booking.destination,
        travel_date: booking.travel_date,
        departure_time: booking.boarding_time,
        seat_number: passenger.seat_number,
        pnr_number: passenger.pnr_number,
        boarding_point: booking.boarding_point_name,
        boarding_address: booking.boarding_address,
        google_maps_link:
          booking.boarding_latitude && booking.boarding_longitude
            ? `https://www.google.com/maps?q=${booking.boarding_latitude},${booking.boarding_longitude}`
            : null,
        helpline_number: '7095666619',
      },
    })

    // 2. 24-hour reminder (if applicable)
    if (passenger.pre_boarding_applicable && boardingDatetime) {
      const reminder24h = new Date(boardingDatetime.getTime() - 24 * 60 * 60 * 1000)
      if (reminder24h > new Date()) {
        await supabase.from('jc_notifications').insert({
          passenger_id: passenger.id,
          booking_id: bookingId,
          trip_id: booking.trip_id,
          operator_id: operatorId,
          notification_type: 'reminder_24h',
          recipient_mobile: passenger.mobile,
          recipient_name: passenger.full_name,
          channel: 'whatsapp',
          priority: 2,
          scheduled_at: reminder24h.toISOString(),
          status: 'pending',
        })
      }
    }

    // 3. 2-hour reminder (if applicable)
    if (passenger.welcome_call_applicable && boardingDatetime) {
      const reminder2h = new Date(boardingDatetime.getTime() - 2 * 60 * 60 * 1000)
      if (reminder2h > new Date()) {
        await supabase.from('jc_notifications').insert({
          passenger_id: passenger.id,
          booking_id: bookingId,
          trip_id: booking.trip_id,
          operator_id: operatorId,
          notification_type: 'reminder_2h',
          recipient_mobile: passenger.mobile,
          recipient_name: passenger.full_name,
          channel: 'whatsapp',
          priority: 1,
          scheduled_at: reminder2h.toISOString(),
          status: 'pending',
        })
      }
    }

    // 4. Wake-up call (if applicable)
    if (passenger.wake_up_call_applicable && booking.dropoff_datetime) {
      const arrivalTime = new Date(booking.dropoff_datetime)
      const wakeupTime = new Date(arrivalTime.getTime() - 30 * 60 * 1000) // 30 min before arrival
      if (wakeupTime > new Date()) {
        await supabase.from('jc_notifications').insert({
          passenger_id: passenger.id,
          booking_id: bookingId,
          trip_id: booking.trip_id,
          operator_id: operatorId,
          notification_type: 'wakeup_call',
          recipient_mobile: passenger.mobile,
          recipient_name: passenger.full_name,
          channel: 'whatsapp',
          priority: 1,
          scheduled_at: wakeupTime.toISOString(),
          status: 'pending',
        })
      }
    }
  }
}

async function scheduleCancellationNotification(
  supabase: Supabase,
  bookingId: string,
  payload: CancellationPayload,
  operatorId?: string
) {
  // Get booking with passengers
  const { data: booking } = await supabase
    .from('jc_bookings')
    .select(
      `
      *,
      jc_passengers (*)
    `
    )
    .eq('id', bookingId)
    .single()

  if (!booking || !booking.jc_passengers?.length) return

  const primaryPassenger = booking.jc_passengers.find((p: any) => p.is_primary) || booking.jc_passengers[0]

  await supabase.from('jc_notifications').insert({
    passenger_id: primaryPassenger.id,
    booking_id: bookingId,
    trip_id: booking.trip_id,
    operator_id: operatorId,
    notification_type: 'cancellation_ack',
    recipient_mobile: primaryPassenger.mobile,
    recipient_email: primaryPassenger.email,
    recipient_name: primaryPassenger.full_name,
    channel: 'whatsapp',
    priority: 1,
    scheduled_at: new Date().toISOString(),
    status: 'pending',
    message_variables: {
      passenger_name: primaryPassenger.full_name,
      pnr_number: booking.travel_operator_pnr,
      origin: booking.origin,
      destination: booking.destination,
      travel_date: booking.travel_date,
      cancellation_charge: payload.cancellation_charge,
      refund_amount: payload.refund_amount,
      helpline_number: '7095666619',
    },
  })
}

async function scheduleDelayNotifications(
  supabase: Supabase,
  tripId: string,
  payload: any,
  operatorId?: string
) {
  // Get all passengers for this trip
  const { data: passengers } = await supabase
    .from('jc_passengers')
    .select('*, jc_bookings!inner(*)')
    .eq('trip_id', tripId)
    .eq('is_cancelled', false)

  if (!passengers?.length) return

  for (const passenger of passengers) {
    await supabase.from('jc_notifications').insert({
      passenger_id: passenger.id,
      booking_id: passenger.booking_id,
      trip_id: tripId,
      operator_id: operatorId,
      notification_type: 'delay_alert',
      recipient_mobile: passenger.mobile,
      recipient_name: passenger.full_name,
      channel: 'whatsapp',
      priority: 1,
      scheduled_at: new Date().toISOString(),
      status: 'pending',
      message_variables: {
        passenger_name: passenger.full_name,
        origin: (passenger as any).jc_bookings?.origin,
        destination: (passenger as any).jc_bookings?.destination,
        original_time: payload.original_departure,
        new_time: payload.new_departure,
        delay_minutes: payload.delay_minutes,
        reason: payload.reason,
        helpline_number: '7095666619',
      },
    })
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// HELPER FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════

function parseDate(dateStr: string): string {
  if (!dateStr) return new Date().toISOString().split('T')[0]

  // Handle various date formats
  // DD-MM-YYYY or DD/MM/YYYY
  const dmyMatch = dateStr.match(/^(\d{2})[-\/](\d{2})[-\/](\d{4})$/)
  if (dmyMatch) {
    return `${dmyMatch[3]}-${dmyMatch[2]}-${dmyMatch[1]}`
  }

  // YYYY-MM-DD (ISO)
  const isoMatch = dateStr.match(/^(\d{4})-(\d{2})-(\d{2})/)
  if (isoMatch) {
    return `${isoMatch[1]}-${isoMatch[2]}-${isoMatch[3]}`
  }

  return dateStr
}

function parseTime(timeStr: string): string | null {
  if (!timeStr) return null

  // Handle "HH:MM" format
  const timeMatch = timeStr.match(/^(\d{1,2}):(\d{2})/)
  if (timeMatch) {
    return `${timeMatch[1].padStart(2, '0')}:${timeMatch[2]}:00`
  }

  // Handle "HH:MM PM/AM" format
  const time12Match = timeStr.match(/^(\d{1,2}):(\d{2})\s*(AM|PM)/i)
  if (time12Match) {
    let hours = parseInt(time12Match[1])
    const minutes = time12Match[2]
    const period = time12Match[3].toUpperCase()

    if (period === 'PM' && hours !== 12) hours += 12
    if (period === 'AM' && hours === 12) hours = 0

    return `${hours.toString().padStart(2, '0')}:${minutes}:00`
  }

  return null
}

function parseDateTime(dateTimeStr: string): string | null {
  if (!dateTimeStr) return null

  // Handle "DD-MM-YYYY HH:MM PM" format
  const match = dateTimeStr.match(
    /^(\d{2})[-\/](\d{2})[-\/](\d{4})\s+(\d{1,2}):(\d{2})\s*(AM|PM)/i
  )
  if (match) {
    const day = match[1]
    const month = match[2]
    const year = match[3]
    let hours = parseInt(match[4])
    const minutes = match[5]
    const period = match[6].toUpperCase()

    if (period === 'PM' && hours !== 12) hours += 12
    if (period === 'AM' && hours === 12) hours = 0

    return `${year}-${month}-${day}T${hours.toString().padStart(2, '0')}:${minutes}:00.000Z`
  }

  // Try parsing as ISO
  try {
    const date = new Date(dateTimeStr)
    if (!isNaN(date.getTime())) {
      return date.toISOString()
    }
  } catch {
    // Ignore parse errors
  }

  return null
}
