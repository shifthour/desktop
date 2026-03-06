import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'
import { sendBookingConfirmation, formatTime } from '@/lib/aisensy'

/**
 * Unified Webhook endpoint for ALL Bitla events
 * Handles: BOOKING, RELEASE, CANCELLATION, BOARDING, SERVICE_UPDATE, VEHICLE_ASSIGN, etc.
 *
 * Bitla sends events with requestType field to identify the event type
 */
export async function POST(request: NextRequest) {
  const startTime = Date.now()

  try {
    // Get headers
    const operatorId = request.headers.get('x-operator-id') || 'mythri'
    const webhookSecret = request.headers.get('x-webhook-secret')
    const timestamp = request.headers.get('x-timestamp')
    const sourceIp = request.headers.get('x-forwarded-for') || request.headers.get('x-real-ip') || 'unknown'
    const contentType = request.headers.get('content-type') || ''

    // Parse payload
    let payload: any
    try {
      const rawBody = await request.text()

      // Try to parse as JSON
      if (contentType.includes('application/json') || rawBody.startsWith('{') || rawBody.startsWith('[')) {
        payload = JSON.parse(rawBody)
      } else if (contentType.includes('application/x-www-form-urlencoded') || rawBody.includes('=')) {
        // Parse form-urlencoded data (e.g., requestType=BOARDING&ticket_number=123)
        const params = new URLSearchParams(rawBody)
        payload = {}
        params.forEach((value, key) => {
          // Decode URL-encoded values (+ becomes space, %XX becomes characters)
          payload[key] = decodeURIComponent(value.replace(/\+/g, ' '))
        })
      } else {
        // Unknown format - store as raw
        payload = { raw_body: rawBody }
      }
    } catch (parseError) {
      console.error('Failed to parse webhook payload:', parseError)
      payload = { parse_error: true }
    }

    // Determine event type from payload
    const requestType = payload.requestType || payload.request_type || payload.event_type || payload.type || payload.status || 'UNKNOWN'
    const eventType = requestType.toLowerCase().replace(/_/g, '-')

    // Create Supabase client
    const supabase = createServerClient()

    // Get operator
    const { data: operator } = await supabase
      .from('jc_operators')
      .select('id')
      .eq('operator_code', operatorId)
      .single()

    // Store raw webhook event for audit
    const { data: webhookEvent, error: insertError } = await supabase
      .from('jc_webhook_events')
      .insert({
        operator_id: operator?.id || null,
        event_type: eventType,
        endpoint: '/api/webhooks/bitla/events',
        method: 'POST',
        headers: {
          'x-operator-id': operatorId,
          'x-timestamp': timestamp,
          'content-type': contentType,
        },
        payload: payload,
        source_ip: sourceIp,
        signature: webhookSecret,
        signature_valid: true,
        processed: false,
      })
      .select('id')
      .single()

    if (insertError) {
      console.error('Failed to store webhook event:', insertError)
    }

    // Process the event based on type
    let processingResult: any = { processed: false }

    try {
      switch (requestType.toUpperCase()) {
        case 'BOOKING':
        case 'NEW_BOOKING':
        case 'TICKET_BOOKING':
        case 'CONFIRMED':
        case 'CONFIRM':
        case 'BOOKED':
        case 'NEW':
        case 'TICKET':
        case 'RESERVATION':
        case 'PENDING':
          processingResult = await processBookingEvent(supabase, payload, operator?.id)
          break

        case 'RELEASE':
        case 'TICKET_RELEASE':
        case 'RELEASED':
          processingResult = await processReleaseEvent(supabase, payload, operator?.id)
          break

        case 'CANCELLATION':
        case 'CANCEL':
        case 'CANCELLED':
        case 'TICKET_CANCEL':
          processingResult = await processCancellationEvent(supabase, payload, operator?.id)
          break

        case 'BOARDING':
        case 'BOARDED':
        case 'BOARDING_STATUS':
          processingResult = await processBoardingEvent(supabase, payload, operator?.id)
          break

        case 'SERVICE':
        case 'SERVICE_UPDATE':
        case 'TRIP_UPDATE':
          processingResult = await processServiceUpdateEvent(supabase, payload, operator?.id)
          break

        case 'VEHICLE_ASSIGN':
        case 'BUS_ASSIGN':
          processingResult = await processVehicleAssignEvent(supabase, payload, operator?.id)
          break

        case 'UPDATE':
        case 'BOOKING_UPDATE':
        case 'TICKET_UPDATE':
        case 'MODIFY':
          processingResult = await processUpdateEvent(supabase, payload, operator?.id)
          break

        default:
          // For unknown event types, try to process as booking if it has booking-related fields
          if (payload.travel_operator_pnr || payload.passenger_details || payload.ticket_number || payload.pnr_number) {
            console.log(`Processing unknown event type '${requestType}' as booking due to booking fields present`)
            processingResult = await processBookingEvent(supabase, payload, operator?.id)
          } else {
            // Store as generic event - don't fail, just log
            console.log(`Received unknown event type: ${requestType}`, payload)
            processingResult = { processed: true, event_type: 'unknown', stored: true }
          }
      }

      // Mark as processed
      if (webhookEvent?.id) {
        await supabase
          .from('jc_webhook_events')
          .update({
            processed: true,
            processed_at: new Date().toISOString(),
          })
          .eq('id', webhookEvent.id)
      }
    } catch (processError: any) {
      // Mark as failed but still return success to Bitla
      if (webhookEvent?.id) {
        await supabase
          .from('jc_webhook_events')
          .update({
            processing_error: processError.message,
          })
          .eq('id', webhookEvent.id)
      }
      console.error(`Error processing ${requestType} event:`, processError)
      processingResult = { error: processError.message }
    }

    const latency = Date.now() - startTime

    // Always return success to Bitla so they know we received it
    return NextResponse.json(
      {
        status: 'accepted',
        code: 200,
        message: 'Webhook received successfully',
        event_id: webhookEvent?.id,
        event_type: eventType,
        request_type: requestType,
        latency_ms: latency,
        processing: processingResult,
      },
      { status: 200 }
    )
  } catch (error: any) {
    console.error('Webhook error:', error)
    // Even on error, try to return a proper response
    return NextResponse.json(
      {
        status: 'error',
        code: 500,
        message: error.message || 'Internal server error',
      },
      { status: 500 }
    )
  }
}

// Health check
export async function GET() {
  return NextResponse.json({
    status: 'ok',
    endpoint: 'bitla-events',
    description: 'Unified webhook endpoint for all Bitla events',
    supported_types: [
      'BOOKING', 'NEW_BOOKING', 'TICKET_BOOKING',
      'UPDATE', 'BOOKING_UPDATE', 'TICKET_UPDATE', 'MODIFY',
      'RELEASE', 'TICKET_RELEASE', 'RELEASED',
      'CANCELLATION', 'CANCEL', 'CANCELLED', 'TICKET_CANCEL',
      'BOARDING', 'BOARDED', 'BOARDING_STATUS',
      'SERVICE', 'SERVICE_UPDATE', 'TRIP_UPDATE',
      'VEHICLE_ASSIGN', 'BUS_ASSIGN',
    ],
    usage: {
      method: 'POST',
      content_type: 'application/json',
      body: {
        requestType: 'BOOKING | RELEASE | CANCELLATION | BOARDING | etc.',
        '...': 'other event-specific fields',
      },
    },
  })
}

// ============ Event Processing Functions ============

// Helper function to ensure a trip record exists for webhook bookings
async function ensureTripExists(supabase: any, payload: any, operatorId: string | null): Promise<string | null> {
  const travelDate = parseDate(payload.travel_date || payload.journey_date)
  const serviceNumber = payload.route_id || payload.service_number || null
  const bitlaRouteId = payload.route_id ? parseInt(payload.route_id) : null

  if (!travelDate || !serviceNumber) {
    console.log('[Trip Ensure] Missing travel_date or service_number, skipping trip creation')
    return null
  }

  // First, try to find the route in jc_routes
  let routeUUID: string | null = null

  if (bitlaRouteId) {
    const { data: route } = await supabase
      .from('jc_routes')
      .select('id')
      .eq('bitla_route_id', bitlaRouteId)
      .single()

    if (route) {
      routeUUID = route.id
    }
  }

  // Check if trip already exists for this date + service_number
  let existingTrip = null

  // Try by route_id first
  if (routeUUID) {
    const { data: tripByRoute } = await supabase
      .from('jc_trips')
      .select('id')
      .eq('travel_date', travelDate)
      .eq('route_id', routeUUID)
      .single()
    existingTrip = tripByRoute
  }

  // Try by service_number
  if (!existingTrip) {
    const { data: tripByService } = await supabase
      .from('jc_trips')
      .select('id')
      .eq('travel_date', travelDate)
      .eq('service_number', String(serviceNumber))
      .single()
    existingTrip = tripByService
  }

  if (existingTrip) {
    console.log(`[Trip Ensure] Found existing trip ${existingTrip.id} for date=${travelDate}, service=${serviceNumber}`)
    return existingTrip.id
  }

  // Create new trip record
  const tripData: any = {
    travel_date: travelDate,
    service_number: String(serviceNumber),
    origin: payload.origin || null,
    destination: payload.destination || null,
    departure_time: payload.route_departure_time || payload.departure_time || null,
    arrival_time: payload.route_arrival_time || payload.arrival_time || null,
    bus_type: payload.bus_type || 'Sleeper A/C',
    coach_num: payload.coach_num || payload.bus_number || null,
    trip_status: 'scheduled',
    // Set initial seat counts (will be updated as bookings come in)
    total_seats: 40, // Default, will be updated by pull-data
    available_seats: 40,
    booked_seats: 0,
    blocked_seats: 0,
    last_synced_at: new Date().toISOString(),
  }

  // Add route_id if we found it
  if (routeUUID) {
    tripData.route_id = routeUUID
  }

  // Parse captain details if available
  if (payload.captain1_details) {
    const parts = payload.captain1_details.split(' - ')
    tripData.captain1_name = parts[0]?.trim() || null
    tripData.captain1_phone = parts[1]?.trim() || null
  }
  if (payload.captain2_details) {
    const parts = payload.captain2_details.split(' - ')
    tripData.captain2_name = parts[0]?.trim() || null
    tripData.captain2_phone = parts[1]?.trim() || null
  }

  console.log(`[Trip Ensure] Creating new trip for date=${travelDate}, service=${serviceNumber}`)

  const { data: newTrip, error: insertError } = await supabase
    .from('jc_trips')
    .insert(tripData)
    .select('id')
    .single()

  if (insertError) {
    console.error('[Trip Ensure] Failed to create trip:', insertError)
    return null
  }

  console.log(`[Trip Ensure] Created new trip ${newTrip.id}`)
  return newTrip.id
}

async function processBookingEvent(supabase: any, payload: any, operatorId: string | null) {
  // Bitla sends passenger_details as an ARRAY, and travel_operator_pnr at top level
  const passengerDetailsArray = payload.passenger_details || payload.passengerDetails || []
  const firstPassenger = Array.isArray(passengerDetailsArray) ? passengerDetailsArray[0] || {} : passengerDetailsArray

  // Get PNR - prefer travel_operator_pnr at top level, then from passenger_details array
  const ticketNumber = payload.travel_operator_pnr ||
                       payload.travelOperatorPnr ||
                       firstPassenger.pnr_number ||
                       firstPassenger.pnrNumber ||
                       payload.pnr_number ||
                       payload.pnrNumber ||
                       payload.ticket_number ||
                       payload.pnr ||
                       null

  if (!ticketNumber) {
    console.log('No ticket/PNR number found in payload:', JSON.stringify(payload))
    return { processed: false, reason: 'No ticket number provided', payload_keys: Object.keys(payload) }
  }

  // Check if booking exists
  const { data: existingBooking } = await supabase
    .from('jc_bookings')
    .select('id')
    .eq('travel_operator_pnr', ticketNumber)
    .single()

  // Extract route_id - Bitla sends this as 'route_id', store in service_number column
  const serviceNumber = payload.route_id ||
                        payload.routeId ||
                        payload.service_number ||
                        payload.service ||
                        payload.serviceNumber ||
                        payload.route ||
                        null

  // Determine booking status from requestType or payload
  const requestType = (payload.requestType || payload.request_type || '').toUpperCase()
  let bookingStatus = 'confirmed'
  if (requestType === 'PENDING') {
    bookingStatus = 'pending'
  } else if (payload.ticket_status) {
    // Use ticket_status from payload if available (e.g., "Booked", "Pending")
    const ticketStatus = payload.ticket_status.toLowerCase()
    if (ticketStatus === 'pending') {
      bookingStatus = 'pending'
    } else if (ticketStatus === 'booked' || ticketStatus === 'confirmed') {
      bookingStatus = 'confirmed'
    }
  }

  // Parse boarding time - handle seconds since midnight format
  let boardingTime = null
  if (payload.boarding_point_details_dep_time) {
    boardingTime = parseSecondsToTime(payload.boarding_point_details_dep_time)
  } else if (payload.dep_time) {
    boardingTime = parseSecondsToTime(payload.dep_time)
  } else if (payload.boarding_time) {
    // If already in HH:MM format, use as-is; otherwise try to parse as seconds
    if (payload.boarding_time.includes(':')) {
      boardingTime = payload.boarding_time
    } else {
      boardingTime = parseSecondsToTime(payload.boarding_time)
    }
  }

  // Booking data - includes all columns in jc_bookings table
  // Must match columns used in pull-data route for consistency
  const bookingData: any = {
    operator_id: operatorId,
    travel_operator_pnr: ticketNumber,
    ticket_number: ticketNumber,
    service_number: serviceNumber,
    origin: payload.origin || null,
    destination: payload.destination || null,
    travel_date: parseDate(payload.travel_date || payload.journey_date),
    total_fare: firstPassenger.adult_fare || firstPassenger.net_amount || payload.amount || payload.fare || payload.net_amount || payload.total_fare || 0,
    booking_status: bookingStatus,
    booking_source: 'webhook',
    raw_data: payload,
    // Route/trip columns (same as pull-data)
    bitla_route_id: payload.route_id ? parseInt(payload.route_id) : null,
    bitla_reservation_id: payload.reservation_id ? parseInt(payload.reservation_id) : null,
    coach_num: payload.coach_num || payload.bus_number || null,
    captain1_details: payload.captain1_details || null,
    captain2_details: payload.captain2_details || null,
    route_duration: payload.route_duration || null,
    departure_time: payload.route_departure_time || payload.departure_time || null,
    arrival_time: payload.route_arrival_time || payload.arrival_time || null,
    // Booking details columns (same as pull-data)
    booked_by: payload.issued_by || payload.booking_created_by || payload.booked_by || payload.bookedBy || payload.agent || null,
    boarding_point_name: payload.boarding_point_name || payload.boarding_point || firstPassenger.boarding_at || null,
    boarding_address: payload.boarding_point_details_address || payload.boarding_address || firstPassenger.boarding_address || null,
    boarding_time: boardingTime || firstPassenger.boarding_time || null,
    boarding_landmark: payload.boarding_point_details_landmark || payload.boarding_landmark || firstPassenger.boarding_landmark || null,
  }

  // Ensure trip exists for this booking (so it shows in Trips & Operations page)
  const tripId = await ensureTripExists(supabase, payload, operatorId)
  if (tripId) {
    bookingData.trip_id = tripId
  }

  console.log('Processing booking with PNR:', ticketNumber, 'trip_id:', tripId, 'Data:', JSON.stringify(bookingData))

  if (existingBooking) {
    const { error: updateError } = await supabase
      .from('jc_bookings')
      .update(bookingData)
      .eq('id', existingBooking.id)

    if (updateError) {
      console.error('Error updating booking:', updateError)
      return { processed: false, reason: 'Update failed', error: updateError.message, details: updateError }
    }
    return { processed: true, action: 'updated', booking_id: existingBooking.id }
  } else {
    console.log('Inserting new booking...')
    const { data: newBooking, error: insertError } = await supabase
      .from('jc_bookings')
      .insert(bookingData)
      .select('id')
      .single()

    if (insertError) {
      console.error('Error inserting booking:', insertError, 'Data was:', JSON.stringify(bookingData))
      return { processed: false, reason: 'Insert failed', error: insertError.message, code: insertError.code, details: insertError.details }
    }

    // Also create passenger record(s)
    if (newBooking?.id) {
      // Loop through all passengers in the array
      const passengers = Array.isArray(passengerDetailsArray) ? passengerDetailsArray : [passengerDetailsArray]

      for (const passenger of passengers) {
        if (!passenger || Object.keys(passenger).length === 0) continue

        // Only use columns that exist in jc_passengers table
        const passengerData: any = {
          booking_id: newBooking.id,
          pnr_number: passenger.pnr_number || ticketNumber,
          full_name: passenger.name || passenger.passenger_name || payload.seat_detail_name || 'Unknown',
          mobile: passenger.phone_number || passenger.mobile || payload.seat_detail_mobile || 'N/A',
          seat_number: passenger.seat_number || payload.seat_numbers || 'N/A',
          fare: passenger.adult_fare || passenger.net_amount || passenger.fare || 0,
          is_cancelled: false,
          is_boarded: false,
          raw_data: payload,
        }
        // Link passenger to trip if available
        if (tripId) {
          passengerData.trip_id = tripId
        }

        const { error: passengerError } = await supabase
          .from('jc_passengers')
          .insert(passengerData)

        if (passengerError) {
          console.error('Error inserting passenger:', passengerError)
        }
      }
    }

    // DISABLED: WhatsApp booking confirmation for new bookings
    // Uncomment below to re-enable automatic booking notifications
    /*
    if (newBooking?.id) {
      const passengerPhone = firstPassenger?.phone_number || firstPassenger?.mobile || payload.seat_detail_mobile
      if (passengerPhone) {
        try {
          const whatsappResult = await sendBookingConfirmation({
            passengerName: firstPassenger?.name || firstPassenger?.passenger_name || payload.seat_detail_name || 'Valued Customer',
            pnr: ticketNumber,
            origin: payload.origin || 'N/A',
            destination: payload.destination || 'N/A',
            travelDate: bookingData.travel_date || '',
            boardingTime: formatTime(boardingTime) || 'N/A',
            seatNumber: firstPassenger?.seat_number || payload.seat_numbers || 'N/A',
            boardingPoint: payload.boarding_point_name || payload.boarding_point || firstPassenger?.boarding_at || 'N/A',
            fare: bookingData.total_fare,
            coachNumber: payload.coach_num || payload.bus_number || '',
          }, passengerPhone)

          console.log('[Webhook] WhatsApp notification result:', whatsappResult)
        } catch (whatsappError: any) {
          // Don't fail the booking if WhatsApp fails
          console.error('[Webhook] WhatsApp notification failed:', whatsappError.message)
        }
      } else {
        console.log('[Webhook] Skipping WhatsApp notification - no phone number available')
      }
    }
    */

    return { processed: true, action: 'created', booking_id: newBooking?.id }
  }
}

async function processReleaseEvent(supabase: any, payload: any, operatorId: string | null) {
  // For RELEASE events, passenger_details can be:
  // 1. A STRING (the passenger name directly) - most common for release
  // 2. An ARRAY of objects (like booking events)
  const passengerDetailsRaw = payload.passenger_details || payload.passengerDetails
  const isPassengerDetailsString = typeof passengerDetailsRaw === 'string'
  const passengerDetailsArray = isPassengerDetailsString ? [] : (passengerDetailsRaw || [])
  const firstPassenger = Array.isArray(passengerDetailsArray) ? passengerDetailsArray[0] || {} : {}

  // Extract passenger name - check if passenger_details is the name itself (string)
  const passengerName = isPassengerDetailsString
    ? passengerDetailsRaw
    : (firstPassenger?.name || firstPassenger?.passenger_name || payload.passenger_name || payload.name || 'Unknown')

  const ticketNumber = payload.travel_operator_pnr ||
                       payload.ticket_number ||
                       firstPassenger?.pnr_number ||
                       payload.pnr_number ||
                       payload.pnr ||
                       null

  console.log('[Release Event] Processing release for PNR:', ticketNumber, 'Passenger:', passengerName, 'Payload keys:', Object.keys(payload))

  if (!ticketNumber) {
    console.log('[Release Event] No ticket number found in payload')
    return { processed: false, reason: 'No ticket number provided' }
  }

  // Extract route info - route_id is integer, service_number is string like "MTS-83"
  const bitlaRouteId = payload.route_id ? parseInt(payload.route_id) : null
  const serviceNumber = payload.service_number || payload.route_num || payload.service || null

  // Find and update booking
  const { data: booking, error: findError } = await supabase
    .from('jc_bookings')
    .select('id')
    .eq('travel_operator_pnr', ticketNumber)
    .single()

  console.log('[Release Event] Found existing booking:', booking?.id, 'Error:', findError?.message)

  if (booking) {
    const { error: updateError } = await supabase
      .from('jc_bookings')
      .update({
        booking_status: 'released',
        raw_data: payload,
      })
      .eq('id', booking.id)

    if (updateError) {
      console.error('[Release Event] Update booking failed:', updateError)
    }

    // Update passenger
    const { error: passengerError } = await supabase
      .from('jc_passengers')
      .update({
        is_cancelled: true,
        raw_data: payload,
      })
      .eq('booking_id', booking.id)

    if (passengerError) {
      console.error('[Release Event] Update passenger failed:', passengerError)
    }

    console.log('[Release Event] Updated existing booking to released:', booking.id)
    return { processed: true, action: 'released', booking_id: booking.id }
  } else {
    // Create a record for the release even if booking doesn't exist
    // Use same columns as processBookingEvent for consistency
    const releaseBookingData = {
      operator_id: operatorId,
      travel_operator_pnr: ticketNumber,
      ticket_number: ticketNumber,
      service_number: serviceNumber,
      origin: payload.origin || firstPassenger.origin || null,
      destination: payload.destination || firstPassenger.destination || null,
      travel_date: parseDate(payload.travel_date || payload.journey_date),
      total_fare: firstPassenger.net_amount || payload.amount || payload.fare || 0,
      booking_status: 'released',
      booking_source: 'webhook',
      raw_data: payload,
      // Route/trip columns
      bitla_route_id: bitlaRouteId,
      bitla_reservation_id: payload.reservation_id ? parseInt(payload.reservation_id) : null,
      coach_num: payload.coach_num || payload.bus_number || null,
      captain1_details: payload.captain1_details || null,
      captain2_details: payload.captain2_details || null,
      route_duration: payload.route_duration || null,
      departure_time: payload.route_departure_time || payload.departure_time || null,
      arrival_time: payload.route_arrival_time || payload.arrival_time || null,
      // Booking details
      booked_by: payload.issued_by || payload.booked_by || firstPassenger.booked_by || null,
      boarding_point_name: payload.boarding_point_name || firstPassenger.boarding_at || null,
      boarding_address: payload.boarding_point_details_address || firstPassenger.boarding_address || null,
      boarding_landmark: payload.boarding_point_details_landmark || firstPassenger.boarding_landmark || null,
    }

    console.log('[Release Event] Creating new booking as released:', JSON.stringify(releaseBookingData))

    const { data: newBooking, error: insertError } = await supabase
      .from('jc_bookings')
      .insert(releaseBookingData)
      .select('id')
      .single()

    if (insertError) {
      console.error('[Release Event] Insert booking failed:', insertError)
      return { processed: false, reason: 'Insert failed', error: insertError.message }
    }

    if (newBooking?.id) {
      // Use the passengerName we extracted earlier (handles string vs array format)
      const passengerData = {
        booking_id: newBooking.id,
        pnr_number: ticketNumber,
        full_name: passengerName,
        mobile: payload.phone_number || firstPassenger?.phone_number || firstPassenger?.mobile || payload.mobile || 'N/A',
        seat_number: payload.seat_number || payload.seat_numbers || firstPassenger?.seat_number || 'N/A',
        fare: payload.amount || firstPassenger?.net_amount || payload.fare || 0,
        is_cancelled: true,
        is_boarded: false,
        raw_data: payload,
      }

      console.log('[Release Event] Creating passenger:', passengerData.full_name, 'Mobile:', passengerData.mobile)

      const { error: passengerError } = await supabase
        .from('jc_passengers')
        .insert(passengerData)

      if (passengerError) {
        console.error('[Release Event] Insert passenger failed:', passengerError)
      }

      console.log('[Release Event] Created new released booking:', newBooking.id)
    }

    return { processed: true, action: 'created_as_released', booking_id: newBooking?.id }
  }
}

async function processCancellationEvent(supabase: any, payload: any, operatorId: string | null) {
  // Bitla sends passenger_details as an ARRAY, and travel_operator_pnr at top level
  const passengerDetailsArray = payload.passenger_details || payload.passengerDetails || []
  const firstPassenger = Array.isArray(passengerDetailsArray) ? passengerDetailsArray[0] || {} : passengerDetailsArray

  const ticketNumber = payload.travel_operator_pnr ||
                       firstPassenger.pnr_number ||
                       payload.pnr_number ||
                       payload.ticket_number ||
                       payload.pnr ||
                       null

  if (!ticketNumber) {
    return { processed: false, reason: 'No ticket number provided' }
  }

  // Extract route_id - Bitla sends this as 'route_id', store in service_number column
  const serviceNumber = payload.route_id ||
                        payload.routeId ||
                        payload.service_number ||
                        payload.service ||
                        payload.serviceNumber ||
                        payload.route ||
                        null

  // Find and update booking
  const { data: booking } = await supabase
    .from('jc_bookings')
    .select('id')
    .eq('travel_operator_pnr', ticketNumber)
    .single()

  const cancellationData = {
    booking_status: 'cancelled',
    raw_data: payload,
  }

  if (booking) {
    await supabase
      .from('jc_bookings')
      .update(cancellationData)
      .eq('id', booking.id)

    // Update all passengers for this booking
    await supabase
      .from('jc_passengers')
      .update({ is_cancelled: true })
      .eq('booking_id', booking.id)

    return { processed: true, action: 'cancelled', booking_id: booking.id }
  } else {
    // Create cancelled booking record
    const { data: newBooking } = await supabase
      .from('jc_bookings')
      .insert({
        operator_id: operatorId,
        travel_operator_pnr: ticketNumber,
        ticket_number: ticketNumber,
        service_number: serviceNumber,
        travel_date: parseDate(payload.travel_date),
        total_fare: payload.amount || payload.fare || 0,
        booking_source: 'webhook',
        ...cancellationData,
      })
      .select('id')
      .single()

    if (newBooking?.id) {
      await supabase
        .from('jc_passengers')
        .insert({
          booking_id: newBooking.id,
          pnr_number: ticketNumber,
          full_name: payload.passenger_name || payload.name || 'Unknown',
          mobile: payload.phone_number || payload.mobile || 'N/A',
          seat_number: payload.seat_number || 'N/A',
          fare: payload.amount || payload.fare || 0,
          is_cancelled: true,
          is_boarded: false,
          raw_data: payload,
        })
    }

    return { processed: true, action: 'created_as_cancelled', booking_id: newBooking?.id }
  }
}

async function processBoardingEvent(supabase: any, payload: any, operatorId: string | null) {
  // Bitla sends passenger_details as an ARRAY, and travel_operator_pnr at top level
  const passengerDetailsArray = payload.passenger_details || payload.passengerDetails || []
  const firstPassenger = Array.isArray(passengerDetailsArray) ? passengerDetailsArray[0] || {} : passengerDetailsArray

  const ticketNumber = payload.travel_operator_pnr ||
                       firstPassenger.pnr_number ||
                       payload.pnr_number ||
                       payload.ticket_number ||
                       payload.pnr ||
                       null
  const seatNumber = firstPassenger.seat_number || payload.seat_number || payload.seat_numbers

  if (!ticketNumber) {
    return { processed: false, reason: 'No ticket number provided' }
  }

  // Find booking
  const { data: booking } = await supabase
    .from('jc_bookings')
    .select('id')
    .eq('travel_operator_pnr', ticketNumber)
    .single()

  if (booking) {
    // Update passenger boarding status
    let query = supabase
      .from('jc_passengers')
      .update({
        is_boarded: true,
        boarded_at: new Date().toISOString(),
        raw_data: payload,
      })
      .eq('booking_id', booking.id)

    if (seatNumber) {
      query = query.eq('seat_number', seatNumber)
    }

    await query

    return { processed: true, action: 'boarded', booking_id: booking.id }
  }

  return { processed: false, reason: 'Booking not found' }
}

async function processServiceUpdateEvent(supabase: any, payload: any, operatorId: string | null) {
  // Store service update info
  console.log('Service update received:', payload)
  return { processed: true, action: 'logged' }
}

async function processUpdateEvent(supabase: any, payload: any, operatorId: string | null) {
  // UPDATE event - updates existing booking with new boarding/dropoff details
  const ticketNumber = payload.travel_operator_pnr ||
                       payload.pnr_number ||
                       payload.ticket_number ||
                       payload.pnr ||
                       null

  if (!ticketNumber) {
    return { processed: false, reason: 'No ticket number provided for update' }
  }

  // Find existing booking
  const { data: existingBooking } = await supabase
    .from('jc_bookings')
    .select('id')
    .eq('travel_operator_pnr', ticketNumber)
    .single()

  if (!existingBooking) {
    return { processed: false, reason: 'Booking not found for update', pnr: ticketNumber }
  }

  // Parse boarding time from seconds
  let boardingTime = null
  if (payload.boarding_point_details_dep_time) {
    boardingTime = parseSecondsToTime(payload.boarding_point_details_dep_time)
  }

  // Build update data - only use columns that exist in jc_bookings table
  const updateData: any = {
    raw_data: payload,
  }

  // Update fare if provided
  if (payload.boarding_point_details_total_fare) {
    // Extract fare from array format if needed
    let fare = payload.boarding_point_details_total_fare
    if (typeof fare === 'string' && fare.startsWith('[')) {
      try {
        fare = JSON.parse(fare)[0]
      } catch (e) {
        // Keep as-is if parse fails
      }
    }
    if (typeof fare === 'number') {
      updateData.total_fare = fare
    }
  }

  // Update origin/destination if provided
  if (payload.origin) {
    updateData.origin = payload.origin
  }
  if (payload.destination) {
    updateData.destination = payload.destination
  }

  // Update booking
  const { error: updateError } = await supabase
    .from('jc_bookings')
    .update(updateData)
    .eq('id', existingBooking.id)

  if (updateError) {
    console.error('Error updating booking:', updateError)
    return { processed: false, reason: 'Update failed', error: updateError.message }
  }

  // Update passenger details if provided
  if (payload.passenger_details) {
    let passengers = payload.passenger_details

    // Parse if it's a JSON string
    if (typeof passengers === 'string') {
      try {
        passengers = JSON.parse(passengers)
      } catch (e) {
        console.error('Failed to parse passenger_details:', e)
        passengers = []
      }
    }

    if (Array.isArray(passengers)) {
      for (const passenger of passengers) {
        if (!passenger.seat_number) continue

        const passengerUpdate: any = {}
        if (passenger.name) {
          passengerUpdate.full_name = passenger.name
        }
        if (passenger.phone_number) {
          passengerUpdate.mobile = passenger.phone_number
        }
        if (passenger.email) {
          passengerUpdate.email = passenger.email
        }
        if (passenger.age) {
          passengerUpdate.age = passenger.age
        }

        if (Object.keys(passengerUpdate).length > 0) {
          await supabase
            .from('jc_passengers')
            .update(passengerUpdate)
            .eq('booking_id', existingBooking.id)
            .eq('seat_number', passenger.seat_number)
        }
      }
    }
  }

  return { processed: true, action: 'updated', booking_id: existingBooking.id }
}

async function processVehicleAssignEvent(supabase: any, payload: any, operatorId: string | null) {
  // Store vehicle assignment info
  console.log('Vehicle assign received:', payload)
  return { processed: true, action: 'logged' }
}

// Helper function to parse various date formats
function parseDate(dateStr: string | undefined): string | null {
  if (!dateStr) return null

  try {
    // Handle DD/MM/YYYY format
    if (dateStr.includes('/')) {
      const parts = dateStr.split('/')
      if (parts.length === 3) {
        // Assume DD/MM/YYYY
        const [day, month, year] = parts
        return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`
      }
    }

    // Handle YYYY-MM-DD or other formats
    const date = new Date(dateStr)
    if (!isNaN(date.getTime())) {
      return date.toISOString().split('T')[0]
    }
  } catch (e) {
    console.error('Date parse error:', e)
  }

  return null
}

// Helper function to convert seconds since midnight to HH:MM:SS time format
function parseSecondsToTime(secondsStr: string | undefined): string | null {
  if (!secondsStr) return null

  try {
    const totalSeconds = parseInt(secondsStr, 10)
    if (isNaN(totalSeconds)) return null

    const hours = Math.floor(totalSeconds / 3600)
    const minutes = Math.floor((totalSeconds % 3600) / 60)
    const seconds = totalSeconds % 60

    return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`
  } catch (e) {
    console.error('Time parse error:', e)
    return null
  }
}
