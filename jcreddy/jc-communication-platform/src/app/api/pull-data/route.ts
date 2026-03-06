import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

// Route segment config for Vercel - allow 60 second timeout
export const maxDuration = 60

// Bitla/Mythri API credentials
const BITLA_API_BASE = 'http://myth.mythribus.com/api'
const BITLA_LOGIN = 'myth.admin-vja'
const BITLA_PASSWORD = 'Chinna666!'

// Cache for API key (refreshed via login)
// NOTE: API keys expire! Always login to get fresh key.
let cachedApiKey: string | null = null
let cacheExpiry: number = 0

async function getApiKey(): Promise<string> {
  // Return cached key if valid
  if (cachedApiKey && Date.now() < cacheExpiry) {
    console.log('[Pull Data] Using cached API key')
    return cachedApiKey
  }

  // Login to get new API key using POST method
  console.log('[Pull Data] Logging in to get fresh API key...')
  try {
    const response = await fetch(`${BITLA_API_BASE}/login.json`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: `login=${encodeURIComponent(BITLA_LOGIN)}&password=${encodeURIComponent(BITLA_PASSWORD)}`,
    })

    if (!response.ok) {
      throw new Error(`Login failed with status ${response.status}`)
    }

    const data = await response.json()
    if (data.code === 200 && data.key) {
      // Cache the key for 1 hour (keys can expire)
      cachedApiKey = data.key
      cacheExpiry = Date.now() + (60 * 60 * 1000) // 1 hour
      console.log('[Pull Data] Got fresh API key, cached for 1 hour')
      return data.key
    }

    throw new Error(`Login returned unexpected response: ${JSON.stringify(data)}`)
  } catch (error: any) {
    console.error('[Pull Data] Login error:', error.message)
    throw new Error(`Failed to authenticate with Bitla API: ${error.message}`)
  }
}

// Interface for route from Bitla API
interface BitlaRoute {
  id: number  // This is the route_id used for API calls
  route_status: string
  route_num: string
  route_from?: string
  route_to?: string
  coach_type?: string
  via?: string
  days?: string
  [key: string]: any
}

// Interface for passenger from Bitla API
interface BitlaPassenger {
  pnr_number: string
  title: string
  name: string
  age: number
  mobile: string
  email: string
  seat_number: string
  origin: string
  destination: string
  booked_by: string
  boarding_at: string
  drop_off: string
  basic_amount: number
  net_amount: number
  is_boarded: number
  is_cancelled?: number
  booked_date: string
  boarding_address: string
  boarding_landmark: string
  cgst: number
  sgst: number
  igst: number
  [key: string]: any
}

// Interface for trip from Bitla API
interface BitlaTrip {
  route_num: string
  route_id: number
  travel_date: string
  coach_num: string
  bus_type: string
  available_seats: number
  blocked_count: number
  origin: string
  destination: string
  route_departure_time: string
  route_arrival_time: string
  captain1_details: string
  captain2_details: string
  customer_helpline_number: string
  reservation_id?: number
  passenger_details: BitlaPassenger[]
  [key: string]: any
}

// Record status types
type RecordStatus = 'inserted' | 'updated' | 'not_modified'

interface RouteReconciliation {
  route_id: number
  route_num: string
  route_from: string
  route_to: string
  status: RecordStatus
  raw_data: BitlaRoute
}

interface PassengerReconciliation {
  pnr_number: string
  name: string
  seat_number: string
  mobile: string
  origin: string
  destination: string
  fare: number
  booked_by: string
  is_boarded: boolean
  booking_status: RecordStatus
  passenger_status: RecordStatus
  raw_data: BitlaPassenger
}

interface TripReconciliation {
  route_id: number
  route_num: string
  origin: string
  destination: string
  travel_date: string
  departure_time: string
  arrival_time: string
  available_seats: number
  booked_seats: number
  blocked_seats: number
  phone_booking_count: number
  total_seats: number
  passenger_count: number
  status: RecordStatus
  passengers: PassengerReconciliation[]
  raw_data: BitlaTrip
}

// Helper to check if records are different (for not_modified status)
function hasChanges(existing: any, newData: any, fields: string[]): boolean {
  for (const field of fields) {
    if (existing[field] !== newData[field]) {
      return true
    }
  }
  return false
}

// POST - Pull and sync operation (supports single route, batch, or all routes)
export async function POST(request: NextRequest) {
  const startTime = Date.now()
  const supabase = createServerClient()

  // Parse request body
  let requestBody: {
    route_id?: number;
    date?: string;
    target_date?: string;
    routes_only?: boolean;
    batch?: number;  // Batch number (1-based), each batch has 3 routes
    batch_size?: number;  // Routes per batch, defaults to 3
    sync_job_id?: string;  // From cron trigger
    triggered_by?: string;  // 'cron' or 'manual'
  } = {}
  try {
    requestBody = await request.json()
  } catch {
    // No body provided, process all routes
  }

  const singleRouteId = requestBody.route_id
  // Accept both 'target_date' and 'date' for flexibility
  const targetDate = requestBody.target_date || requestBody.date || new Date().toISOString().split('T')[0]
  const routesOnly = requestBody.routes_only === true
  const batchNumber = requestBody.batch  // 1-based batch number
  const batchSize = requestBody.batch_size || 3  // Default 3 routes per batch
  const cronSyncJobId = requestBody.sync_job_id  // From cron trigger
  const triggeredBy = requestBody.triggered_by  // 'cron' or 'manual'

  console.log(`[Pull Data] Processing for date: ${targetDate}, triggered_by: ${triggeredBy || 'manual'}, sync_job_id: ${cronSyncJobId || 'none'}`)

  // Get or create operator using upsert
  let operatorId: string | undefined

  try {
    // First try to get existing operator
    const { data: operator, error: selectError } = await supabase
      .from('jc_operators')
      .select('id')
      .eq('operator_code', 'mythri')
      .single()

    if (operator?.id) {
      operatorId = operator.id
      console.log('[Pull Data] Found existing operator:', operatorId)
    } else if (selectError && selectError.code !== 'PGRST116') {
      // PGRST116 = "no rows returned" which is expected if operator doesn't exist
      console.error('[Pull Data] Error fetching operator:', selectError)
    }

    // If no operator found, create one
    if (!operatorId) {
      console.log('[Pull Data] Operator "mythri" not found, creating...')
      const { data: newOperator, error: insertError } = await supabase
        .from('jc_operators')
        .upsert({
          operator_code: 'mythri',
          operator_name: 'Mythri Travels',
          status: 'active',
        }, {
          onConflict: 'operator_code',
        })
        .select('id')
        .single()

      if (insertError) {
        console.error('[Pull Data] Failed to upsert operator:', insertError)
        // Try one more time to fetch - maybe it was created by another request
        const { data: retryOperator } = await supabase
          .from('jc_operators')
          .select('id')
          .eq('operator_code', 'mythri')
          .single()

        if (retryOperator?.id) {
          operatorId = retryOperator.id
          console.log('[Pull Data] Found operator on retry:', operatorId)
        } else {
          return NextResponse.json({
            status: 'error',
            message: 'Failed to create/find operator: ' + insertError.message
          }, { status: 500 })
        }
      } else {
        operatorId = newOperator?.id
        console.log('[Pull Data] Created operator with ID:', operatorId)
      }
    }
  } catch (err: any) {
    console.error('[Pull Data] Operator setup error:', err)
    return NextResponse.json({
      status: 'error',
      message: 'Database connection error: ' + err.message
    }, { status: 500 })
  }

  // Only create sync job for full sync (not single route)
  let syncJobId: string | undefined
  if (!singleRouteId && !routesOnly) {
    const { data: syncJob } = await supabase
      .from('jc_sync_jobs')
      .insert({
        operator_id: operatorId,
        sync_type: 'full',
        target_date: targetDate,
        status: 'running',
        started_at: new Date().toISOString(),
        details: { source: 'pull_data_page' }
      })
      .select('id')
      .single()
    syncJobId = syncJob?.id
  }

  try {
    const apiKey = await getApiKey()

    // Step 1: Fetch all routes from Bitla API
    const routesUrl = `${BITLA_API_BASE}/all_routes.json?api_key=${apiKey}`
    const routesResponse = await fetch(routesUrl)

    if (!routesResponse.ok) {
      throw new Error('Failed to fetch routes from Bitla API')
    }

    const routesData = await routesResponse.json()

    // Handle case where API returns an error object instead of array
    if (!Array.isArray(routesData)) {
      console.error('[Pull Data] Bitla API returned non-array:', JSON.stringify(routesData).slice(0, 200))
      return NextResponse.json({
        status: 'error',
        message: 'Bitla API returned invalid response (not an array)',
        details: typeof routesData === 'object' ? routesData : String(routesData).slice(0, 100)
      }, { status: 502 })
    }

    const allRoutes: BitlaRoute[] = routesData
    // Only process active routes
    let activeRoutes = allRoutes.filter((r) => r.route_status === 'Active')

    // If single route requested, filter to just that route
    if (singleRouteId) {
      activeRoutes = activeRoutes.filter((r) => r.id === singleRouteId)
      if (activeRoutes.length === 0) {
        return NextResponse.json({
          status: 'error',
          message: `Route ${singleRouteId} not found or not active`
        }, { status: 404 })
      }
    }

    // Calculate total batches for reference
    const totalBatches = Math.ceil(activeRoutes.length / batchSize)

    // If batch number specified, filter to just that batch
    if (batchNumber && !singleRouteId) {
      if (batchNumber < 1 || batchNumber > totalBatches) {
        return NextResponse.json({
          status: 'error',
          message: `Invalid batch number ${batchNumber}. Valid range: 1-${totalBatches}`,
          total_routes: activeRoutes.length,
          total_batches: totalBatches,
          batch_size: batchSize,
        }, { status: 400 })
      }
      const startIdx = (batchNumber - 1) * batchSize
      const endIdx = startIdx + batchSize
      activeRoutes = activeRoutes.slice(startIdx, endIdx)
      console.log(`[Pull Data] Processing batch ${batchNumber}/${totalBatches} (routes ${startIdx + 1}-${Math.min(endIdx, activeRoutes.length + startIdx)})`)
    }

    console.log(`Processing ${activeRoutes.length} route(s)${singleRouteId ? ` (route_id: ${singleRouteId})` : ''}${batchNumber ? ` (batch ${batchNumber}/${totalBatches})` : ''}`)

    // If routes_only mode, return just the routes without processing passengers
    if (routesOnly) {
      // Calculate batches for display
      const allActiveRoutes = allRoutes.filter((r) => r.route_status === 'Active')
      const batches = []
      for (let i = 0; i < allActiveRoutes.length; i += batchSize) {
        const batchRoutes = allActiveRoutes.slice(i, i + batchSize)
        batches.push({
          batch_number: Math.floor(i / batchSize) + 1,
          routes: batchRoutes.map(r => ({
            id: r.id,
            route_num: r.route_num,
            route_from: r.route_from || '',
            route_to: r.route_to || '',
          }))
        })
      }

      return NextResponse.json({
        status: 'success',
        message: 'Routes fetched successfully',
        routes: allActiveRoutes.map(r => ({
          id: r.id,
          route_num: r.route_num,
          route_from: r.route_from || '',
          route_to: r.route_to || '',
          route_status: r.route_status
        })),
        total_routes: allRoutes.length,
        active_routes: allActiveRoutes.length,
        total_batches: Math.ceil(allActiveRoutes.length / batchSize),
        batch_size: batchSize,
        batches: batches,
      })
    }

    // Track route reconciliation details
    const routeReconciliations: RouteReconciliation[] = []

    // Step 2: Sync routes to database
    let routesSynced = 0
    let routesUpdated = 0
    let routesNotModified = 0
    let routeSyncErrors = 0

    // Map to store synced route UUIDs by bitla_route_id for quick lookup in Step 3
    const syncedRouteMap = new Map<number, string>()

    for (const route of activeRoutes) {
      const { data: existingRoute } = await supabase
        .from('jc_routes')
        .select('id, updated_at, service_number, status')
        .eq('bitla_route_id', route.id)
        .single()

      const routeData = {
        operator_id: operatorId,
        bitla_route_id: route.id,
        service_number: route.route_num,
        route_name: route.route_num || `${route.route_from} - ${route.route_to}`,
        // origin and destination are NOT NULL in schema - MUST provide them
        origin: route.route_from || route.route_num?.split(' To ')?.[0]?.replace('Mythri ', '') || 'Unknown',
        destination: route.route_to || route.route_num?.split(' To ')?.[1]?.split(' SPL')?.[0] || 'Unknown',
        status: route.route_status?.toLowerCase() === 'active' ? 'active' : 'inactive',
        last_synced_at: new Date().toISOString(),
      }

      let routeStatus: RecordStatus
      let dbRouteId: string | undefined

      if (existingRoute) {
        dbRouteId = existingRoute.id
        // Check if there are actual changes
        const hasRouteChanges = existingRoute.service_number !== route.route_num ||
                                existingRoute.status !== routeData.status

        if (hasRouteChanges) {
          const { error: updateError } = await supabase
            .from('jc_routes')
            .update(routeData)
            .eq('id', existingRoute.id)

          if (updateError) {
            console.error(`[Route Sync] Failed to update route ${route.id}:`, updateError)
            routeSyncErrors++
            continue
          }
          routesUpdated++
          routeStatus = 'updated'
        } else {
          // Just update last_synced_at
          await supabase
            .from('jc_routes')
            .update({ last_synced_at: new Date().toISOString() })
            .eq('id', existingRoute.id)
          routesNotModified++
          routeStatus = 'not_modified'
        }
      } else {
        // INSERT new route with error handling
        const { data: newRoute, error: insertError } = await supabase
          .from('jc_routes')
          .insert(routeData)
          .select('id')
          .single()

        if (insertError) {
          console.error(`[Route Sync] Failed to insert route ${route.id} (${route.route_num}):`, insertError)
          routeSyncErrors++
          continue
        }

        dbRouteId = newRoute?.id
        routesSynced++
        routeStatus = 'inserted'
        console.log(`[Route Sync] Inserted route ${route.id} (${route.route_num}) with DB ID: ${dbRouteId}`)
      }

      // Store the mapping for quick lookup in Step 3
      if (dbRouteId) {
        syncedRouteMap.set(route.id, dbRouteId)
      }

      routeReconciliations.push({
        route_id: route.id,
        route_num: route.route_num,
        route_from: route.route_from || '',
        route_to: route.route_to || '',
        status: routeStatus,
        raw_data: route,
      })
    }

    console.log(`[Route Sync] Completed: ${routesSynced} inserted, ${routesUpdated} updated, ${routesNotModified} unchanged, ${routeSyncErrors} errors`)
    console.log(`[Route Sync] syncedRouteMap has ${syncedRouteMap.size} entries`)

    // Step 3: Loop through each active route and fetch passenger details for today
    let totalTrips = 0
    let totalBookings = 0
    let totalPassengers = 0
    let bookingsInserted = 0
    let bookingsUpdated = 0
    let bookingsNotModified = 0
    let passengersInserted = 0
    let passengersUpdated = 0
    let passengersNotModified = 0
    const errors: string[] = []

    // Track trip reconciliation details
    const tripReconciliations: TripReconciliation[] = []

    // Loop through each active route and fetch passengers
    for (const route of activeRoutes) {
      try {
        // Fetch passengers for this specific route with timeout
        const passengersUrl = `${BITLA_API_BASE}/get_passenger_details/${targetDate}.json?api_key=${apiKey}&route_id=${route.id}`
        console.log(`Fetching passengers for route ${route.id}: ${passengersUrl}`)

        let passengersResponse: Response
        try {
          // Add timeout to prevent hanging
          const controller = new AbortController()
          const timeoutId = setTimeout(() => controller.abort(), 30000) // 30 second timeout

          passengersResponse = await fetch(passengersUrl, {
            signal: controller.signal
          })
          clearTimeout(timeoutId)
        } catch (fetchError: any) {
          const errorMsg = fetchError.name === 'AbortError'
            ? `Timeout fetching route ${route.id}`
            : `Network error for route ${route.id}: ${fetchError.message}`
          console.log(errorMsg)
          errors.push(errorMsg)
          continue
        }

        if (!passengersResponse.ok) {
          console.log(`No data for route ${route.id}: ${passengersResponse.status}`)
          errors.push(`Route ${route.id}: HTTP ${passengersResponse.status}`)
          continue
        }

        // Check if response is JSON before parsing
        const responseText = await passengersResponse.text()

        // Check if response looks like an error message
        if (responseText.startsWith('An error') || responseText.startsWith('<!') || responseText.startsWith('<html')) {
          console.log(`Error response for route ${route.id}: ${responseText.substring(0, 100)}`)
          errors.push(`Route ${route.id}: API returned error`)
          continue
        }

        let tripDataArray: BitlaTrip[]
        try {
          tripDataArray = JSON.parse(responseText)
        } catch (parseError) {
          console.log(`Invalid JSON for route ${route.id}: ${responseText.substring(0, 100)}`)
          errors.push(`Route ${route.id}: Invalid JSON response`)
          continue
        }

        // Handle empty response
        if (!tripDataArray || (Array.isArray(tripDataArray) && tripDataArray.length === 0)) {
          console.log(`No trips for route ${route.id}`)
          continue
        }

        const trips = Array.isArray(tripDataArray) ? tripDataArray : [tripDataArray]

        for (const tripData of trips) {
          // IMPORTANT: Use the syncedRouteMap from Step 2 - this is guaranteed to have the correct UUID
          const routeIdFromLoop = route.id
          totalTrips++

          // First check our syncedRouteMap (most reliable - we JUST synced these)
          let dbRouteId = syncedRouteMap.get(routeIdFromLoop)
          let dbRoute: { id: string; origin?: string; destination?: string } | null = null

          if (dbRouteId) {
            // We have the UUID from sync, just need to fetch origin/destination
            const { data: routeData } = await supabase
              .from('jc_routes')
              .select('id, origin, destination')
              .eq('id', dbRouteId)
              .single()
            dbRoute = routeData
          }

          // Fallback: try database lookup by bitla_route_id
          if (!dbRoute) {
            const { data: routeByBitlaId } = await supabase
              .from('jc_routes')
              .select('id, origin, destination')
              .eq('bitla_route_id', routeIdFromLoop)
              .single()
            dbRoute = routeByBitlaId
          }

          // If still not found, skip this trip
          if (!dbRoute) {
            console.log(`WARNING: Route not found. bitla_route_id=${routeIdFromLoop}, route_num=${route.route_num}. Was it synced in Step 2?`)
            errors.push(`Route not synced: ${route.route_num} (ID: ${routeIdFromLoop})`)
            continue
          }

          const routeId = routeIdFromLoop // Use for logging/tracking

        // Update route with origin/destination from trip data if not already set
        if (dbRoute && tripData.origin && tripData.destination && (!dbRoute.origin || !dbRoute.destination)) {
          await supabase
            .from('jc_routes')
            .update({
              origin: tripData.origin,
              destination: tripData.destination,
              route_name: `${tripData.origin} - ${tripData.destination}`,
            })
            .eq('id', dbRoute.id)
        }

        // Parse captain/attendant details (format: "Name - Phone")
        const parseCaptainDetails = (details: string | undefined) => {
          if (!details) return { name: null, phone: null }
          const parts = details.split(' - ')
          return {
            name: parts[0]?.trim() || null,
            phone: parts[1]?.trim() || null,
          }
        }

        // Parse booking date (format: "14/12/2025" to "2025-12-14")
        const parseBookingDate = (dateStr: string) => {
          if (!dateStr) return null
          // Handle DD/MM/YYYY format
          const parts = dateStr.split('/')
          if (parts.length === 3) {
            return `${parts[2]}-${parts[1].padStart(2, '0')}-${parts[0].padStart(2, '0')}`
          }
          return dateStr
        }

        const captain1 = parseCaptainDetails(tripData.captain1_details)
        const captain2 = parseCaptainDetails(tripData.captain2_details)
        const attendant = parseCaptainDetails(tripData.attendent_details)

        // Parse travel date (format: "17-12-2025" to "2025-12-17")
        // Falls back to targetDate if not provided (we're fetching for that date anyway)
        const parseTravelDate = (dateStr: string | undefined | null): string => {
          if (!dateStr) {
            console.log(`[Trip Sync] No travel_date in API response, using targetDate: ${targetDate}`)
            return targetDate // Use the date we're fetching for
          }
          // Handle DD-MM-YYYY format
          const dashParts = dateStr.split('-')
          if (dashParts.length === 3 && dashParts[0].length <= 2) {
            return `${dashParts[2]}-${dashParts[1].padStart(2, '0')}-${dashParts[0].padStart(2, '0')}`
          }
          // Handle DD/MM/YYYY format
          const slashParts = dateStr.split('/')
          if (slashParts.length === 3) {
            return `${slashParts[2]}-${slashParts[1].padStart(2, '0')}-${slashParts[0].padStart(2, '0')}`
          }
          // Already in YYYY-MM-DD format or other format
          return dateStr
        }

        // Calculate total seats
        const bookedSeats = tripData.passenger_details?.length || 0
        const availableSeats = tripData.available_seats || 0
        const blockedSeats = tripData.blocked_count || 0
        const totalSeats = bookedSeats + availableSeats + blockedSeats

        // Create or update trip with ALL fields
        // NOTE: We removed reservation_id and coach_id as they are integers from Bitla but DB expects UUID
        // We use route_id + travel_date + service_number for matching instead

        // Validate that dbRoute.id is a valid UUID (should be from our database)
        const isValidUUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(dbRoute.id)
        if (!isValidUUID) {
          console.error(`[Trip Sync] Invalid UUID for route_id: ${dbRoute.id}, skipping trip`)
          errors.push(`Invalid route UUID: ${dbRoute.id} for service ${tripData.route_num}`)
          continue
        }

        const tripRecord: any = {
          route_id: dbRoute.id, // Always use the UUID from our database
          travel_date: parseTravelDate(tripData.travel_date),
          service_number: tripData.route_num,
          bus_type: tripData.bus_type,
          departure_time: tripData.route_departure_time,
          arrival_time: tripData.route_arrival_time,
          // Seat counts
          total_seats: totalSeats,
          available_seats: availableSeats,
          blocked_seats: blockedSeats,
          booked_seats: bookedSeats,
          blocked_seat_numbers: tripData.blocked_seat_numbers || null,
          phone_booking_count: tripData.phone_booking_count || 0,
          phone_booking_seat_numbers: tripData.phone_booking_seat_numbers || null,
          // Coach/Bus details - only include non-ID fields
          coach_num: tripData.coach_num || null,
          // coach_id removed - integer from Bitla but DB expects UUID
          coach_mobile_number: tripData.coach_mobile_number || null,
          // Captain and attendant details
          captain1_name: captain1.name,
          captain1_phone: captain1.phone,
          captain2_name: captain2.name,
          captain2_phone: captain2.phone,
          attendant_name: attendant.name,
          attendant_phone: attendant.phone,
          // Route details
          origin: tripData.origin || null,
          destination: tripData.destination || null,
          route_duration: tripData.route_duration || null,
          // Additional details
          helpline_number: tripData.customer_helpline_number,
          chart_operated_by: tripData.chart_operated_by || null,
          pickup_van_details: tripData.pickup_van_details || null,
          trip_status: 'scheduled',
          raw_data: tripData,
          last_synced_at: new Date().toISOString(),
        }

        const parsedTravelDate = parseTravelDate(tripData.travel_date)

        console.log(`[Trip Sync] Processing trip: service=${tripData.route_num}, date=${parsedTravelDate}, booked=${bookedSeats}, available=${availableSeats}, blocked=${blockedSeats}`)

        // Build query to find existing trip - match on travel_date + service_number
        // This is more reliable than matching on reservation_id which can change
        let existingTrip = null

        // First try: match by travel_date + route_id (most specific)
        if (dbRoute?.id) {
          const { data: tripByRouteId } = await supabase
            .from('jc_trips')
            .select('id, available_seats, booked_seats, blocked_seats, departure_time, arrival_time')
            .eq('travel_date', parsedTravelDate)
            .eq('route_id', dbRoute.id)
            .single()
          existingTrip = tripByRouteId
        }

        // Second try: match by travel_date + service_number
        if (!existingTrip) {
          const { data: tripByServiceNum } = await supabase
            .from('jc_trips')
            .select('id, available_seats, booked_seats, blocked_seats, departure_time, arrival_time')
            .eq('travel_date', parsedTravelDate)
            .eq('service_number', tripData.route_num)
            .single()
          existingTrip = tripByServiceNum
        }

        // Note: reservation_id matching removed as it's an integer from Bitla but DB expects UUID

        console.log(`[Trip Sync] Existing trip found: ${existingTrip ? 'YES (id=' + existingTrip.id + ')' : 'NO'}`)

        let tripId: string
        let tripStatus: RecordStatus

        if (existingTrip) {
          console.log(`[Trip Sync] Found existing trip ${existingTrip.id}, updating with: available=${tripRecord.available_seats}, booked=${tripRecord.booked_seats}, blocked=${tripRecord.blocked_seats}, phone=${tripRecord.phone_booking_count}`)

          // Update the trip record
          const { data: updatedTrip, error: updateError } = await supabase
            .from('jc_trips')
            .update({
              available_seats: tripRecord.available_seats,
              booked_seats: tripRecord.booked_seats,
              blocked_seats: tripRecord.blocked_seats,
              phone_booking_count: tripRecord.phone_booking_count,
              total_seats: tripRecord.total_seats,
              blocked_seat_numbers: tripRecord.blocked_seat_numbers,
              phone_booking_seat_numbers: tripRecord.phone_booking_seat_numbers,
              departure_time: tripRecord.departure_time,
              arrival_time: tripRecord.arrival_time,
              bus_type: tripRecord.bus_type,
              origin: tripRecord.origin,
              destination: tripRecord.destination,
              captain1_name: tripRecord.captain1_name,
              captain1_phone: tripRecord.captain1_phone,
              captain2_name: tripRecord.captain2_name,
              captain2_phone: tripRecord.captain2_phone,
              attendant_name: tripRecord.attendant_name,
              attendant_phone: tripRecord.attendant_phone,
              helpline_number: tripRecord.helpline_number,
              raw_data: tripRecord.raw_data,
              last_synced_at: new Date().toISOString(),
            })
            .eq('id', existingTrip.id)
            .select('id, available_seats, booked_seats, blocked_seats, phone_booking_count')
            .single()

          if (updateError) {
            console.error(`[Trip Sync] UPDATE FAILED for trip ${existingTrip.id}:`, updateError)
            errors.push(`Trip update failed: ${updateError.message}`)
          } else {
            console.log(`[Trip Sync] UPDATE SUCCESS - Verified: available=${updatedTrip?.available_seats}, booked=${updatedTrip?.booked_seats}, blocked=${updatedTrip?.blocked_seats}, phone=${updatedTrip?.phone_booking_count}`)
          }

          tripStatus = 'updated'
          tripId = existingTrip.id
        } else {
          console.log(`[Trip Sync] No existing trip found, inserting new record for ${tripData.route_num}`)

          const { data: newTrip, error: insertError } = await supabase
            .from('jc_trips')
            .insert(tripRecord)
            .select('id, available_seats, booked_seats, blocked_seats, phone_booking_count')
            .single()

          if (insertError) {
            console.error(`[Trip Sync] INSERT FAILED for service ${tripData.route_num}:`, insertError)
            errors.push(`Trip insert failed: ${insertError.message}`)
          } else {
            console.log(`[Trip Sync] INSERT SUCCESS - id=${newTrip?.id}, available=${newTrip?.available_seats}, booked=${newTrip?.booked_seats}, blocked=${newTrip?.blocked_seats}`)
          }

          tripId = newTrip?.id
          tripStatus = 'inserted'
        }

        // Track passenger reconciliations for this trip
        const passengerReconciliations: PassengerReconciliation[] = []

        // Process passengers - wrap each in try-catch so one failure doesn't break all
        if (tripData.passenger_details && tripData.passenger_details.length > 0) {
          // Group passengers by PNR to handle group bookings correctly
          const passengersByPnr = new Map<string, BitlaPassenger[]>()
          for (const passenger of tripData.passenger_details) {
            const pnr = passenger.pnr_number
            if (!passengersByPnr.has(pnr)) {
              passengersByPnr.set(pnr, [])
            }
            passengersByPnr.get(pnr)!.push(passenger)
          }

          console.log(`[Booking Sync] Found ${passengersByPnr.size} unique PNRs from ${tripData.passenger_details.length} passengers`)

          // Process each PNR group
          for (const [pnr, pnrPassengers] of Array.from(passengersByPnr.entries())) {
            const primaryPassenger = pnrPassengers[0]
            const seatNumbers = pnrPassengers.map((p: BitlaPassenger) => p.seat_number).join(', ')
            const totalFare = pnrPassengers.reduce((sum: number, p: BitlaPassenger) => sum + (p.net_amount || 0), 0)
            const baseFare = pnrPassengers.reduce((sum: number, p: BitlaPassenger) => sum + (p.basic_amount || 0), 0)
            const totalCgst = pnrPassengers.reduce((sum: number, p: BitlaPassenger) => sum + (p.cgst || 0), 0)
            const totalSgst = pnrPassengers.reduce((sum: number, p: BitlaPassenger) => sum + (p.sgst || 0), 0)
            const totalIgst = pnrPassengers.reduce((sum: number, p: BitlaPassenger) => sum + (p.igst || 0), 0)
            const totalSeatsForPnr = pnrPassengers.length

            // Determine booking status - if any passenger is cancelled, mark as cancelled
            const hasAnyCancelled = pnrPassengers.some((p: BitlaPassenger) => p.is_cancelled === 1)
            const allCancelled = pnrPassengers.every((p: BitlaPassenger) => p.is_cancelled === 1)
            let bookingStatusValue = 'confirmed'
            if (allCancelled) {
              bookingStatusValue = 'cancelled'
            } else if (hasAnyCancelled) {
              bookingStatusValue = 'partial_cancelled'
            }

            // Debug: Log tripData fields to verify they exist
            console.log(`[Booking Sync] Creating booking for PNR ${pnr}, tripId=${tripId}, service=${tripData.route_num}, seats=${totalSeatsForPnr}`)
            console.log(`[Booking Sync] tripData fields: route_id=${tripData.route_id}, reservation_id=${tripData.reservation_id}, coach_num=${tripData.coach_num}, captain1=${tripData.captain1_details}, departure=${tripData.route_departure_time}`)

            // Log the actual tripData object to verify it has the fields
            console.log(`[Booking Sync] FULL tripData keys: ${Object.keys(tripData).join(', ')}`)
            console.log(`[Booking Sync] tripData.route_id VALUE: ${JSON.stringify(tripData.route_id)}`)
            console.log(`[Booking Sync] tripData.coach_num VALUE: ${JSON.stringify(tripData.coach_num)}`)

            const bookingRecord: any = {
              trip_id: tripId,
              operator_id: operatorId,
              ticket_number: pnr,
              travel_operator_pnr: pnr,
              service_number: tripData.route_num,
              travel_date: parseTravelDate(tripData.travel_date),
              origin: primaryPassenger.origin || tripData.origin,
              destination: primaryPassenger.destination || tripData.destination,
              // Route and trip details from tripData
              bitla_route_id: tripData.route_id,
              bitla_reservation_id: tripData.reservation_id,
              coach_num: tripData.coach_num,
              captain1_details: tripData.captain1_details,
              captain2_details: tripData.captain2_details,
              route_duration: tripData.route_duration,
              departure_time: tripData.route_departure_time,
              arrival_time: tripData.route_arrival_time,
              // Fare details
              total_fare: totalFare,
              // Booking info
              booked_by: primaryPassenger.booked_by || null,
              booking_source: 'api_sync',
              booking_status: bookingStatusValue,
              // Boarding point details from passenger
              boarding_point_name: primaryPassenger.boarding_at || primaryPassenger.boarding_point || null,
              boarding_address: primaryPassenger.boarding_address || null,
              boarding_time: primaryPassenger.boarding_time || null,
              boarding_landmark: primaryPassenger.boarding_landmark || null,
              // Raw data for reference (all passengers in this PNR)
              raw_data: pnrPassengers.length > 1 ? pnrPassengers : primaryPassenger,
            }

            const { data: existingBooking } = await supabase
              .from('jc_bookings')
              .select('id, total_fare, boarding_point_name, dropoff_point_name, travel_date')
              .eq('travel_operator_pnr', pnr)
              .single()

            let bookingId: string
            let bookingStatus: RecordStatus

            if (existingBooking) {
              // Always update booking to ensure travel_date and all fields are current
              const { error: updateError } = await supabase
                .from('jc_bookings')
                .update(bookingRecord)
                .eq('id', existingBooking.id)

              if (updateError) {
                console.error(`[Booking Sync] Update failed for PNR ${pnr}:`, updateError)
              } else {
                console.log(`[Booking Sync] Updated booking ${existingBooking.id} with travel_date=${bookingRecord.travel_date}, total_seats=${totalSeatsForPnr}`)
              }

              bookingsUpdated++
              bookingStatus = 'updated'
              bookingId = existingBooking.id
            } else {
              const { data: newBooking, error: insertError } = await supabase
                .from('jc_bookings')
                .insert(bookingRecord)
                .select('id')
                .single()

              if (insertError) {
                console.error(`[Booking Sync] Insert failed for PNR ${pnr}:`, insertError)
              } else {
                console.log(`[Booking Sync] Inserted new booking ${newBooking?.id} with travel_date=${bookingRecord.travel_date}, total_seats=${totalSeatsForPnr}`)
              }

              bookingId = newBooking?.id
              bookingsInserted++
              bookingStatus = 'inserted'
            }

            totalBookings++

            // Now process each passenger in this PNR group
            for (const passenger of pnrPassengers) {
              try {
                totalPassengers++

              // Parse booked_date (format: "14/12/2025" to "2025-12-14")
              const parseBookedDate = (dateStr: string) => {
                if (!dateStr) return null
                const parts = dateStr.split('/')
                if (parts.length === 3) {
                  return `${parts[2]}-${parts[1]}-${parts[0]}`
                }
                return dateStr
              }

              // Create or update passenger with ALL fields
              const passengerRecord: any = {
                booking_id: bookingId,
                trip_id: tripId,
                pnr_number: pnr,
                // Name fields
                title: passenger.title || null,
                first_name: passenger.name?.split(' ')?.[0] || passenger.name,
                last_name: passenger.name?.split(' ')?.slice(1)?.join(' ') || '',
                full_name: `${passenger.title || ''} ${passenger.name || ''}`.trim(),
                // Demographics
                age: passenger.age || null,
                gender: passenger.title === 'Mr' ? 'Male' : passenger.title === 'Ms' || passenger.title === 'Miss' ? 'Female' : null,
                // Contact
                mobile: passenger.mobile,
                email: passenger.email,
                seat_number: passenger.seat_number,
                // Route
                origin: passenger.origin || tripData.origin,
                destination: passenger.destination || tripData.destination,
                // Booking source
                booked_by: passenger.booked_by || null,
                booked_by_login: passenger.booked_by_login || null,
                booking_type_id: passenger.booking_type_id || null,
                booked_date: parseBookedDate(passenger.booked_date),
                // Boarding details
                boarding_at: passenger.boarding_at || null,
                boarding_at_id: passenger.boarding_at_id || null,
                boarding_address: passenger.boarding_address || null,
                boarding_landmark: passenger.boarding_landmark || null,
                // Drop off details
                drop_off: passenger.drop_off || null,
                dropoff_id: passenger.dropoff_id || null,
                // Fare details
                fare: passenger.net_amount || 0,
                base_fare: passenger.basic_amount || 0,
                service_tax: passenger.service_tax_amount || 0,
                commission_amount: passenger.commission_amount || 0,
                seat_discount: passenger.seat_discount || 0,
                net_amount: passenger.net_amount || 0,
                transaction_charges: passenger.transaction_charges || 0,
                // GPS coordinates
                bp_latitude: passenger.bp_stage_latitude ? parseFloat(passenger.bp_stage_latitude) : null,
                bp_longitude: passenger.bp_stage_longitude ? parseFloat(passenger.bp_stage_longitude) : null,
                dp_latitude: passenger.dp_stage_latitude ? parseFloat(passenger.dp_stage_latitude) : null,
                dp_longitude: passenger.dp_stage_longitude ? parseFloat(passenger.dp_stage_longitude) : null,
                // ID Card
                id_card_type: passenger.id_card?.type || null,
                id_card_number: passenger.id_card?.number || null,
                // Status flags
                is_primary: passenger.is_primary === 1,
                is_confirmed: passenger.is_ticket_confirm !== false,
                is_boarded: passenger.is_boarded === 1,
                is_shifted: passenger.is_shifted === true,
                is_phone_blocked: passenger.is_phone_blocked === true,
                is_coupon_created_ticket: passenger.is_coupon_created_ticket === true,
                is_inclusive_service_tax: passenger.is_inclusive_service_tax === true,
                is_trackingo_sms_allowed: passenger.is_trackingo_sms_allowed !== false,
                // Service flags
                wake_up_call_applicable: passenger.wake_up_call_applicable === true,
                pre_boarding_applicable: passenger.pre_boarding_applicable === true,
                welcome_call_applicable: passenger.welcome_call_applicable === true,
                // CS fields
                cs_booking_type: passenger.cs_booking_type || null,
                cs_booked_by: passenger.cs_booked_by || null,
                // Raw data
                raw_data: passenger,
              }

              const { data: existingPassenger } = await supabase
                .from('jc_passengers')
                .select('id, is_boarded, fare, seat_number')
                .eq('booking_id', bookingId)
                .eq('seat_number', passenger.seat_number)
                .single()

              let passengerStatus: RecordStatus

              if (existingPassenger) {
                // Always update passenger to ensure all fields are current
                const { error: updateError } = await supabase
                  .from('jc_passengers')
                  .update(passengerRecord)
                  .eq('id', existingPassenger.id)

                if (updateError) {
                  console.error(`[Passenger Sync] Update failed for seat ${passenger.seat_number}:`, updateError)
                }

                passengersUpdated++
                passengerStatus = 'updated'
              } else {
                const { error: insertError } = await supabase
                  .from('jc_passengers')
                  .insert(passengerRecord)

                if (insertError) {
                  console.error(`[Passenger Sync] Insert failed for seat ${passenger.seat_number}:`, insertError)
                }

                passengersInserted++
                passengerStatus = 'inserted'
              }

              passengerReconciliations.push({
                pnr_number: pnr,
                name: `${passenger.title || ''} ${passenger.name || ''}`.trim(),
                seat_number: passenger.seat_number,
                mobile: passenger.mobile,
                origin: passenger.origin || tripData.origin,
                destination: passenger.destination || tripData.destination,
                fare: passenger.net_amount,
                booked_by: passenger.booked_by,
                is_boarded: passenger.is_boarded === 1,
                booking_status: bookingStatus,
                passenger_status: passengerStatus,
                raw_data: passenger,
              })
            } catch (passengerError: any) {
              console.error(`Error processing passenger ${passenger.pnr_number}:`, passengerError)
              // Still add the passenger to reconciliations with error status
              passengerReconciliations.push({
                pnr_number: passenger.pnr_number,
                name: `${passenger.title || ''} ${passenger.name || ''}`.trim(),
                seat_number: passenger.seat_number,
                mobile: passenger.mobile,
                origin: passenger.origin || tripData.origin,
                destination: passenger.destination || tripData.destination,
                fare: passenger.net_amount,
                booked_by: passenger.booked_by,
                is_boarded: passenger.is_boarded === 1,
                booking_status: 'not_modified',
                passenger_status: 'not_modified',
                raw_data: passenger,
              })
            }
          } // end inner passenger loop (for each passenger in PNR group)
          } // end PNR group loop
        } // end if passengers exist

        // Add trip reconciliation - ALWAYS add even if no passengers
        tripReconciliations.push({
          route_id: routeId,
          route_num: tripData.route_num,
          origin: tripData.origin,
          destination: tripData.destination,
          travel_date: tripData.travel_date,
          departure_time: tripData.route_departure_time,
          arrival_time: tripData.route_arrival_time,
          available_seats: tripData.available_seats,
          booked_seats: tripData.passenger_details?.length || 0,
          blocked_seats: tripData.blocked_count || 0,
          phone_booking_count: tripData.phone_booking_count || 0,
          total_seats: (tripData.passenger_details?.length || 0) + (tripData.available_seats || 0) + (tripData.blocked_count || 0),
          passenger_count: tripData.passenger_details?.length || 0,
          status: tripStatus,
          passengers: passengerReconciliations,
          raw_data: tripData,
        })
        } // end for tripData
      } catch (routeError: any) {
        console.error(`Error processing route ${route.id}:`, routeError)
        errors.push(`Route ${route.id}: ${routeError.message}`)
      }
    } // end for route

    const latency = Date.now() - startTime

    // Update sync job as completed
    if (syncJobId) {
      await supabase
        .from('jc_sync_jobs')
        .update({
          status: 'completed',
          completed_at: new Date().toISOString(),
          records_fetched: totalPassengers,
          records_created: bookingsInserted + passengersInserted,
          records_updated: bookingsUpdated + passengersUpdated,
          records_failed: errors.length,
          details: {
            routes_total: activeRoutes.length,
            routes_active: activeRoutes.length,
            routes_synced: routesSynced,
            routes_updated: routesUpdated,
            routes_not_modified: routesNotModified,
            trips_processed: totalTrips,
            bookings_inserted: bookingsInserted,
            bookings_updated: bookingsUpdated,
            bookings_not_modified: bookingsNotModified,
            passengers_inserted: passengersInserted,
            passengers_updated: passengersUpdated,
            passengers_not_modified: passengersNotModified,
            errors: errors.length > 0 ? errors : undefined,
            latency_ms: latency,
          }
        })
        .eq('id', syncJobId)
    }

    // Update cron sync job if triggered by cron
    if (cronSyncJobId) {
      const completedAt = new Date().toISOString()
      await supabase
        .from('jc_sync_jobs')
        .update({
          status: errors.length > 0 ? 'completed_with_errors' : 'completed',
          completed_at: completedAt,
          metadata: {
            trigger: 'cron-job.org',
            target_date: targetDate,
            summary: {
              routes_synced: routesSynced,
              routes_updated: routesUpdated,
              trips_processed: totalTrips,
              bookings_total: totalBookings,
              passengers_total: totalPassengers,
              errors_count: errors.length,
            }
          }
        })
        .eq('id', cronSyncJobId)

      console.log(`[Pull Data] Updated cron sync job ${cronSyncJobId} to completed`)
    }

    return NextResponse.json({
      status: 'success',
      message: batchNumber
        ? `Batch ${batchNumber}/${totalBatches} completed successfully`
        : 'Data pull completed successfully',
      batch_info: batchNumber ? {
        current_batch: batchNumber,
        total_batches: totalBatches,
        batch_size: batchSize,
        routes_in_batch: activeRoutes.length,
      } : undefined,
      summary: {
        routes: {
          total: activeRoutes.length,
          active: activeRoutes.length,
          synced: routesSynced,
          updated: routesUpdated,
          not_modified: routesNotModified,
        },
        trips: {
          processed: totalTrips,
        },
        bookings: {
          total: totalBookings,
          inserted: bookingsInserted,
          updated: bookingsUpdated,
          not_modified: bookingsNotModified,
        },
        passengers: {
          total: totalPassengers,
          inserted: passengersInserted,
          updated: passengersUpdated,
          not_modified: passengersNotModified,
        },
        errors: errors.length > 0 ? errors : undefined,
      },
      // Raw data and reconciliation details
      raw_data: {
        routes: routeReconciliations,
        trips: tripReconciliations,
      },
      sync_job_id: syncJobId,
      latency_ms: latency,
    })
  } catch (error: any) {
    console.error('Pull data error:', error)

    // Update sync job as failed
    if (syncJobId) {
      await supabase
        .from('jc_sync_jobs')
        .update({
          status: 'failed',
          completed_at: new Date().toISOString(),
          error_message: error.message,
        })
        .eq('id', syncJobId)
    }

    return NextResponse.json(
      { status: 'error', message: error.message },
      { status: 500 }
    )
  }
}

// GET - Fetch routes or passenger details (for preview/testing)
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const action = searchParams.get('action')
    const routeId = searchParams.get('route_id')
    const date = searchParams.get('date')

    const apiKey = await getApiKey()

    if (action === 'routes') {
      // Fetch all routes
      const routesUrl = `${BITLA_API_BASE}/all_routes.json?api_key=${apiKey}`
      const response = await fetch(routesUrl)

      if (!response.ok) {
        throw new Error('Failed to fetch routes from Bitla API')
      }

      const routes = await response.json()

      // Filter to show only active routes by default
      const activeRoutes = routes.filter((r: any) => r.route_status === 'Active')

      return NextResponse.json({
        status: 'success',
        total: routes.length,
        active_count: activeRoutes.length,
        routes: routes,
      })
    }

    if (action === 'passengers') {
      // Validate required parameters
      if (!routeId || !date) {
        return NextResponse.json(
          { status: 'error', message: 'route_id and date are required' },
          { status: 400 }
        )
      }

      // Fetch passenger details
      // Date format should be YYYY-MM-DD
      const passengersUrl = `${BITLA_API_BASE}/get_passenger_details/${date}.json?api_key=${apiKey}&route_id=${routeId}`
      const response = await fetch(passengersUrl)

      if (!response.ok) {
        throw new Error('Failed to fetch passenger details from Bitla API')
      }

      const data = await response.json()

      // Handle empty response or error
      if (!data || (Array.isArray(data) && data.length === 0)) {
        return NextResponse.json({
          status: 'success',
          message: 'No trips found for this route and date',
          trips: [],
        })
      }

      return NextResponse.json({
        status: 'success',
        trip_count: Array.isArray(data) ? data.length : 1,
        data: data,
      })
    }

    return NextResponse.json(
      { status: 'error', message: 'Invalid action. Use action=routes or action=passengers' },
      { status: 400 }
    )
  } catch (error: any) {
    console.error('Pull data GET error:', error)

    return NextResponse.json(
      { status: 'error', message: error.message },
      { status: 500 }
    )
  }
}
