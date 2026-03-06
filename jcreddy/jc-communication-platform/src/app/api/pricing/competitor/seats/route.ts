import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

const MYTHRI_API = 'https://www.mythribus.com/api/cms_booking_engine.json'
const VKAVERI_API = 'https://www.vkaveribus.com/api/cms_booking_engine.json'
const VIKRAM_BUSMAP_API = 'https://www.vikramtravels.com/search/busmap'

/**
 * GET /api/pricing/competitor/seats
 * Fetch seat layouts on-demand from all operators.
 *
 * Query params:
 *   ?mythri_id=28666              Mythri reservation ID (q3)
 *   ?operator=vkaveri|vikram      Competitor operator name
 *   ?competitor_id=564494         Competitor reservation/trip ID
 *   ?route_from=Bangalore         Route origin (for Vikram station code lookup)
 *   ?route_to=Warangal            Route destination
 *   ?travel_date=2026-02-18       Travel date (for Vikram API)
 */
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const mythriId = searchParams.get('mythri_id')
    const operator = searchParams.get('operator') || ''
    const competitorId = searchParams.get('competitor_id')
    const routeFrom = searchParams.get('route_from') || ''
    const routeTo = searchParams.get('route_to') || ''
    const travelDate = searchParams.get('travel_date') || ''

    const promises: Promise<any>[] = []

    // Mythri layout (always fetch if we have an ID)
    if (mythriId) {
      promises.push(fetchTicketSimplyLayout(MYTHRI_API, mythriId))
    } else {
      promises.push(Promise.resolve(null))
    }

    // Competitor layout
    if (operator === 'vkaveri' && competitorId) {
      promises.push(fetchTicketSimplyLayout(VKAVERI_API, competitorId))
    } else if (operator === 'vikram' && competitorId) {
      promises.push(fetchVikramLayout(competitorId, routeFrom, routeTo, travelDate))
    } else {
      promises.push(Promise.resolve(null))
    }

    const [mythriLayout, competitorLayout] = await Promise.all(promises)

    return NextResponse.json({
      success: true,
      mythri_layout: mythriLayout,
      competitor_layout: competitorLayout,
      competitor_operator: operator,
    })
  } catch (error: any) {
    console.error('[Seats API] Error:', error.message)
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    )
  }
}

async function fetchTicketSimplyLayout(apiUrl: string, resId: string): Promise<any[][] | null> {
  try {
    const res = await fetch(apiUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: 'q3', id: parseInt(resId, 10) }),
    })

    if (!res.ok) {
      console.error(`[Seats API] TicketSimply q3 failed: ${res.status}`)
      return null
    }

    const data = await res.json()
    const layout = data.coach_layout

    if (!layout || !Array.isArray(layout)) {
      console.error('[Seats API] No coach_layout in q3 response')
      return null
    }

    return layout
  } catch (err: any) {
    console.error(`[Seats API] TicketSimply error: ${err.message}`)
    return null
  }
}

/**
 * Fetch Vikram (EzeeBus) seat layout and normalize to TicketSimply coach_layout format.
 * POST https://www.vikramtravels.com/search/busmap
 */
async function fetchVikramLayout(
  tripCode: string,
  routeFrom: string,
  routeTo: string,
  travelDate: string
): Promise<any[][] | null> {
  try {
    // Look up Vikram station codes from route config
    const supabase = createServerClient()
    const { data: routeConfig } = await supabase
      .from('jc_competitor_route_config')
      .select('vikram_from_code, vikram_to_code')
      .eq('route_from', routeFrom)
      .eq('route_to', routeTo)
      .eq('is_active', true)
      .limit(1)
      .single()

    if (!routeConfig?.vikram_from_code || !routeConfig?.vikram_to_code) {
      console.error('[Seats API] No Vikram station codes found for route')
      return null
    }

    const res = await fetch(VIKRAM_BUSMAP_API, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0',
        'Origin': 'https://www.vikramtravels.com',
      },
      body: JSON.stringify({
        tripCode,
        fromStationCode: routeConfig.vikram_from_code,
        toStationCode: routeConfig.vikram_to_code,
        travelDate,
      }),
    })

    if (!res.ok) {
      console.error(`[Seats API] Vikram busmap failed: ${res.status}`)
      return null
    }

    const json = await res.json()
    const data = json.data
    if (!data || !data.seatStatus) {
      console.error('[Seats API] No seatStatus in Vikram response')
      return null
    }

    return normalizeVikramLayout(data)
  } catch (err: any) {
    console.error(`[Seats API] Vikram error: ${err.message}`)
    return null
  }
}

/**
 * Convert Vikram's seatStatus + matrix dimensions into TicketSimply-compatible coach_layout.
 *
 * Vikram key pattern: {deck}{row}{col}
 *   deck: 1=lower, 2=upper
 *   row: 1,2=double seats, 4=single seat (gangway side)
 *   col: 1-N from front to back
 *
 * Output: TicketSimply-compatible 2D array where each row = one bus position front-to-back:
 *   [UpperSingle(t13)] [LowerSingle(t12)] [GW] [LowerDouble1(t17)] [LowerDouble2(t17)] [UpperDouble1(t18)] [UpperDouble2(t18)]
 */
function normalizeVikramLayout(data: any): any[][] {
  const seatStatus: Record<string, any> = data.seatStatus || {}
  const lmatrix = data.lmatrix || {}
  const umatrix = data.umatrix || {}

  const maxCol = Math.max(lmatrix.colmax || 0, umatrix.colmax || 0, 7)
  const layout: any[][] = []

  for (let col = 1; col <= maxCol; col++) {
    const row: any[] = []

    // Upper single (row 4, deck 2) → seat_type_value 13 (SUB)
    row.push(vikramSeatToCell(seatStatus[`24${col}`], 13))
    // Lower single (row 4, deck 1) → seat_type_value 12 (SLB)
    row.push(vikramSeatToCell(seatStatus[`14${col}`], 12))
    // Gangway
    row.push({ is_ganway_col: true, is_renderable: true, seat_number: '' })
    // Lower double 1 (row 2, deck 1) → seat_type_value 17 (DLB)
    row.push(vikramSeatToCell(seatStatus[`12${col}`], 17))
    // Lower double 2 (row 1, deck 1) → seat_type_value 17 (DLB)
    row.push(vikramSeatToCell(seatStatus[`11${col}`], 17))
    // Upper double 1 (row 2, deck 2) → seat_type_value 18 (DUB)
    row.push(vikramSeatToCell(seatStatus[`22${col}`], 18))
    // Upper double 2 (row 1, deck 2) → seat_type_value 18 (DUB)
    row.push(vikramSeatToCell(seatStatus[`21${col}`], 18))

    layout.push(row)

    // Separator row (skip for last column)
    if (col < maxCol) {
      layout.push([null, null, { is_ganway_col: true, is_renderable: true, seat_number: '' }])
    }
  }

  return layout
}

/** Convert a single Vikram seat to TicketSimply-compatible cell */
function vikramSeatToCell(seat: any, seatTypeValue: number): any {
  if (!seat) {
    return { seat_number: '', is_renderable: false, is_ganway_col: false, seat_type_value: seatTypeValue }
  }

  const statusCode = seat.status?.code || ''
  const genderCode = seat.genderstatus?.code || seat.genderstatus || ''

  // Map Vikram status to TicketSimply td_class
  let tdClass = ''
  if (statusCode === 'AL' || statusCode === 'AM' || statusCode === 'AF') {
    tdClass = ' class="available_seat layout_ecell center hand_cursor availableSleeper "'
  } else if (statusCode === 'BO' && (genderCode === 'F' || genderCode === 'AF')) {
    tdClass = ' class="booked_by_ladies_seat ladiesSleeper layout_ecell center "'
  } else {
    tdClass = ' class="e_ticketing_seat reservedSleeper layout_ecell center hand_cursor"'
  }

  const fare = (statusCode === 'AL' || statusCode === 'AM' || statusCode === 'AF')
    ? (seat.fare || 0)
    : 0

  return {
    seat_number: seat.seatName || '',
    is_renderable: true,
    is_seat_reservable: statusCode === 'AL' || statusCode === 'AM' || statusCode === 'AF',
    is_ganway_col: false,
    seat_type_value: seatTypeValue,
    fare,
    td_class: tdClass,
    is_window_seat: false,
    is_single_seat: seatTypeValue === 12 || seatTypeValue === 13,
  }
}
