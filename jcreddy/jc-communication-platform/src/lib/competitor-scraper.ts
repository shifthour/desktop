/**
 * Competitor Fare Scraper
 *
 * Scrapes fare data from:
 * 1. mythribus.com (TicketSimply platform) - q2 for trips
 * 2. vikramtravels.com (EzeeBus platform) - /search/search-list for trips
 * 3. vkaveribus.com (TicketSimply platform) - /api/available_routes for trips
 *
 * All APIs are open (no auth needed), no bot protection.
 */

// ─── Types ──────────────────────────────────────────────────────────────────

export type OperatorName = 'mythri' | 'vikram' | 'vkaveri'

export interface RouteConfig {
  id: string
  route_name: string
  route_from: string
  route_to: string
  mythri_from_id: string
  mythri_to_id: string
  vikram_from_code: string
  vikram_to_code: string
  vkaveri_from_id: string
  vkaveri_to_id: string
}

export interface SeatTypeFare {
  seat_type: string
  seat_name: string
  fare: number
  available_count: number
}

export interface SeatFare {
  seat_number: string
  fare: number
  seat_type: string
  is_available: boolean
  is_window: boolean
  is_single: boolean
}

export interface ScrapedTrip {
  operator: OperatorName
  route_from: string
  route_to: string
  travel_date: string
  service_number: string
  bus_type: string
  departure_time: string | null
  arrival_time: string | null
  trip_identifier: string
  total_seats: number
  available_seats: number
  min_fare: number
  max_fare: number
  avg_fare: number
  fare_by_seat_type: SeatTypeFare[]
  seat_fares: SeatFare[]
}

export interface FareComparison {
  route_from: string
  route_to: string
  travel_date: string
  competitor_operator: OperatorName
  mythri: ScrapedTrip
  competitor: ScrapedTrip
  fare_difference: number
  recommended_mythri_fare: number
  undercut_amount: number
  savings_percent: number
  seat_type_comparison: Array<{
    seat_type: string
    mythri_fare: number
    competitor_fare: number
    difference: number
    recommended: number
  }>
}

// ─── Seat type name map (shared across TicketSimply operators) ───────────────

const SEAT_TYPE_NAMES: Record<string, string> = {
  SLB: 'Single Lower Berth',
  SUB: 'Single Upper Berth',
  DLB: 'Double Lower Berth',
  DUB: 'Double Upper Berth',
  SS: 'Seater',
  SSW: 'Seater Window',
  SLSL: 'Single Lower Sleeper',
  LSL: 'Lower Sleeper',
  USL: 'Upper Sleeper',
  SUSL: 'Single Upper Sleeper',
  LB: 'Lower Berth',
  UB: 'Upper Berth',
}

// ─── Mythri Scraper (TicketSimply) ──────────────────────────────────────────

const MYTHRI_API = 'https://www.mythribus.com/api/cms_booking_engine.json'

export async function scrapeMythriTrips(
  fromId: string,
  toId: string,
  travelDate: string,
  fromCity: string,
  toCity: string
): Promise<ScrapedTrip[]> {
  const trips: ScrapedTrip[] = []

  try {
    const searchRes = await fetch(MYTHRI_API, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: 'q2',
        from: fromId,
        to: toId,
        depart: travelDate,
        triptype: 'oneway',
        current_page: 1,
        per_page: 50,
      }),
    })

    if (!searchRes.ok) {
      console.error(`[Mythri Scraper] Search failed: ${searchRes.status}`)
      return trips
    }

    const searchData = await searchRes.json()
    const reservations = searchData.reservations || []

    for (const res of reservations) {
      const resId = res.res_id
      if (!resId) continue

      const fareByType: SeatTypeFare[] = []
      const fc = res.fare_content || {}

      for (const [seatType, fares] of Object.entries(fc)) {
        const adultFares = (fares as any)?.adult_fares
        if (adultFares?.fare) {
          fareByType.push({
            seat_type: seatType,
            seat_name: SEAT_TYPE_NAMES[seatType] || seatType,
            fare: parseFloat(adultFares.fare),
            available_count: 0,
          })
        }
      }

      const seatFares: SeatFare[] = []
      const typeFares = fareByType.filter((f) => f.fare > 0).map((f) => f.fare)
      const minFare = typeFares.length > 0 ? Math.min(...typeFares) : res.fare_amt || 0
      const maxFare = typeFares.length > 0 ? Math.max(...typeFares) : res.fare_amt || 0
      const avgFare =
        typeFares.length > 0
          ? Math.round((typeFares.reduce((a, b) => a + b, 0) / typeFares.length) * 100) / 100
          : res.fare_amt || 0

      let departureTime: string | null = null
      let arrivalTime: string | null = null
      if (res.dep_time) {
        departureTime = new Date(res.dep_time).toISOString()
      }
      if (res.arr_date_word && res.arr_date_num) {
        try {
          const arrStr = `${res.arr_date_word} 2026 ${res.arr_date_num}`
          const parsed = new Date(arrStr)
          if (!isNaN(parsed.getTime())) {
            arrivalTime = parsed.toISOString()
          }
        } catch {}
      }

      trips.push({
        operator: 'mythri',
        route_from: fromCity,
        route_to: toCity,
        travel_date: travelDate,
        service_number: res.route_number || '',
        bus_type: res.coach_name || '',
        departure_time: departureTime,
        arrival_time: arrivalTime,
        trip_identifier: String(resId),
        total_seats: 36,
        available_seats: res.available_seats || 0,
        min_fare: minFare,
        max_fare: maxFare,
        avg_fare: avgFare,
        fare_by_seat_type: fareByType,
        seat_fares: seatFares,
      })
    }
  } catch (err: any) {
    console.error(`[Mythri Scraper] Error: ${err.message}`)
  }

  return trips
}

// ─── Vikram Scraper (EzeeBus) ───────────────────────────────────────────────

const VIKRAM_API = 'https://www.vikramtravels.com/search/search-list'

export async function scrapeVikramTrips(
  fromCode: string,
  toCode: string,
  travelDate: string,
  fromCity: string,
  toCity: string
): Promise<ScrapedTrip[]> {
  const trips: ScrapedTrip[] = []

  try {
    const res = await fetch(VIKRAM_API, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'X-Requested-With': 'XMLHttpRequest',
        Referer: `https://www.vikramtravels.com/search/${fromCity.toLowerCase()}-to-${toCity.toLowerCase()}`,
      },
      body: new URLSearchParams({
        fromStationCode: fromCode,
        toStationCode: toCode,
        onwardDate: travelDate,
        returnDate: '',
        searchType: '',
        reschedule: '0',
      }).toString(),
    })

    if (!res.ok) {
      console.error(`[Vikram Scraper] Search failed: ${res.status}`)
      return trips
    }

    const data = await res.json()
    if (data.status !== 1) {
      console.error(`[Vikram Scraper] API returned status: ${data.status}`)
      return trips
    }

    const regGroups = data.data?.reg || []

    for (const group of regGroups) {
      const tripList = group.triplist || []

      for (const trip of tripList) {
        const schedule = trip.schedule || {}
        const bus = trip.bus || {}
        const fromStation = trip.fromStation || {}
        const toStation = trip.toStation || {}
        const computed = trip.computed || {}
        const stageFares = trip.stageFare || []

        const fareByType: SeatTypeFare[] = stageFares.map((f: any) => ({
          seat_type: f.seatType || '',
          seat_name: f.seatName || '',
          fare: f.fare || 0,
          available_count: f.availableSeatCount || 0,
        }))

        const stageFareValues = stageFares.filter((f: any) => f.fare > 0).map((f: any) => f.fare as number)
        const minFare = stageFareValues.length > 0 ? Math.min(...stageFareValues) : 0
        const maxFare = stageFareValues.length > 0 ? Math.max(...stageFareValues) : 0
        const avgFare = stageFareValues.length > 0
          ? Math.round((stageFareValues.reduce((a: number, b: number) => a + b, 0) / stageFareValues.length) * 100) / 100
          : 0

        const seatFares: SeatFare[] = []

        // Vikram API returns IST times without timezone info
        const parseIST = (dt: string | null) => {
          if (!dt) return null
          const cleaned = dt.trim().replace(' ', 'T')
          if (!cleaned.includes('+') && !cleaned.includes('Z')) {
            return new Date(cleaned + '+05:30').toISOString()
          }
          return new Date(cleaned).toISOString()
        }

        trips.push({
          operator: 'vikram',
          route_from: fromCity,
          route_to: toCity,
          travel_date: travelDate,
          service_number: schedule.serviceNumber || '',
          bus_type: bus.busType || '',
          departure_time: parseIST(fromStation.dateTime),
          arrival_time: parseIST(toStation.dateTime),
          trip_identifier: trip.tripCode || '',
          total_seats: bus.totalSeatCount || 40,
          available_seats: computed.availableSeatCount ?? 0,
          min_fare: minFare,
          max_fare: maxFare,
          avg_fare: avgFare,
          fare_by_seat_type: fareByType,
          seat_fares: seatFares,
        })
      }
    }
  } catch (err: any) {
    console.error(`[Vikram Scraper] Error: ${err.message}`)
  }

  return trips
}

// ─── VKaveri Scraper (TicketSimply) ─────────────────────────────────────────

const VKAVERI_API = 'https://www.vkaveribus.com/api/available_routes'

export async function scrapeVkaveriTrips(
  fromId: string,
  toId: string,
  travelDate: string,
  fromCity: string,
  toCity: string
): Promise<ScrapedTrip[]> {
  const trips: ScrapedTrip[] = []

  try {
    const url = `${VKAVERI_API}/${fromId}/${toId}/${travelDate}.json?show_only_available_services=false&show_injourney_services=true`
    const res = await fetch(url)

    if (!res.ok) {
      console.error(`[VKaveri Scraper] Search failed: ${res.status}`)
      return trips
    }

    const services = await res.json()
    if (!Array.isArray(services)) {
      console.error(`[VKaveri Scraper] Unexpected response format`)
      return trips
    }

    for (const svc of services) {
      // Parse departure time: dep_time "07:15 PM", dep_date "13/02/2026"
      let departureTime: string | null = null
      let arrivalTime: string | null = null

      if (svc.dep_time && svc.dep_date) {
        try {
          // dep_date is DD/MM/YYYY, dep_time is "HH:MM AM/PM"
          const [dd, mm, yyyy] = svc.dep_date.split('/')
          const dateStr = `${yyyy}-${mm}-${dd}`
          const timeParts = svc.dep_time.match(/(\d+):(\d+)\s*(AM|PM)/i)
          if (timeParts) {
            let h = parseInt(timeParts[1])
            const m = parseInt(timeParts[2])
            const ampm = timeParts[3].toUpperCase()
            if (ampm === 'PM' && h < 12) h += 12
            if (ampm === 'AM' && h === 12) h = 0
            departureTime = new Date(`${dateStr}T${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:00+05:30`).toISOString()
          }
        } catch {}
      }

      if (svc.arr_time && svc.arr_date) {
        try {
          const [dd, mm, yyyy] = svc.arr_date.split('/')
          const dateStr = `${yyyy}-${mm}-${dd}`
          const timeParts = svc.arr_time.match(/(\d+):(\d+)\s*(AM|PM)/i)
          if (timeParts) {
            let h = parseInt(timeParts[1])
            const m = parseInt(timeParts[2])
            const ampm = timeParts[3].toUpperCase()
            if (ampm === 'PM' && h < 12) h += 12
            if (ampm === 'AM' && h === 12) h = 0
            arrivalTime = new Date(`${dateStr}T${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:00+05:30`).toISOString()
          }
        } catch {}
      }

      // Parse fare — use base fare (fare_str), not discounted
      const baseFare = parseFloat(svc.fare_str || svc.fare_string || '0')

      // Parse seat_type_availability: "LB:12, UB:12, SLB:4, SUB:4"
      const fareByType: SeatTypeFare[] = []
      const seatAvail = svc.seat_type_availability || ''
      if (seatAvail) {
        const parts = seatAvail.split(',').map((s: string) => s.trim())
        for (const part of parts) {
          const [type, countStr] = part.split(':')
          if (type && countStr) {
            fareByType.push({
              seat_type: type.trim(),
              seat_name: SEAT_TYPE_NAMES[type.trim()] || type.trim(),
              fare: baseFare, // VKaveri has uniform fare across seat types
              available_count: parseInt(countStr.trim()) || 0,
            })
          }
        }
      }

      trips.push({
        operator: 'vkaveri',
        route_from: fromCity,
        route_to: toCity,
        travel_date: travelDate,
        service_number: svc.number || '',
        bus_type: svc.bus_type || '',
        departure_time: departureTime,
        arrival_time: arrivalTime,
        trip_identifier: String(svc.reservation_id || svc.id || ''),
        total_seats: svc.total_seats || 36,
        available_seats: svc.available_seats || 0,
        min_fare: baseFare,
        max_fare: baseFare,
        avg_fare: baseFare,
        fare_by_seat_type: fareByType,
        seat_fares: [],
      })
    }
  } catch (err: any) {
    console.error(`[VKaveri Scraper] Error: ${err.message}`)
  }

  return trips
}

// ─── Trip Matching & Comparison ─────────────────────────────────────────────

/**
 * Match Mythri trips with competitor trips by departure time proximity (within 1.5 hours)
 * and generate fare comparison recommendations.
 */
export function compareTrips(
  mythriTrips: ScrapedTrip[],
  competitorTrips: ScrapedTrip[],
  competitorOperator: OperatorName,
  undercutAmount: number = 30
): FareComparison[] {
  const comparisons: FareComparison[] = []

  for (const mythri of mythriTrips) {
    let bestMatch: ScrapedTrip | null = null
    let bestTimeDiff = Infinity

    for (const comp of competitorTrips) {
      if (!mythri.departure_time || !comp.departure_time) continue

      const mythriDep = new Date(mythri.departure_time).getTime()
      const compDep = new Date(comp.departure_time).getTime()
      const timeDiff = Math.abs(mythriDep - compDep) / (1000 * 60 * 60)

      if (timeDiff <= 1.5 && timeDiff < bestTimeDiff) {
        bestTimeDiff = timeDiff
        bestMatch = comp
      }
    }

    if (!bestMatch) continue

    const fareDiff = mythri.min_fare - bestMatch.min_fare

    const recommendedFare = fareDiff > 0
      ? Math.max(bestMatch.min_fare - undercutAmount, 0)
      : mythri.min_fare
    const savingsPercent =
      mythri.min_fare > 0 && fareDiff > 0
        ? Math.round(((mythri.min_fare - recommendedFare) / mythri.min_fare) * 10000) / 100
        : 0

    const seatTypeComparison: FareComparison['seat_type_comparison'] = []
    for (const mType of mythri.fare_by_seat_type) {
      const cMatch = bestMatch.fare_by_seat_type.find(
        (v) =>
          v.seat_type === mType.seat_type ||
          v.seat_name.toLowerCase().includes(mType.seat_name.toLowerCase().split(' ')[0])
      )

      if (cMatch) {
        const diff = mType.fare - cMatch.fare
        seatTypeComparison.push({
          seat_type: mType.seat_name || mType.seat_type,
          mythri_fare: mType.fare,
          competitor_fare: cMatch.fare,
          difference: diff,
          recommended: diff > 0 ? Math.max(cMatch.fare - undercutAmount, 0) : mType.fare,
        })
      } else {
        seatTypeComparison.push({
          seat_type: mType.seat_name || mType.seat_type,
          mythri_fare: mType.fare,
          competitor_fare: 0,
          difference: 0,
          recommended: mType.fare,
        })
      }
    }

    comparisons.push({
      route_from: mythri.route_from,
      route_to: mythri.route_to,
      travel_date: mythri.travel_date,
      competitor_operator: competitorOperator,
      mythri,
      competitor: bestMatch,
      fare_difference: fareDiff,
      recommended_mythri_fare: recommendedFare,
      undercut_amount: undercutAmount,
      savings_percent: savingsPercent,
      seat_type_comparison: seatTypeComparison,
    })
  }

  return comparisons
}

// ─── Date Helpers ───────────────────────────────────────────────────────────

/** Get dates for next N days from today (IST) */
export function getUpcomingDates(days: number): string[] {
  const dates: string[] = []
  const now = new Date()
  const istOffset = 5.5 * 60 * 60 * 1000
  const istNow = new Date(now.getTime() + istOffset)

  for (let i = 0; i < days; i++) {
    const d = new Date(istNow)
    d.setDate(d.getDate() + i)
    dates.push(d.toISOString().split('T')[0])
  }
  return dates
}
