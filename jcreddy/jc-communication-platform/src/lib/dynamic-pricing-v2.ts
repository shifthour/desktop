/**
 * Dynamic Pricing Engine v2
 * 7-factor additive pricing model for Mythri bus services.
 *
 * Factors:
 *  1. Cost Floor        — enforces minimum ₹428/seat to meet ₹40K/day target
 *  2. Time Curve        — S-curve based on hours to departure
 *  3. Velocity          — booking acceleration / deceleration
 *  4. Competitor React.  — reactive to Vikram / VKaveri fares
 *  5. Day-of-Week       — direction-aware demand patterns
 *  6. Event / Festival  — surge pricing for holidays & long weekends
 *  7. Seat Scarcity     — last-seat premium / low-fill stimulus
 *
 * Pure computation — no database or side effects.
 */

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

export type Direction = 'BLR_WGL' | 'WGL_BLR'
export type Urgency = 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW'
export type CompetitorPosition = 'CHEAPEST' | 'COMPETITIVE' | 'PREMIUM' | 'OVERPRICED'
export type EventImpact = 'SURGE' | 'HIGH' | 'NORMAL'

export interface CompetitorFareInput {
  operator: string
  min_fare: number
  avg_fare: number
  available_seats: number
  bus_type?: string
  is_volvo?: boolean  // true if this is a Volvo/premium bus (higher fare class)
}

export interface DynamicPricingInput {
  // Trip context
  travel_date: string            // YYYY-MM-DD
  direction: Direction
  service_number: string
  departure_time: string         // HH:MM
  bus_type?: string              // e.g. "2+1, BRAND NEW ... Sleeper, AC, Non-Volvo"
  is_volvo?: boolean             // true if Mythri's own bus is Volvo

  // Current state
  base_fare: number              // route base_fare from jc_routes
  current_occupancy: number      // 0-100
  booked_seats: number
  total_seats: number
  available_seats: number

  // Velocity
  bookings_last_1h: number
  bookings_last_6h: number
  bookings_last_24h: number
  cancellations_last_24h: number
  hours_to_departure: number

  // Competitor data
  competitor_fares: CompetitorFareInput[]

  // Calendar
  day_of_week: number            // 0=Sun … 6=Sat
  is_event: boolean
  event_name?: string
  event_impact?: EventImpact
  is_long_weekend: boolean
}

export interface PricingFactor {
  name: string
  adjustment: number             // ₹ delta (positive = raise, negative = lower)
  weight: number                 // 0-1
  explanation: string
}

export interface DynamicPricingResult {
  recommended_fare: number
  floor_fare: number
  ceiling_fare: number
  factors: PricingFactor[]
  demand_score: number           // 0-100
  urgency: Urgency
  position_vs_competitors: CompetitorPosition
  revenue_at_current: number
  revenue_at_recommended: number
  revenue_delta: number
  confidence: number             // 0-100
}

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Revenue target: ₹40,000 minimum per BUS per ONE-WAY ROUTE.
 * This is NOT per day across all services — it's per single trip.
 *
 * Floor fare = ₹40,000 ÷ 36 seats ÷ 85% expected fill = ₹1,308/seat
 * This ensures that even at 85% occupancy, one bus earns ₹40K.
 */
export const BUS_MINIMUM_REVENUE = 40_000
export const SEATS_PER_SERVICE = 36
export const TARGET_FILL_RATE = 0.85
export const FLOOR_FARE = Math.ceil(
  BUS_MINIMUM_REVENUE / (SEATS_PER_SERVICE * TARGET_FILL_RATE)
) // ₹1,308

// Keep these for display in UI
export const DAILY_MINIMUM = BUS_MINIMUM_REVENUE
export const SERVICES_PER_DAY = 1 // per-bus calculation

export const MAX_MARKUP_RATIO = 1.50 // 50% ceiling above base (tighter since base is higher)

/**
 * Direction-aware day-of-week demand multipliers.
 * Index: 0 = Sunday … 6 = Saturday.
 * BLR→WGL peaks Thu(4) / Fri(5) — workers / students heading home.
 * WGL→BLR peaks Sun(0) / Mon(1) — returning to Bangalore.
 */
export const DAY_PATTERNS: Record<Direction, Record<number, number>> = {
  BLR_WGL: { 0: 0.90, 1: 0.85, 2: 0.85, 3: 0.95, 4: 1.15, 5: 1.20, 6: 1.00 },
  WGL_BLR: { 0: 1.20, 1: 1.15, 2: 0.85, 3: 0.85, 4: 0.90, 5: 0.95, 6: 1.00 },
}

/**
 * Festival / event calendar (Feb 26 – Mar 27, 2026).
 * Keyed by YYYY-MM-DD.
 */
export interface CalendarEvent {
  emoji: string
  name: string
  shortName: string
  impact: EventImpact
}

export const CALENDAR_EVENTS: Record<string, CalendarEvent> = {
  '2026-03-03': { emoji: '🔥', name: 'Holika Dahana', shortName: 'HOLIKA', impact: 'HIGH' },
  '2026-03-04': { emoji: '🎨', name: 'Holi', shortName: 'HOLI', impact: 'SURGE' },
  '2026-03-19': { emoji: '🌙', name: 'Ugadi (Telugu New Year)', shortName: 'UGADI', impact: 'SURGE' },
  '2026-03-20': { emoji: '🕌', name: 'Jamat Ul-Vida', shortName: 'JAMAT', impact: 'HIGH' },
  '2026-03-21': { emoji: '🕌', name: 'Eid al-Fitr (Ramzan)', shortName: 'EID', impact: 'HIGH' },
  '2026-03-26': { emoji: '🛕', name: 'Rama Navami', shortName: 'NAVAMI', impact: 'HIGH' },
}

export interface LongWeekend {
  name: string
  start: string
  end: string
}

export const LONG_WEEKENDS: LongWeekend[] = [
  { name: 'Holi Long Weekend', start: '2026-03-03', end: '2026-03-08' },
  { name: 'Ugadi + Eid Long Weekend', start: '2026-03-19', end: '2026-03-22' },
  { name: 'Rama Navami Long Weekend', start: '2026-03-26', end: '2026-03-29' },
]

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

export function isInLongWeekend(dateStr: string): LongWeekend | null {
  for (const lw of LONG_WEEKENDS) {
    if (dateStr >= lw.start && dateStr <= lw.end) return lw
  }
  return null
}

export function getEventForDate(dateStr: string): CalendarEvent | null {
  return CALENDAR_EVENTS[dateStr] || null
}

export function detectDirection(routeFrom: string, routeTo: string): Direction {
  const from = (routeFrom || '').toLowerCase()
  const to = (routeTo || '').toLowerCase()
  if (from.includes('bangalore') || from.includes('bengaluru') || from.includes('blr')) {
    return 'BLR_WGL'
  }
  if (to.includes('bangalore') || to.includes('bengaluru') || to.includes('blr')) {
    return 'WGL_BLR'
  }
  return 'BLR_WGL' // default
}

// ─────────────────────────────────────────────────────────────────────────────
// Factor 1 — Cost Floor
// ─────────────────────────────────────────────────────────────────────────────

function calculateFloorFactor(input: DynamicPricingInput): PricingFactor {
  const gap = FLOOR_FARE - input.base_fare

  // Check if same-class competitors exist and are BELOW the floor
  const mythriIsVolvo = input.is_volvo || isVolvoOrPremium(input.bus_type)
  const sameClassComps = mythriIsVolvo
    ? input.competitor_fares
    : input.competitor_fares.filter((c) => !c.is_volvo && !isVolvoOrPremium(c.bus_type))
  const cheapestSameClass = sameClassComps.length > 0
    ? Math.min(...sameClassComps.map((c) => c.min_fare))
    : null

  if (gap > 0) {
    // Floor fare is above current base fare
    if (cheapestSameClass !== null && cheapestSameClass < FLOOR_FARE) {
      // Market reality: competitors are below our floor target.
      // An empty bus at ₹1308 earns ₹0. A full bus at ₹830 earns ₹25K+.
      // Use competitive pricing instead of enforcing the unachievable floor.
      const competitiveTarget = cheapestSameClass - 30 // undercut same-class by ₹30
      const actualGap = Math.max(0, competitiveTarget - input.base_fare)
      return {
        name: 'Cost Floor',
        adjustment: actualGap,
        weight: 1.0,
        explanation: `Floor target ₹${FLOOR_FARE}/seat (₹${BUS_MINIMUM_REVENUE.toLocaleString()} per bus) but same-class competitor at ₹${cheapestSameClass}. Pricing competitively at ₹${competitiveTarget} (undercut ₹30). ⚠️ Below ₹40K target — fill rate must compensate.`,
      }
    }

    // No same-class competitors below floor, or no competitors — enforce floor normally
    return {
      name: 'Cost Floor',
      adjustment: gap,
      weight: 1.0,
      explanation: `Base fare ₹${input.base_fare} is below ₹${FLOOR_FARE} floor (₹${BUS_MINIMUM_REVENUE.toLocaleString()} min per bus ÷ ${SEATS_PER_SERVICE} seats ÷ ${(TARGET_FILL_RATE * 100).toFixed(0)}% fill). Raised to minimum.`,
    }
  }
  return {
    name: 'Cost Floor',
    adjustment: 0,
    weight: 1.0,
    explanation: `Base fare ₹${input.base_fare} meets the ₹${FLOOR_FARE} floor. No adjustment needed.`,
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Factor 2 — Time Curve
// ─────────────────────────────────────────────────────────────────────────────

function calculateTimeCurve(input: DynamicPricingInput): PricingFactor {
  const h = input.hours_to_departure
  const occ = input.current_occupancy
  const base = input.base_fare
  let pct = 0
  let explanation = ''

  if (h > 168) {
    // 7+ days out — early bird discount
    pct = -0.05
    explanation = `${Math.round(h / 24)} days out — early bird discount -5%`
  } else if (h > 72) {
    // 3-7 days — slight discount
    pct = -0.03
    explanation = `${Math.round(h / 24)} days out — mild early discount -3%`
  } else if (h > 48) {
    // 2-3 days — base
    pct = 0
    explanation = '2-3 days out — base pricing window'
  } else if (h > 24) {
    // 1-2 days — warming up
    pct = 0.05
    explanation = '24-48h to departure — demand warming +5%'
  } else if (h > 12) {
    // 12-24h — prime booking window
    pct = occ > 40 ? 0.10 : 0.03
    explanation = occ > 40
      ? `12-24h out with ${occ.toFixed(0)}% occupied — peak window +10%`
      : `12-24h out but only ${occ.toFixed(0)}% occupied — mild +3%`
  } else if (h > 6) {
    // 6-12h — last push
    if (occ > 50) {
      pct = 0.15
      explanation = `<12h with ${occ.toFixed(0)}% occupied — urgency premium +15%`
    } else {
      pct = -0.08
      explanation = `<12h but only ${occ.toFixed(0)}% occupied — stimulate bookings -8%`
    }
  } else {
    // <6h — fire sale or last-minute premium
    if (occ > 60) {
      pct = 0.20
      explanation = `<6h departure, ${occ.toFixed(0)}% full — last-minute premium +20%`
    } else if (occ > 30) {
      pct = 0.05
      explanation = `<6h departure, ${occ.toFixed(0)}% full — mild last-minute +5%`
    } else {
      pct = -0.15
      explanation = `<6h departure, only ${occ.toFixed(0)}% full — fire sale -15%`
    }
  }

  return {
    name: 'Time Curve',
    adjustment: Math.round(base * pct),
    weight: 1.0,
    explanation,
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Factor 3 — Velocity Acceleration
// ─────────────────────────────────────────────────────────────────────────────

function calculateVelocityFactor(input: DynamicPricingInput): PricingFactor {
  const base = input.base_fare
  const recent = input.bookings_last_1h
  const hourlyAvg6h = input.bookings_last_6h / 6

  // If no bookings data at all, neutral
  if (input.bookings_last_6h === 0 && input.bookings_last_1h === 0) {
    return {
      name: 'Velocity',
      adjustment: 0,
      weight: 0.8,
      explanation: 'No recent booking data — neutral velocity.',
    }
  }

  const ratio = hourlyAvg6h > 0 ? recent / hourlyAvg6h : recent > 0 ? 3.0 : 0

  let pct = 0
  let explanation = ''

  if (ratio > 2.0) {
    // Strong acceleration — demand surge
    pct = 0.15
    explanation = `Booking velocity surging — ${recent} in last hour vs ${hourlyAvg6h.toFixed(1)}/h avg (${ratio.toFixed(1)}x). Premium +15%.`
  } else if (ratio > 1.5) {
    // Moderate acceleration
    pct = 0.08
    explanation = `Bookings accelerating — ${recent} in last hour vs ${hourlyAvg6h.toFixed(1)}/h avg (${ratio.toFixed(1)}x). Lift +8%.`
  } else if (ratio < 0.3 && input.current_occupancy < 40) {
    // Stalling with low occupancy — discount to stimulate
    pct = -0.08
    explanation = `Bookings stalling (${ratio.toFixed(1)}x avg) and only ${input.current_occupancy.toFixed(0)}% occupied. Stimulus -8%.`
  } else if (ratio < 0.5 && input.current_occupancy < 30) {
    pct = -0.05
    explanation = `Bookings slowing (${ratio.toFixed(1)}x avg) with low ${input.current_occupancy.toFixed(0)}% occupancy. Mild stimulus -5%.`
  } else {
    explanation = `Normal velocity (${ratio.toFixed(1)}x avg). No adjustment.`
  }

  return {
    name: 'Velocity',
    adjustment: Math.round(base * pct),
    weight: 0.8,
    explanation,
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Factor 4 — Competitor Reactive (Bus-Type Aware)
//
// KEY BUSINESS RULE:
//   Mythri runs non-Volvo sleeper buses. Vikram runs Volvo (premium).
//   VKaveri runs non-Volvo (same class as Mythri).
//
//   → Compare Mythri PRIMARILY against same-class competitors (VKaveri).
//   → Volvo competitors (Vikram) are used as a CEILING, not a target.
//   → Never recommend raising Mythri above the cheapest non-Volvo competitor.
//   → Volvo fares only influence pricing as an upper bound.
// ─────────────────────────────────────────────────────────────────────────────

/** Detect if a bus type string indicates a Volvo/premium bus */
export function isVolvoOrPremium(busType?: string): boolean {
  if (!busType) return false
  const bt = busType.toLowerCase()
  return bt.includes('volvo') || bt.includes('scania') || bt.includes('multi-axle')
}

function calculateCompetitorFactor(input: DynamicPricingInput): PricingFactor {
  if (!input.competitor_fares || input.competitor_fares.length === 0) {
    return {
      name: 'Competitor',
      adjustment: 0,
      weight: 0,
      explanation: 'No competitor data available for this date. Factor disabled.',
    }
  }

  const mythri = input.base_fare
  const mythriIsVolvo = input.is_volvo || isVolvoOrPremium(input.bus_type)

  // Split competitors into same-class (non-Volvo) and premium (Volvo)
  const sameClass: CompetitorFareInput[] = []
  const premiumClass: CompetitorFareInput[] = []

  for (const c of input.competitor_fares) {
    if (c.is_volvo || isVolvoOrPremium(c.bus_type)) {
      premiumClass.push(c)
    } else {
      sameClass.push(c)
    }
  }

  // If Mythri itself is Volvo, compare with all competitors normally
  // Otherwise, compare primarily with same-class (non-Volvo) competitors
  const primaryComps = mythriIsVolvo ? input.competitor_fares : sameClass
  const hasSameClass = primaryComps.length > 0
  const hasPremium = premiumClass.length > 0

  // On high-demand / event days, competitor pressure matters less
  const isHighDemand = input.is_event || input.is_long_weekend || input.current_occupancy > 70
  const weight = isHighDemand ? 0.4 : 0.9

  let adjustment = 0
  let explanation = ''

  if (hasSameClass) {
    // ── Compare with same-class (non-Volvo) competitors ──
    const cheapest = Math.min(...primaryComps.map((c) => c.min_fare))
    const cheapestOp = primaryComps.reduce((best, c) =>
      c.min_fare < best.min_fare ? c : best
    )
    const diff = mythri - cheapest
    const diffPct = cheapest > 0 ? (diff / cheapest) * 100 : 0

    if (diffPct > 15) {
      const target = cheapest - 30
      adjustment = target - mythri
      explanation = `Mythri ₹${mythri} is ${diffPct.toFixed(0)}% above ${cheapestOp.operator} ₹${cheapest} (same bus class). Suggest ₹${target} (undercut ₹30).`
    } else if (diffPct > 8) {
      const target = cheapest - 20
      adjustment = target - mythri
      explanation = `Mythri ₹${mythri} is ${diffPct.toFixed(0)}% above ${cheapestOp.operator} ₹${cheapest} (same class). Suggest ₹${target} (undercut ₹20).`
    } else if (diffPct > 0) {
      adjustment = -Math.min(diff + 10, 50)
      explanation = `Mythri ₹${mythri} is ₹${diff} above ${cheapestOp.operator} ₹${cheapest} (same class). Small undercut ₹${Math.abs(adjustment)}.`
    } else if (diff < -50) {
      // Mythri cheaper than same-class — can raise, but NOT above same-class competitor
      const maxRaise = Math.abs(diff) - 30 // stay ₹30 below same-class competitor
      const raise = Math.min(maxRaise, 100)
      adjustment = raise
      explanation = `Mythri ₹${mythri} is ₹${Math.abs(diff)} below ${cheapestOp.operator} ₹${cheapest} (same class). Raise +₹${raise} while staying below same-class competitor.`
    } else if (diff < -10) {
      const raise = Math.min(Math.abs(diff) - 10, 50)
      adjustment = raise
      explanation = `Mythri ₹${mythri} is ₹${Math.abs(diff)} below ${cheapestOp.operator} ₹${cheapest} (same class). Room to raise +₹${raise}.`
    } else {
      explanation = `Mythri ₹${mythri} is competitive with ${cheapestOp.operator} ₹${cheapest} (same class). No adjustment.`
    }

    // SAFETY: Never recommend a price above the cheapest same-class competitor
    const recommendedFare = mythri + adjustment
    if (!mythriIsVolvo && recommendedFare > cheapest) {
      adjustment = cheapest - 30 - mythri // max: undercut same-class by ₹30
      if (adjustment > 0) adjustment = 0 // don't raise above competitor
      explanation += ` Capped to stay below same-class competitor ₹${cheapest}.`
    }
  } else if (hasPremium && !mythriIsVolvo) {
    // ── Only Volvo competitors available (e.g., only Vikram) ──
    // Mythri is non-Volvo, so Volvo fares are NOT a comparison target.
    // Instead, use Volvo fare as a ceiling — we should be significantly below.
    const cheapestVolvo = Math.min(...premiumClass.map((c) => c.min_fare))
    const cheapestVolvoOp = premiumClass.reduce((best, c) =>
      c.min_fare < best.min_fare ? c : best
    )

    // Mythri should be at least 15-25% below Volvo fares (non-Volvo discount)
    const targetMaxFare = Math.round(cheapestVolvo * 0.80) // 20% below Volvo
    const diff = mythri - targetMaxFare

    if (diff > 0) {
      // Mythri is too close to Volvo pricing — people will choose Volvo
      adjustment = targetMaxFare - mythri
      explanation = `Only Volvo competitor: ${cheapestVolvoOp.operator} ₹${cheapestVolvo}. Non-Volvo bus should be ~20% below Volvo. Suggest ₹${targetMaxFare} max (currently ₹${diff} too high).`
    } else if (diff < -200) {
      // Mythri is far below Volvo — room to raise
      const raise = Math.min(Math.abs(diff) - 100, 100) // keep healthy gap
      if (raise > 0) {
        adjustment = raise
        explanation = `Only Volvo competitor: ${cheapestVolvoOp.operator} ₹${cheapestVolvo}. Mythri ₹${mythri} has room to raise +₹${raise} while maintaining 20%+ discount vs Volvo.`
      } else {
        explanation = `Only Volvo competitor: ${cheapestVolvoOp.operator} ₹${cheapestVolvo}. Mythri ₹${mythri} is well-positioned below Volvo. No adjustment.`
      }
    } else {
      explanation = `Only Volvo competitor: ${cheapestVolvoOp.operator} ₹${cheapestVolvo}. Mythri ₹${mythri} is correctly priced below Volvo. No adjustment.`
    }
  } else {
    explanation = 'No comparable competitor data for this bus class. No adjustment.'
  }

  if (isHighDemand && adjustment < 0) {
    adjustment = 0
    explanation += ' (High-demand period — competitor discount suppressed.)'
  }

  return {
    name: 'Competitor',
    adjustment: Math.round(adjustment),
    weight,
    explanation,
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Factor 5 — Day-of-Week Pattern
// ─────────────────────────────────────────────────────────────────────────────

function calculateDayPattern(input: DynamicPricingInput): PricingFactor {
  const pattern = DAY_PATTERNS[input.direction] || DAY_PATTERNS.BLR_WGL
  const multiplier = pattern[input.day_of_week] ?? 1.0
  const pct = multiplier - 1.0
  const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
  const dayName = dayNames[input.day_of_week] || 'Unknown'
  const dirLabel = input.direction === 'BLR_WGL' ? 'BLR→WGL' : 'WGL→BLR'

  return {
    name: 'Day Pattern',
    adjustment: Math.round(input.base_fare * pct),
    weight: 0.9,
    explanation:
      pct > 0
        ? `${dayName} is a peak day for ${dirLabel} (+${(pct * 100).toFixed(0)}%). Higher demand expected.`
        : pct < 0
          ? `${dayName} is a low-demand day for ${dirLabel} (${(pct * 100).toFixed(0)}%). Discounted to stimulate bookings.`
          : `${dayName} is a neutral day for ${dirLabel}. No day-of-week adjustment.`,
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Factor 6 — Event / Festival
// ─────────────────────────────────────────────────────────────────────────────

function calculateEventFactor(input: DynamicPricingInput): PricingFactor {
  const base = input.base_fare

  if (input.is_event && input.event_impact === 'SURGE') {
    const pct = 0.20 // +20% for SURGE events
    return {
      name: 'Event',
      adjustment: Math.round(base * pct),
      weight: 1.0,
      explanation: `${input.event_name || 'Major festival'} — SURGE event. Premium +20%.`,
    }
  }

  if (input.is_event && input.event_impact === 'HIGH') {
    const pct = 0.12 // +12% for HIGH events
    return {
      name: 'Event',
      adjustment: Math.round(base * pct),
      weight: 1.0,
      explanation: `${input.event_name || 'Holiday'} — HIGH impact event. Lift +12%.`,
    }
  }

  if (input.is_long_weekend && !input.is_event) {
    const pct = 0.08 // +8% for long weekends without specific event
    return {
      name: 'Event',
      adjustment: Math.round(base * pct),
      weight: 0.9,
      explanation: 'Long weekend period — increased travel demand. Lift +8%.',
    }
  }

  return {
    name: 'Event',
    adjustment: 0,
    weight: 0,
    explanation: 'No festival or long weekend. Factor inactive.',
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Factor 7 — Seat Scarcity
// ─────────────────────────────────────────────────────────────────────────────

function calculateScarcityFactor(input: DynamicPricingInput): PricingFactor {
  const base = input.base_fare
  const avail = input.available_seats
  const occ = input.current_occupancy
  const h = input.hours_to_departure

  if (avail <= 5 && avail > 0) {
    const pct = 0.20
    return {
      name: 'Scarcity',
      adjustment: Math.round(base * pct),
      weight: 1.0,
      explanation: `Only ${avail} seats left! Last-seat premium +20%.`,
    }
  }

  if (avail <= 10) {
    const pct = 0.12
    return {
      name: 'Scarcity',
      adjustment: Math.round(base * pct),
      weight: 0.95,
      explanation: `${avail} seats remaining — scarcity premium +12%.`,
    }
  }

  if (occ > 60) {
    const pct = 0.05
    return {
      name: 'Scarcity',
      adjustment: Math.round(base * pct),
      weight: 0.8,
      explanation: `${occ.toFixed(0)}% occupied — healthy demand, mild lift +5%.`,
    }
  }

  if (occ < 20 && h > 48) {
    const pct = -0.05
    return {
      name: 'Scarcity',
      adjustment: Math.round(base * pct),
      weight: 0.8,
      explanation: `Only ${occ.toFixed(0)}% occupied with ${Math.round(h)}h to go — discount to stimulate -5%.`,
    }
  }

  return {
    name: 'Scarcity',
    adjustment: 0,
    weight: 0.5,
    explanation: `${occ.toFixed(0)}% occupied, ${avail} seats available. No scarcity adjustment.`,
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Demand Score (0-100)
// ─────────────────────────────────────────────────────────────────────────────

function computeDemandScore(input: DynamicPricingInput): number {
  // Occupancy component (0-40)
  const occScore = Math.min(input.current_occupancy, 100) * 0.4

  // Velocity component (0-25): 5+ bookings/hour = max
  const velScore = Math.min(input.bookings_last_1h * 5, 25)

  // Time urgency (0-15): closer to departure = higher
  const maxH = 168 // 7 days
  const timeScore = Math.max(0, 15 * (1 - input.hours_to_departure / maxH))

  // Event boost (0-10)
  let eventScore = 0
  if (input.event_impact === 'SURGE') eventScore = 10
  else if (input.event_impact === 'HIGH') eventScore = 7
  else if (input.is_long_weekend) eventScore = 5

  // Day pattern boost (0-10)
  const pattern = DAY_PATTERNS[input.direction] || DAY_PATTERNS.BLR_WGL
  const dayMult = pattern[input.day_of_week] ?? 1.0
  const dayScore = Math.max(0, (dayMult - 0.85) * 28.57) // maps 0.85-1.20 to 0-10

  const total = occScore + velScore + timeScore + eventScore + dayScore
  return Math.round(Math.min(Math.max(total, 0), 100) * 100) / 100
}

// ─────────────────────────────────────────────────────────────────────────────
// Urgency Classification
// ─────────────────────────────────────────────────────────────────────────────

function classifyUrgency(
  changePct: number,
  demandScore: number,
  hoursToDepart: number
): Urgency {
  const absChange = Math.abs(changePct)
  if (absChange > 15 || (demandScore > 70 && hoursToDepart < 24)) return 'CRITICAL'
  if (absChange > 8 || demandScore > 50) return 'HIGH'
  if (absChange > 3) return 'MEDIUM'
  return 'LOW'
}

// ─────────────────────────────────────────────────────────────────────────────
// Competitor Position
// ─────────────────────────────────────────────────────────────────────────────

function classifyPosition(
  recommendedFare: number,
  competitors: CompetitorFareInput[],
  mythriIsVolvo?: boolean
): CompetitorPosition {
  if (competitors.length === 0) return 'COMPETITIVE'

  // Compare against same-class competitors only (non-Volvo for non-Volvo Mythri)
  const sameClass = mythriIsVolvo
    ? competitors
    : competitors.filter((c) => !c.is_volvo && !isVolvoOrPremium(c.bus_type))

  const compsToUse = sameClass.length > 0 ? sameClass : competitors
  const cheapest = Math.min(...compsToUse.map((c) => c.min_fare))
  const diffPct = cheapest > 0 ? ((recommendedFare - cheapest) / cheapest) * 100 : 0
  if (diffPct < -5) return 'CHEAPEST'
  if (diffPct <= 5) return 'COMPETITIVE'
  if (diffPct <= 15) return 'PREMIUM'
  return 'OVERPRICED'
}

// ─────────────────────────────────────────────────────────────────────────────
// Confidence Score
// ─────────────────────────────────────────────────────────────────────────────

function calculateConfidence(input: DynamicPricingInput): number {
  let score = 50 // baseline

  // More competitor data = more confident
  if (input.competitor_fares.length >= 2) score += 20
  else if (input.competitor_fares.length === 1) score += 10

  // Recent bookings = better velocity signal
  if (input.bookings_last_24h > 5) score += 15
  else if (input.bookings_last_24h > 0) score += 8

  // Closer to departure = more certain about demand
  if (input.hours_to_departure < 48) score += 10
  else if (input.hours_to_departure < 168) score += 5

  // Event data available
  if (input.is_event) score += 5

  return Math.min(score, 100)
}

// ─────────────────────────────────────────────────────────────────────────────
// Main Entry Point
// ─────────────────────────────────────────────────────────────────────────────

export function generateDynamicPrice(
  input: DynamicPricingInput
): DynamicPricingResult {
  // Compute all 7 factors
  const factors: PricingFactor[] = [
    calculateFloorFactor(input),
    calculateTimeCurve(input),
    calculateVelocityFactor(input),
    calculateCompetitorFactor(input),
    calculateDayPattern(input),
    calculateEventFactor(input),
    calculateScarcityFactor(input),
  ]

  // Sum weighted adjustments
  const totalAdjustment = factors.reduce(
    (sum, f) => sum + f.adjustment * f.weight,
    0
  )

  let recommended = Math.round(input.base_fare + totalAdjustment)

  // ── Market-aware floor & ceiling ──
  // Determine the competitive ceiling (cheapest same-class competitor)
  const mythriIsVolvo = input.is_volvo || isVolvoOrPremium(input.bus_type)
  const sameClassComps = mythriIsVolvo
    ? input.competitor_fares
    : input.competitor_fares.filter((c) => !c.is_volvo && !isVolvoOrPremium(c.bus_type))
  const cheapestSameClass = sameClassComps.length > 0
    ? Math.min(...sameClassComps.map((c) => c.min_fare))
    : null

  // Markup ceiling (150% of current fare)
  const markupCeiling = Math.round(input.base_fare * MAX_MARKUP_RATIO)

  // Competitive ceiling: never price above cheapest same-class competitor
  // If no same-class, use markup ceiling only
  const competitiveCeiling = cheapestSameClass !== null
    ? cheapestSameClass - 20 // stay ₹20 below same-class competitor
    : markupCeiling

  // Apply floor: use FLOOR_FARE only if market supports it
  if (cheapestSameClass !== null && cheapestSameClass < FLOOR_FARE) {
    // Market is below our floor — use competitive pricing, not floor
    // Don't force ₹1308 when competitor sells at ₹849
    const softFloor = Math.max(cheapestSameClass - 50, input.base_fare * 0.9) // don't go below 90% of current
    recommended = Math.max(softFloor, recommended)
  } else {
    // Market supports the floor — enforce it
    recommended = Math.max(FLOOR_FARE, recommended)
  }

  // Apply ceiling: never exceed competitive ceiling OR markup ceiling
  recommended = Math.min(recommended, markupCeiling, competitiveCeiling)

  // Round to nearest ₹5 for cleaner display
  recommended = Math.round(recommended / 5) * 5

  // Final safety: NEVER exceed cheapest same-class competitor
  if (cheapestSameClass !== null && recommended > cheapestSameClass) {
    recommended = Math.round((cheapestSameClass - 30) / 5) * 5 // undercut by ₹30
  }

  // Compute metrics
  const demandScore = computeDemandScore(input)
  const changePct =
    input.base_fare > 0
      ? ((recommended - input.base_fare) / input.base_fare) * 100
      : 0

  const urgency = classifyUrgency(changePct, demandScore, input.hours_to_departure)
  const position = classifyPosition(recommended, input.competitor_fares, mythriIsVolvo)
  const confidence = calculateConfidence(input)

  // Revenue estimates
  // Assume available seats fill at target rate
  const estFill = Math.min(input.available_seats, Math.ceil(input.total_seats * TARGET_FILL_RATE) - input.booked_seats)
  const remainingToSell = Math.max(estFill, 0)
  const revenueCurrent = input.booked_seats * (input.base_fare) + remainingToSell * input.base_fare
  const revenueRecommended = input.booked_seats * (input.base_fare) + remainingToSell * recommended

  return {
    recommended_fare: recommended,
    floor_fare: FLOOR_FARE,
    ceiling_fare: markupCeiling,
    factors,
    demand_score: demandScore,
    urgency,
    position_vs_competitors: position,
    revenue_at_current: Math.round(revenueCurrent),
    revenue_at_recommended: Math.round(revenueRecommended),
    revenue_delta: Math.round(revenueRecommended - revenueCurrent),
    confidence,
  }
}
