'use client'

import React, { useState, useMemo, useEffect } from 'react'
import {
  Loader2,
  AlertTriangle,
  Zap,
  Info,
  TrendingUp,
  TrendingDown,
  Minus,
  Bus,
  Clock,
  ChevronDown,
  ChevronUp,
  Calendar,
  ArrowRight,
  LayoutGrid,
  X,
} from 'lucide-react'

// ─────────────────────────────────────────────────────────────────────────────
// Operator color scheme (matches Live Comparison)
// ─────────────────────────────────────────────────────────────────────────────
const OPERATOR_COLORS: Record<string, { badge: string; badgeText: string; bg: string; border: string }> = {
  mythri:  { badge: 'bg-blue-100',   badgeText: 'text-blue-700',   bg: 'bg-blue-50',   border: 'border-blue-200' },
  vikram:  { badge: 'bg-indigo-100', badgeText: 'text-indigo-700', bg: 'bg-indigo-50', border: 'border-indigo-200' },
  vkaveri: { badge: 'bg-purple-100', badgeText: 'text-purple-700', bg: 'bg-purple-50', border: 'border-purple-200' },
}

function formatCurrency(n: number) {
  return `₹${n.toLocaleString('en-IN')}`
}

function formatDepartureTime(dt: string | null) {
  if (!dt) return '--:--'
  if (/^\d{2}:\d{2}$/.test(dt)) return dt
  try {
    return new Date(dt).toTimeString().substring(0, 5)
  } catch {
    return dt
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Seat Grid Constants & Helpers (same as Live Comparison)
// ─────────────────────────────────────────────────────────────────────────────
const LOWER_SEAT_TYPES = new Set([2, 12, 14, 16, 17, 20])
const UPPER_SEAT_TYPES = new Set([3, 13, 15, 18, 19, 21])

function getSeatStatus(seat: any): 'available' | 'booked' | 'ladies' {
  const td = seat?.td_class || ''
  if (td.includes('available_seat') || td.includes('availableSleeper') || td.includes('availableSeater')) {
    return 'available'
  }
  if (td.includes('ladies') || td.includes('female') || td.includes('booked_by_ladies')) {
    return 'ladies'
  }
  if ((seat?.fare || 0) > 0 && !td.includes('reservedSleeper') && !td.includes('reservedSeater')) {
    return 'available'
  }
  return 'booked'
}

function processCoachLayout(layout: any[][]): {
  lower: any[][]
  upper: any[][]
  fareTiers: number[]
} {
  if (!layout || !Array.isArray(layout)) return { lower: [], upper: [], fareTiers: [] }

  const lowerRows: any[][] = []
  const upperRows: any[][] = []
  const fareSet = new Set<number>()

  for (const row of layout) {
    if (!Array.isArray(row)) continue
    const hasSeats = row.some((s) => s && s.seat_number)
    if (!hasSeats) continue

    const lowerSeats: any[] = []
    const upperSeats: any[] = []

    for (const seat of row) {
      if (!seat) continue
      if (seat.is_ganway_col) continue
      if (!seat.seat_number) continue

      const stv = seat.seat_type_value
      const status = getSeatStatus(seat)
      const fare = typeof seat.fare === 'number' ? seat.fare : 0
      if (status === 'available' && fare > 0) fareSet.add(Math.round(fare))

      if (LOWER_SEAT_TYPES.has(stv)) {
        lowerSeats.push(seat)
      } else if (UPPER_SEAT_TYPES.has(stv)) {
        upperSeats.push(seat)
      } else {
        lowerSeats.push(seat)
      }
    }

    if (lowerSeats.length > 0) {
      if (lowerSeats.length >= 2) {
        lowerRows.push([lowerSeats[0], { _gangway: true }, ...lowerSeats.slice(1)])
      } else {
        lowerRows.push(lowerSeats)
      }
    }
    if (upperSeats.length > 0) {
      if (upperSeats.length >= 2) {
        upperRows.push([upperSeats[0], { _gangway: true }, ...upperSeats.slice(1)])
      } else {
        upperRows.push(upperSeats)
      }
    }
  }

  const fareTiers = Array.from(fareSet).sort((a, b) => a - b)
  return { lower: lowerRows, upper: upperRows, fareTiers }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main Component
// ─────────────────────────────────────────────────────────────────────────────

export function AIPricingTab({
  data,
  loading,
  routeFilter,
  routes,
  setRouteFilter,
}: {
  data: any
  loading: boolean
  routeFilter: string
  routes: any[]
  setRouteFilter: (v: string) => void
}) {
  const todayIST = useMemo(() => {
    const now = new Date()
    const istOffset = 5.5 * 60 * 60 * 1000
    const istNow = new Date(now.getTime() + istOffset)
    return istNow.toISOString().split('T')[0]
  }, [])

  const [selectedDate, setSelectedDate] = useState<string>(todayIST)
  const [expandedCard, setExpandedCard] = useState<string | null>(null)
  const [seatModalGroup, setSeatModalGroup] = useState<any | null>(null)

  // Close seat modal on Escape
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if (e.key === 'Escape' && seatModalGroup) setSeatModalGroup(null)
    }
    document.addEventListener('keydown', handleKeyDown)
    return () => document.removeEventListener('keydown', handleKeyDown)
  }, [seatModalGroup])

  const summary = data?.summary || {}
  const recommendations: any[] = data?.recommendations || []
  const dateGroups: any[] = data?.date_groups || []

  // Map recommendations by date+route
  const recByKey = useMemo(() => {
    const map = new Map<string, any[]>()
    for (const rec of recommendations) {
      const key = `${rec.travel_date}|${rec.route_from}|${rec.route_to}`
      if (!map.has(key)) map.set(key, [])
      map.get(key)!.push(rec)
    }
    return map
  }, [recommendations])

  // Filter date groups by selected date
  const filteredGroups = useMemo(() => {
    if (!selectedDate) return dateGroups
    return dateGroups.filter((g: any) => g.date === selectedDate)
  }, [dateGroups, selectedDate])

  // All unique dates for nav
  const availableDates = useMemo(() => {
    const dates = new Set<string>()
    for (const g of dateGroups) dates.add(g.date)
    return Array.from(dates).sort()
  }, [dateGroups])

  // Stats for selected date
  const dateStats = useMemo(() => {
    const dateRecs = recommendations.filter((r: any) => !selectedDate || r.travel_date === selectedDate)
    const raiseCount = dateRecs.filter((r: any) => r.action === 'RAISE').length
    const lowerCount = dateRecs.filter((r: any) => r.action === 'LOWER').length
    const holdCount = dateRecs.filter((r: any) => r.action === 'HOLD').length
    return { total: dateRecs.length, raiseCount, lowerCount, holdCount }
  }, [recommendations, selectedDate])

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20">
        <Loader2 className="h-8 w-8 animate-spin text-blue-500" />
        <span className="ml-3 text-gray-500">Generating AI pricing recommendations...</span>
      </div>
    )
  }

  if (!data || !data.success) {
    return (
      <div className="text-center py-16">
        <AlertTriangle className="h-10 w-10 text-yellow-500 mx-auto mb-3" />
        <p className="text-gray-600 font-medium">Failed to load AI pricing data</p>
        <p className="text-sm text-gray-400 mt-1">{data?.error || 'Unknown error'}</p>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* ── Header ── */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            <Zap className="h-6 w-6 text-amber-500" />
            AI Dynamic Pricing
          </h2>
          <p className="text-sm text-gray-500 mt-1">
            7-factor pricing engine — review recommendations before applying
          </p>
        </div>
        <div className="flex items-center gap-3">
          <select
            value={routeFilter}
            onChange={(e) => setRouteFilter(e.target.value)}
            className="h-9 rounded-lg border border-gray-200 bg-white px-3 text-sm focus:border-blue-500 focus:outline-none"
          >
            <option value="">All Routes</option>
            {routes.map((r: any, i: number) => (
              <option key={i} value={`${r.route_from || r.origin || ''}|${r.route_to || r.destination || ''}`}>
                {r.route_from || r.origin} → {r.route_to || r.destination}
              </option>
            ))}
          </select>
          <input
            type="date"
            value={selectedDate}
            onChange={(e) => setSelectedDate(e.target.value)}
            className="h-9 rounded-lg border border-gray-200 bg-white px-3 text-sm focus:border-blue-500 focus:outline-none"
          />
          {selectedDate && (
            <button onClick={() => setSelectedDate('')} className="text-xs text-gray-400 hover:text-gray-600">
              Show all
            </button>
          )}
        </div>
      </div>

      {/* ── KPI Summary ── */}
      <div className="grid gap-3 grid-cols-2 lg:grid-cols-5">
        <div className="rounded-2xl bg-white p-4 shadow-lg border border-gray-100">
          <p className="text-[10px] font-medium text-gray-500 uppercase tracking-wide">Min Revenue / Bus</p>
          <p className="text-xl font-bold text-gray-900 mt-1">₹{(summary.daily_minimum || 40000).toLocaleString()}</p>
          <p className="text-[10px] text-gray-400 mt-0.5">per bus, one-way trip</p>
        </div>
        <div className="rounded-2xl bg-white p-4 shadow-lg border border-amber-100">
          <p className="text-[10px] font-medium text-gray-500 uppercase tracking-wide">Floor Fare / Seat</p>
          <p className="text-xl font-bold text-amber-600 mt-1">₹{summary.floor_fare || 1308}</p>
          <p className="text-[10px] text-gray-400 mt-0.5">₹40K ÷ {summary.seats_per_service || 36} seats ÷ {((summary.target_fill_rate || 0.85) * 100).toFixed(0)}% fill</p>
        </div>
        <div className="rounded-2xl bg-white p-4 shadow-lg border border-gray-100">
          <p className="text-[10px] font-medium text-gray-500 uppercase tracking-wide">Avg Recommended</p>
          <p className="text-xl font-bold text-blue-600 mt-1">₹{(summary.avg_recommended_fare || 0).toLocaleString()}</p>
          <p className="text-[10px] text-gray-400 mt-0.5">{summary.total_trips || 0} trips analyzed</p>
        </div>
        <div className="rounded-2xl bg-white p-4 shadow-lg border border-gray-100">
          <p className="text-[10px] font-medium text-gray-500 uppercase tracking-wide">Revenue Impact</p>
          <p className={`text-xl font-bold mt-1 ${(summary.revenue_delta || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
            {(summary.revenue_delta || 0) >= 0 ? '+' : ''}₹{Math.abs(summary.revenue_delta || 0).toLocaleString()}
          </p>
          <p className="text-[10px] text-gray-400 mt-0.5">all {summary.total_trips || 0} trips combined</p>
        </div>
        <div className="rounded-2xl bg-white p-4 shadow-lg border border-gray-100">
          <p className="text-[10px] font-medium text-gray-500 uppercase tracking-wide">
            {selectedDate ? `${new Date(selectedDate + 'T00:00:00').toLocaleDateString('en-IN', { day: 'numeric', month: 'short' })} Actions` : 'All Actions'}
          </p>
          <div className="flex items-center gap-3 mt-1">
            <span className="flex items-center gap-1 text-green-600 font-bold text-sm">
              <TrendingUp className="h-3.5 w-3.5" />{dateStats.raiseCount}
            </span>
            <span className="flex items-center gap-1 text-red-600 font-bold text-sm">
              <TrendingDown className="h-3.5 w-3.5" />{dateStats.lowerCount}
            </span>
            <span className="flex items-center gap-1 text-gray-500 font-bold text-sm">
              <Minus className="h-3.5 w-3.5" />{dateStats.holdCount}
            </span>
          </div>
          <p className="text-[10px] text-gray-400 mt-0.5">raise · lower · hold</p>
        </div>
      </div>

      {/* ── Date Quick Nav ── */}
      {availableDates.length > 1 && (
        <div className="flex gap-1.5 overflow-x-auto pb-1">
          {availableDates.map((dt) => {
            const dtObj = new Date(dt + 'T00:00:00+05:30')
            const dayName = dtObj.toLocaleDateString('en-IN', { weekday: 'short' })
            const dateNum = dtObj.getDate()
            const month = dtObj.toLocaleDateString('en-IN', { month: 'short' })
            const isSelected = selectedDate === dt
            const isToday = dt === todayIST
            const group = dateGroups.find((g: any) => g.date === dt)
            const hasEvent = group?.event_name
            const isLW = group?.is_long_weekend
            const isWeekend = dtObj.getDay() === 0 || dtObj.getDay() === 6

            return (
              <button
                key={dt}
                onClick={() => setSelectedDate(dt)}
                className={`flex-shrink-0 w-[70px] rounded-lg border p-2 text-center transition-all ${
                  isSelected
                    ? 'bg-blue-600 border-blue-600 text-white shadow-md'
                    : hasEvent
                      ? 'bg-amber-50 border-amber-200 hover:bg-amber-100'
                      : isLW
                        ? 'bg-purple-50 border-purple-200 hover:bg-purple-100'
                        : isWeekend
                          ? 'bg-blue-50 border-blue-200 hover:bg-blue-100'
                          : 'bg-white border-gray-200 hover:bg-gray-50'
                }`}
              >
                <p className={`text-[9px] font-medium ${isSelected ? 'text-blue-100' : 'text-gray-500'}`}>{dayName}</p>
                <p className={`text-sm font-bold ${isSelected ? 'text-white' : 'text-gray-900'}`}>{dateNum}</p>
                <p className={`text-[9px] ${isSelected ? 'text-blue-200' : 'text-gray-400'}`}>{month}</p>
                {isToday && !isSelected && <div className="w-1 h-1 bg-blue-500 rounded-full mx-auto mt-0.5" />}
                {hasEvent && !isSelected && <p className="text-[9px] mt-0.5">{group.event_emoji}</p>}
              </button>
            )
          })}
        </div>
      )}

      {/* ── Route Cards ── */}
      {filteredGroups.length === 0 ? (
        <div className="rounded-xl border bg-white shadow-sm">
          <div className="flex flex-col items-center justify-center py-16 text-gray-400">
            <Calendar className="h-10 w-10 mb-3" />
            <p className="text-lg font-medium">No data for this date</p>
            <p className="text-sm mt-1">Select a different date or check if the scraper has run</p>
          </div>
        </div>
      ) : (
        <div className="space-y-4">
          {filteredGroups.map((group: any, idx: number) => {
            const cardKey = `${group.date}|${group.route_from}|${group.route_to}`
            const isExpanded = expandedCard === cardKey
            const recs = recByKey.get(cardKey) || []
            const rec = recs[0]

            const mythri = group.operators?.mythri
            const vikram = group.operators?.vikram
            const vkaveri = group.operators?.vkaveri

            const dtObj = new Date(group.date + 'T00:00:00+05:30')
            const dayName = dtObj.toLocaleDateString('en-IN', { weekday: 'long' })
            const dateDisp = dtObj.toLocaleDateString('en-IN', { day: 'numeric', month: 'short', year: 'numeric' })

            const action = rec?.action || 'HOLD'
            const changePct = rec?.change_percent || 0

            // Build group data for seat modal
            const mythriSvc = mythri?.services?.[0]
            const vikramSvc = vikram?.services?.[0]
            const vkaveriSvc = vkaveri?.services?.[0]

            return (
              <div key={idx} className="rounded-xl border bg-white shadow-sm overflow-hidden">
                {/* ── Card Header ── */}
                <div className={`flex items-center justify-between px-5 py-3 border-b ${
                  group.event_name ? 'bg-amber-50/80 border-amber-200' :
                  group.is_long_weekend ? 'bg-purple-50/80 border-purple-200' : 'bg-gray-50/80'
                }`}>
                  <div className="flex items-center gap-3 flex-wrap">
                    <div className="flex items-center gap-1.5">
                      <span className="text-sm font-semibold text-gray-900">{group.route_from}</span>
                      <ArrowRight className="h-3.5 w-3.5 text-gray-400" />
                      <span className="text-sm font-semibold text-gray-900">{group.route_to}</span>
                    </div>
                    <span className="text-sm text-gray-500">{dayName}, {dateDisp}</span>
                    {group.event_name && (
                      <span className="inline-flex items-center gap-1 rounded-full bg-amber-100 px-2 py-0.5 text-xs font-semibold text-amber-700">
                        {group.event_emoji} {group.event_name}
                        {group.event_impact === 'SURGE' && <span className="text-[9px] font-bold text-red-600 ml-1">SURGE</span>}
                      </span>
                    )}
                    {group.is_long_weekend && !group.event_name && (
                      <span className="inline-flex items-center gap-1 rounded-full bg-purple-100 px-2 py-0.5 text-xs font-semibold text-purple-700">
                        {group.long_weekend_name || 'Long Weekend'}
                      </span>
                    )}
                  </div>
                  <div className="flex items-center gap-2">
                    {/* View Seats Button */}
                    <button
                      onClick={(e) => {
                        e.stopPropagation()
                        setSeatModalGroup({
                          route_from: group.route_from,
                          route_to: group.route_to,
                          travel_date: group.date,
                          mythri_trip_id: mythriSvc?.trip_identifier || '',
                          mythri_service: mythriSvc?.service_number || '',
                          mythri_bus_type: mythriSvc?.bus_type || '',
                          mythri_departure: mythriSvc?.departure_time || '',
                          mythri_available_seats: mythriSvc?.available_seats,
                          mythri_seat_type_fares: mythriSvc?.fare_by_seat_type || [],
                          competitors: [
                            ...(vikramSvc ? [{
                              operator: 'vikram',
                              trip_id: vikramSvc.trip_identifier || '',
                              service: vikramSvc.service_number || '',
                              bus_type: vikramSvc.bus_type || '',
                              departure: vikramSvc.departure_time || '',
                              available_seats: vikramSvc.available_seats,
                              seat_type_fares: vikramSvc.fare_by_seat_type || [],
                            }] : []),
                            ...(vkaveriSvc ? [{
                              operator: 'vkaveri',
                              trip_id: vkaveriSvc.trip_identifier || '',
                              service: vkaveriSvc.service_number || '',
                              bus_type: vkaveriSvc.bus_type || '',
                              departure: vkaveriSvc.departure_time || '',
                              available_seats: vkaveriSvc.available_seats,
                              seat_type_fares: vkaveriSvc.fare_by_seat_type || [],
                            }] : []),
                          ],
                          recommendation: rec,
                        })
                      }}
                      className="inline-flex items-center gap-1.5 rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-blue-700 transition-colors shadow-sm"
                    >
                      <LayoutGrid className="h-3.5 w-3.5" />
                      View Seats
                    </button>
                    {rec && <ActionBadge action={action} changePct={changePct} />}
                  </div>
                </div>

                {/* ── Recommendation Banner ── */}
                {rec && rec.reason && action !== 'HOLD' && (
                  <div className={`px-5 py-2 text-sm border-b ${
                    action === 'RAISE' ? 'bg-green-50 border-green-100 text-green-800' : 'bg-red-50 border-red-100 text-red-800'
                  }`}>
                    <span className="font-medium">{action === 'RAISE' ? '↑ Raise' : '↓ Lower'} Recommendation:</span>{' '}
                    {rec.reason}
                    {rec.recommended_fare && (
                      <span className="ml-2 font-bold">
                        {formatCurrency(rec.current_fare)} → {formatCurrency(rec.recommended_fare)} ({changePct > 0 ? '+' : ''}{changePct.toFixed(1)}%)
                      </span>
                    )}
                  </div>
                )}

                {/* ── 3-Column Operator Comparison ── */}
                <div className="grid grid-cols-1 md:grid-cols-3 divide-y md:divide-y-0 md:divide-x divide-gray-100">
                  <OperatorCard label="Mythri" operator="mythri" services={mythri?.services || []} recommendation={rec} isMainOperator={true} />
                  <OperatorCard label="Vikram" operator="vikram" services={vikram?.services || []} isMainOperator={false} />
                  <OperatorCard label="VKaveri" operator="vkaveri" services={vkaveri?.services || []} isMainOperator={false} />
                </div>

                {/* ── Expandable Factor Breakdown ── */}
                {isExpanded && rec && (
                  <div className="border-t border-gray-200 bg-gray-50 px-5 py-4">
                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                      <div className="lg:col-span-2">
                        <h4 className="text-xs font-semibold text-gray-700 uppercase tracking-wide mb-3">7-Factor Pricing Breakdown</h4>
                        <div className="space-y-2">
                          {(rec.factors || []).map((f: any, fIdx: number) => {
                            const maxAdj = Math.max(...(rec.factors || []).map((ff: any) => Math.abs(ff.adjustment * ff.weight)), 1)
                            const barWidth = maxAdj > 0 ? (Math.abs(f.adjustment * f.weight) / maxAdj) * 100 : 0
                            return (
                              <div key={fIdx} className="flex items-center gap-3">
                                <div className="w-28 text-xs font-medium text-gray-600 text-right flex-shrink-0">{f.name}</div>
                                <div className="flex-1 flex items-center gap-2">
                                  <div className="flex-1 h-5 bg-gray-200 rounded-full overflow-hidden">
                                    <div className={`h-full rounded-full ${f.adjustment >= 0 ? 'bg-green-400' : 'bg-red-400'}`} style={{ width: `${Math.min(barWidth, 100)}%` }} />
                                  </div>
                                  <span className={`text-xs font-bold w-16 text-right ${f.adjustment > 0 ? 'text-green-600' : f.adjustment < 0 ? 'text-red-600' : 'text-gray-400'}`}>
                                    {f.adjustment >= 0 ? '+' : ''}₹{f.adjustment}
                                  </span>
                                  {f.weight < 1 && <span className="text-[9px] text-gray-400 w-8">×{f.weight}</span>}
                                </div>
                              </div>
                            )
                          })}
                        </div>
                        <div className="mt-4 space-y-1.5">
                          {(rec.factors || []).filter((f: any) => f.adjustment !== 0).map((f: any, fIdx: number) => (
                            <p key={fIdx} className="text-[11px] text-gray-500">
                              <span className="font-semibold text-gray-600">{f.name}:</span> {f.explanation}
                            </p>
                          ))}
                        </div>
                      </div>
                      <div className="space-y-4">
                        <div>
                          <h4 className="text-xs font-semibold text-gray-700 uppercase tracking-wide mb-2">Trip Details</h4>
                          <div className="bg-white rounded-lg border border-gray-200 p-3 grid grid-cols-2 gap-3 text-xs">
                            <div><span className="text-gray-400">Booked</span><p className="font-bold text-gray-900">{rec.booked_seats} / {rec.total_seats}</p></div>
                            <div><span className="text-gray-400">Available</span><p className={`font-bold ${(rec.available_seats || 0) <= 5 ? 'text-red-600' : 'text-green-600'}`}>{rec.available_seats} seats</p></div>
                            <div><span className="text-gray-400">Occupancy</span><p className="font-bold">{rec.occupancy_percent?.toFixed(0) || 0}%</p></div>
                            <div><span className="text-gray-400">Hrs to Dep</span><p className="font-bold">{rec.hours_to_departure}h</p></div>
                            <div><span className="text-gray-400">Demand</span><p className="font-bold">{rec.demand_score}/100</p></div>
                            <div><span className="text-gray-400">Confidence</span><p className="font-bold">{rec.confidence}%</p></div>
                          </div>
                        </div>
                        <div>
                          <h4 className="text-xs font-semibold text-gray-700 uppercase tracking-wide mb-2">Revenue Impact</h4>
                          <div className="bg-white rounded-lg border border-gray-200 p-3 space-y-2 text-xs">
                            <div className="flex justify-between"><span className="text-gray-400">Current</span><span className="font-bold">₹{(rec.revenue_current || 0).toLocaleString()}</span></div>
                            <div className="flex justify-between"><span className="text-gray-400">Recommended</span><span className={`font-bold ${(rec.revenue_delta || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>₹{(rec.revenue_recommended || 0).toLocaleString()}</span></div>
                            <div className="flex justify-between border-t border-gray-100 pt-2"><span className="font-semibold text-gray-500">Delta</span><span className={`font-bold ${(rec.revenue_delta || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>{(rec.revenue_delta || 0) >= 0 ? '+' : ''}₹{(rec.revenue_delta || 0).toLocaleString()}</span></div>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                )}

                {/* ── Card Footer ── */}
                <div
                  className="flex items-center justify-between px-5 py-2.5 border-t bg-gray-50/50 text-sm cursor-pointer hover:bg-gray-100/50 transition-colors"
                  onClick={() => setExpandedCard(isExpanded ? null : cardKey)}
                >
                  <div className="flex items-center gap-4">
                    {rec && (
                      <>
                        <span className="text-gray-500">Current: <span className="font-semibold text-gray-900">{formatCurrency(rec.current_fare)}</span></span>
                        <ArrowRight className="h-3.5 w-3.5 text-gray-400" />
                        <span className="text-gray-500">Recommended: <span className={`font-bold ${action === 'RAISE' ? 'text-green-600' : action === 'LOWER' ? 'text-red-600' : 'text-gray-700'}`}>{formatCurrency(rec.recommended_fare)}</span></span>
                      </>
                    )}
                  </div>
                  <span className="flex items-center gap-1 text-xs text-gray-400">
                    {isExpanded ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
                    {isExpanded ? 'Collapse' : 'Factor details'}
                  </span>
                </div>
              </div>
            )
          })}
        </div>
      )}

      {/* ── Info Legend ── */}
      <div className="rounded-xl bg-blue-50 border border-blue-200 p-4">
        <div className="flex items-start gap-3">
          <Info className="h-5 w-5 text-blue-500 flex-shrink-0 mt-0.5" />
          <div className="text-xs text-blue-700 space-y-1">
            <p className="font-semibold">How AI Pricing Works</p>
            <p>
              The engine combines 7 factors: <strong>Cost Floor</strong> (₹40K min → ₹{summary.floor_fare || 1308}/seat),{' '}
              <strong>Time Curve</strong>, <strong>Velocity</strong>, <strong>Competitor</strong> (vs Vikram & VKaveri),{' '}
              <strong>Day Pattern</strong>, <strong>Event/Festival</strong>, and <strong>Scarcity</strong>.
              These are <strong>recommendations only</strong> — no fares change automatically.
            </p>
          </div>
        </div>
      </div>

      {/* ── Seat Layout Modal ── */}
      {seatModalGroup && (
        <AISeatBreakdownModal group={seatModalGroup} onClose={() => setSeatModalGroup(null)} />
      )}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Action Badge
// ─────────────────────────────────────────────────────────────────────────────
function ActionBadge({ action, changePct }: { action: string; changePct: number }) {
  if (action === 'RAISE') {
    return (
      <span className="inline-flex items-center gap-1 rounded-full bg-green-100 border border-green-200 px-3 py-1 text-xs font-bold text-green-700">
        <TrendingUp className="h-3.5 w-3.5" />RAISE {changePct > 0 ? `+${changePct.toFixed(1)}%` : ''}
      </span>
    )
  }
  if (action === 'LOWER') {
    return (
      <span className="inline-flex items-center gap-1 rounded-full bg-red-100 border border-red-200 px-3 py-1 text-xs font-bold text-red-700">
        <TrendingDown className="h-3.5 w-3.5" />LOWER {changePct.toFixed(1)}%
      </span>
    )
  }
  return (
    <span className="inline-flex items-center gap-1 rounded-full bg-gray-100 border border-gray-200 px-3 py-1 text-xs font-bold text-gray-600">
      <Minus className="h-3.5 w-3.5" />HOLD
    </span>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Operator Card Column
// ─────────────────────────────────────────────────────────────────────────────
function OperatorCard({ label, operator, services, recommendation, isMainOperator }: {
  label: string; operator: string; services: any[]; recommendation?: any; isMainOperator: boolean
}) {
  const colors = OPERATOR_COLORS[operator] || OPERATOR_COLORS.mythri

  if (services.length === 0) {
    return (
      <div className="p-4 flex flex-col items-center justify-center text-center min-h-[180px]">
        <span className={`inline-flex items-center rounded-md px-2.5 py-1 text-xs font-bold uppercase tracking-wider ${colors.badge} ${colors.badgeText} opacity-50`}>{label}</span>
        <p className="text-xs text-gray-400 mt-3">No data available</p>
      </div>
    )
  }

  const allMinFares = services.map((s: any) => s.min_fare).filter((f: number) => f > 0)
  const allMaxFares = services.map((s: any) => s.max_fare).filter((f: number) => f > 0)
  const minFare = allMinFares.length > 0 ? Math.min(...allMinFares) : 0
  const maxFare = allMaxFares.length > 0 ? Math.max(...allMaxFares) : 0
  const totalSeats = services.reduce((s: number, svc: any) => s + (svc.total_seats || 0), 0)
  const availableSeats = services.reduce((s: number, svc: any) => s + (svc.available_seats || 0), 0)

  return (
    <div className="p-4 space-y-3">
      <div className="flex items-center justify-between">
        <span className={`inline-flex items-center rounded-md px-2.5 py-1 text-xs font-bold uppercase tracking-wider ${colors.badge} ${colors.badgeText}`}>{label}</span>
        {isMainOperator && recommendation && (
          <span className={`text-[10px] font-bold uppercase tracking-wider ${
            recommendation.action === 'RAISE' ? 'text-green-600' : recommendation.action === 'LOWER' ? 'text-red-600' : 'text-gray-500'
          }`}>{recommendation.action === 'RAISE' ? '↑ Raise' : recommendation.action === 'LOWER' ? '↓ Lower' : '— Hold'}</span>
        )}
      </div>

      {services.map((svc: any, sIdx: number) => (
        <div key={sIdx} className="space-y-1">
          <div className="flex items-center gap-1.5">
            <Bus className="h-3.5 w-3.5 text-gray-400" />
            <span className="text-sm font-semibold text-gray-900">{svc.service_number || '-'}</span>
          </div>
          {svc.bus_type && <p className="text-xs text-gray-500 truncate" title={svc.bus_type}>{svc.bus_type}</p>}
          <div className="flex items-center justify-between text-xs">
            <div className="flex items-center gap-1">
              <Clock className="h-3 w-3 text-gray-400" />
              <span className="text-gray-700">{formatDepartureTime(svc.departure_time)}</span>
            </div>
            <span className={`font-medium px-2 py-0.5 rounded-full ${
              (svc.available_seats ?? 0) <= 5 ? 'bg-red-100 text-red-700' :
              (svc.available_seats ?? 0) <= 15 ? 'bg-yellow-100 text-yellow-700' : 'bg-green-100 text-green-700'
            }`}>{svc.available_seats ?? '-'} / {svc.total_seats ?? '-'} seats</span>
          </div>
        </div>
      ))}

      {/* Fare display */}
      <div className={`rounded-lg p-3 text-center ${
        isMainOperator && recommendation
          ? recommendation.action === 'RAISE' ? 'bg-green-50 border border-green-200'
            : recommendation.action === 'LOWER' ? 'bg-red-50 border border-red-200'
            : 'bg-gray-50 border border-gray-200'
          : 'bg-gray-50 border border-gray-200'
      }`}>
        <div className="text-xs text-gray-500 mb-1">{isMainOperator ? 'Current Fare' : 'Fare Range'}</div>
        <div className={`text-2xl font-bold ${isMainOperator ? 'text-blue-700' : 'text-gray-900'}`}>{formatCurrency(minFare)}</div>
        {maxFare > 0 && maxFare !== minFare && <div className="text-xs text-gray-400 mt-0.5">to {formatCurrency(maxFare)}</div>}
        {isMainOperator && recommendation && recommendation.action !== 'HOLD' && (
          <div className="mt-2 pt-2 border-t border-gray-200">
            <div className="text-xs text-gray-500">AI Recommended</div>
            <div className={`text-xl font-bold ${recommendation.action === 'RAISE' ? 'text-green-600' : 'text-red-600'}`}>
              {formatCurrency(recommendation.recommended_fare)}
            </div>
            <div className={`text-xs font-semibold ${recommendation.action === 'RAISE' ? 'text-green-600' : 'text-red-600'}`}>
              {recommendation.change_percent > 0 ? '+' : ''}{recommendation.change_percent.toFixed(1)}%
            </div>
          </div>
        )}
      </div>

      {isMainOperator && recommendation && totalSeats > 0 && (
        <div>
          <div className="flex justify-between text-[10px] text-gray-500 mb-1">
            <span>Occupancy</span><span>{recommendation.occupancy_percent?.toFixed(0) || 0}%</span>
          </div>
          <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
            <div className={`h-full rounded-full ${
              (recommendation.occupancy_percent || 0) > 70 ? 'bg-green-500' :
              (recommendation.occupancy_percent || 0) > 40 ? 'bg-yellow-500' : 'bg-red-400'
            }`} style={{ width: `${Math.min(recommendation.occupancy_percent || 0, 100)}%` }} />
          </div>
        </div>
      )}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Seat Breakdown Modal — with AI Recommendation overlay
// ─────────────────────────────────────────────────────────────────────────────

function AISeatBreakdownModal({ group, onClose }: { group: any; onClose: () => void }) {
  const [seatData, setSeatData] = useState<any>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const vikram = group.competitors?.find((c: any) => c.operator === 'vikram')
  const vkaveri = group.competitors?.find((c: any) => c.operator === 'vkaveri')
  const rec = group.recommendation

  useEffect(() => {
    async function fetchSeats() {
      setLoading(true)
      setError(null)
      try {
        const layouts: Record<string, any> = { mythri: null, vikram: null, vkaveri: null }
        const routeParams = `&route_from=${encodeURIComponent(group.route_from || '')}&route_to=${encodeURIComponent(group.route_to || '')}&travel_date=${encodeURIComponent(group.travel_date || '')}`

        const mythriId = group.mythri_trip_id
        if (mythriId) {
          const res = await fetch(`/api/pricing/competitor/seats?mythri_id=${mythriId}`)
          const data = await res.json()
          if (data.success) layouts.mythri = data.mythri_layout
        }

        if (vikram?.trip_id) {
          const res = await fetch(`/api/pricing/competitor/seats?operator=vikram&competitor_id=${encodeURIComponent(vikram.trip_id)}${routeParams}`)
          const data = await res.json()
          if (data.success) layouts.vikram = data.competitor_layout
        }

        if (vkaveri?.trip_id) {
          const res = await fetch(`/api/pricing/competitor/seats?operator=vkaveri&competitor_id=${vkaveri.trip_id}`)
          const data = await res.json()
          if (data.success) layouts.vkaveri = data.competitor_layout
        }

        setSeatData(layouts)
      } catch (err: any) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    fetchSeats()
  }, [group.mythri_trip_id, vikram?.trip_id, vkaveri?.trip_id, group.route_from, group.route_to, group.travel_date])

  function fmtTime(dt: string | null) {
    if (!dt) return '-'
    if (/^\d{2}:\d{2}$/.test(dt)) return dt
    try {
      return new Date(dt).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', hour12: true })
    } catch {
      return dt
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/50" onClick={onClose} />
      <div className="relative w-full max-w-7xl max-h-[92vh] bg-white rounded-xl shadow-2xl flex flex-col mx-4">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b bg-gradient-to-r from-blue-50 to-purple-50 rounded-t-xl">
          <div>
            <h2 className="text-lg font-bold text-gray-900">Bus Seat Layout</h2>
            <p className="text-sm text-gray-500">
              {group.route_from} → {group.route_to} &middot;{' '}
              {new Date((group.travel_date || '') + 'T00:00:00').toLocaleDateString('en-IN', { day: '2-digit', month: 'short', weekday: 'short' })}
            </p>
          </div>
          <div className="flex items-center gap-3">
            {/* AI Recommendation Badge in Modal Header */}
            {rec && rec.action !== 'HOLD' && (
              <div className={`rounded-lg px-3 py-2 text-xs ${
                rec.action === 'RAISE' ? 'bg-green-100 border border-green-200' : 'bg-red-100 border border-red-200'
              }`}>
                <span className={`font-bold ${rec.action === 'RAISE' ? 'text-green-700' : 'text-red-700'}`}>
                  AI: {rec.action === 'RAISE' ? '↑' : '↓'} {formatCurrency(rec.current_fare)} → {formatCurrency(rec.recommended_fare)}
                </span>
                <span className={`ml-1 text-[10px] ${rec.action === 'RAISE' ? 'text-green-600' : 'text-red-600'}`}>
                  ({rec.change_percent > 0 ? '+' : ''}{rec.change_percent.toFixed(1)}%)
                </span>
              </div>
            )}
            <button onClick={onClose} className="p-2 hover:bg-white/60 rounded-lg transition-colors">
              <X className="h-5 w-5 text-gray-500" />
            </button>
          </div>
        </div>

        {/* AI Recommendation Reason Banner */}
        {rec && rec.reason && rec.action !== 'HOLD' && (
          <div className={`px-5 py-2 text-sm border-b ${
            rec.action === 'RAISE' ? 'bg-green-50 border-green-100 text-green-800' : 'bg-red-50 border-red-100 text-red-800'
          }`}>
            <Zap className="inline h-3.5 w-3.5 mr-1" />
            <span className="font-medium">AI Recommendation:</span> {rec.reason}
          </div>
        )}

        {/* Body */}
        <div className="flex-1 overflow-y-auto p-5">
          {loading ? (
            <div className="flex flex-col items-center justify-center py-20">
              <Loader2 className="h-8 w-8 animate-spin text-blue-600 mb-3" />
              <p className="text-sm text-gray-500">Loading seat layouts...</p>
            </div>
          ) : error ? (
            <div className="flex flex-col items-center justify-center py-20 text-red-500">
              <AlertTriangle className="h-8 w-8 mb-2" />
              <p className="text-sm">{error}</p>
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
              <SeatLayoutColumn
                operator="mythri"
                label="Mythri"
                service={group.mythri_service}
                busType={group.mythri_bus_type}
                departure={group.mythri_departure}
                seats={group.mythri_available_seats}
                layout={seatData?.mythri}
                seatTypeFares={group.mythri_seat_type_fares}
                formatTime={fmtTime}
                recommendation={rec}
              />
              <SeatLayoutColumn
                operator="vikram"
                label="Vikram"
                service={vikram?.service}
                busType={vikram?.bus_type}
                departure={vikram?.departure}
                seats={vikram?.available_seats}
                layout={seatData?.vikram}
                seatTypeFares={vikram?.seat_type_fares}
                formatTime={fmtTime}
              />
              <SeatLayoutColumn
                operator="vkaveri"
                label="VKaveri"
                service={vkaveri?.service}
                busType={vkaveri?.bus_type}
                departure={vkaveri?.departure}
                seats={vkaveri?.available_seats}
                layout={seatData?.vkaveri}
                seatTypeFares={vkaveri?.seat_type_fares}
                formatTime={fmtTime}
              />
            </div>
          )}
        </div>

        {/* Footer — Legend */}
        <div className="border-t px-5 py-3 bg-gray-50 rounded-b-xl">
          <div className="flex items-center justify-center gap-6 text-xs text-gray-500">
            <span className="flex items-center gap-1.5">
              <span className="w-6 h-5 rounded border border-green-400 bg-green-50" />
              Available
            </span>
            <span className="flex items-center gap-1.5">
              <span className="w-6 h-5 rounded bg-red-500" />
              Booked
            </span>
            <span className="flex items-center gap-1.5">
              <span className="w-6 h-5 rounded bg-pink-400" />
              Ladies
            </span>
            {rec && rec.action !== 'HOLD' && (
              <span className="flex items-center gap-1.5 border-l pl-6 ml-2 border-gray-300">
                <span className={`w-6 h-5 rounded border-2 ${rec.action === 'RAISE' ? 'border-green-500 bg-green-50' : 'border-orange-500 bg-orange-50'}`} />
                AI Rec: {formatCurrency(rec.recommended_fare)}
              </span>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Seat Layout Column — renders one operator's bus seat map
// ─────────────────────────────────────────────────────────────────────────────

function SeatLayoutColumn({
  operator,
  label,
  service,
  busType,
  departure,
  seats,
  layout,
  seatTypeFares,
  formatTime,
  recommendation,
}: {
  operator: string
  label: string
  service?: string
  busType?: string
  departure?: string | null
  seats?: number | null
  layout: any[][] | null
  seatTypeFares?: any[]
  formatTime: (dt: string | null) => string
  recommendation?: any
}) {
  const [fareFilter, setFareFilter] = useState<number | null>(null)
  const colors = OPERATOR_COLORS[operator] || OPERATOR_COLORS.mythri

  if (!service) {
    return (
      <div className="rounded-lg border border-dashed border-gray-200 p-4 flex flex-col items-center justify-center min-h-[300px]">
        <span className={`inline-flex items-center rounded-md px-2.5 py-1 text-xs font-bold uppercase tracking-wider ${colors.badge} ${colors.badgeText} opacity-50`}>{label}</span>
        <p className="text-sm text-gray-400 mt-2">No matching trip</p>
      </div>
    )
  }

  const processed = layout && Array.isArray(layout) ? processCoachLayout(layout) : null
  const safeSeatTypeFares = Array.isArray(seatTypeFares) ? seatTypeFares : []

  return (
    <div className={`rounded-lg border ${colors.border} overflow-hidden`}>
      {/* Operator Header */}
      <div className={`p-3 ${colors.bg} border-b ${colors.border}`}>
        <div className="flex items-center justify-between mb-1">
          <span className={`inline-flex items-center rounded-md px-2 py-0.5 text-xs font-bold uppercase tracking-wider ${colors.badge} ${colors.badgeText}`}>{label}</span>
          <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${
            (seats ?? 0) <= 5 ? 'bg-red-100 text-red-700' : 'bg-green-100 text-green-700'
          }`}>{seats ?? '-'} seats</span>
        </div>
        <div className="flex items-center gap-1.5">
          <Bus className="h-3 w-3 text-gray-400" />
          <span className="text-sm font-semibold text-gray-800">{service}</span>
        </div>
        <p className="text-xs text-gray-500 truncate">{busType || '-'}</p>
        <div className="flex items-center gap-1 mt-1">
          <Clock className="h-3 w-3 text-gray-400" />
          <span className="text-xs text-gray-600">{formatTime(departure || null)}</span>
        </div>
        {/* AI Recommendation in Mythri header */}
        {recommendation && operator === 'mythri' && recommendation.action !== 'HOLD' && (
          <div className={`mt-2 rounded px-2 py-1 text-xs font-bold ${
            recommendation.action === 'RAISE' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
          }`}>
            AI: {formatCurrency(recommendation.current_fare)} → {formatCurrency(recommendation.recommended_fare)}
            <span className="ml-1 font-medium">
              ({recommendation.change_percent > 0 ? '+' : ''}{recommendation.change_percent.toFixed(1)}%)
            </span>
          </div>
        )}
      </div>

      {/* Seat Grid */}
      <div className="p-3">
        {processed && (processed.lower.length > 0 || processed.upper.length > 0) ? (
          <div className="space-y-3">
            {processed.fareTiers.length > 0 && (
              <div className="flex items-center gap-1 flex-wrap">
                <span className="text-[10px] font-semibold text-gray-500 mr-1">Seat Price</span>
                <button
                  onClick={() => setFareFilter(null)}
                  className={`px-2 py-0.5 rounded text-[10px] font-semibold transition-colors ${
                    fareFilter === null ? 'bg-gray-800 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                >All</button>
                {processed.fareTiers.map((f) => (
                  <button
                    key={f}
                    onClick={() => setFareFilter(f)}
                    className={`px-2 py-0.5 rounded text-[10px] font-semibold transition-colors ${
                      fareFilter === f ? 'bg-gray-800 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                    }`}
                  >{f}</button>
                ))}
              </div>
            )}

            <div className="flex gap-3 overflow-x-auto">
              {processed.lower.length > 0 && (
                <div className="flex-1 min-w-0 overflow-x-auto">
                  <p className="text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-2 text-center">Lower</p>
                  <BusSeatGrid rows={processed.lower} fareFilter={fareFilter} recommendation={operator === 'mythri' ? recommendation : undefined} />
                </div>
              )}
              {processed.upper.length > 0 && (
                <div className="flex-1 min-w-0 overflow-x-auto">
                  <p className="text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-2 text-center">Upper</p>
                  <BusSeatGrid rows={processed.upper} fareFilter={fareFilter} recommendation={operator === 'mythri' ? recommendation : undefined} />
                </div>
              )}
            </div>
          </div>
        ) : safeSeatTypeFares.length > 0 ? (
          <div>
            <p className="text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-2">Fare by Seat Type</p>
            <div className="space-y-1.5">
              {safeSeatTypeFares.map((sf: any, i: number) => (
                <div key={i} className="flex items-center justify-between rounded border border-gray-200 px-2.5 py-1.5">
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-semibold text-gray-700">{sf?.seat_type || sf?.seat_name || '-'}</span>
                    {(sf?.available_count || 0) > 0 && <span className="text-[10px] text-gray-400">{sf.available_count} avl</span>}
                  </div>
                  <span className="text-xs font-bold text-gray-900">{formatCurrency(sf?.fare || 0)}</span>
                </div>
              ))}
            </div>
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center py-8 text-gray-400">
            <LayoutGrid className="h-8 w-8 mb-1" />
            <p className="text-xs">No seat data</p>
          </div>
        )}
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Bus Seat Grid — renders seat cells with AI recommendation overlay for Mythri
// ─────────────────────────────────────────────────────────────────────────────

function BusSeatGrid({ rows, fareFilter, recommendation }: { rows: any[][]; fareFilter: number | null; recommendation?: any }) {
  if (!rows || !Array.isArray(rows)) return null

  // Calculate per-seat recommended fare if we have a recommendation
  // The AI recommendation is at the service level, so we calculate the ratio
  // and apply it proportionally to each seat's current fare
  const fareRatio = recommendation && recommendation.current_fare > 0
    ? recommendation.recommended_fare / recommendation.current_fare
    : null

  return (
    <div className="space-y-1.5">
      {rows.map((row, rowIdx) => {
        if (!Array.isArray(row)) return null
        return (
          <div key={rowIdx} className="flex gap-1 justify-center items-center">
            {row.map((seat: any, colIdx: number) => {
              if (!seat || seat._gangway) {
                return <div key={colIdx} className="w-2.5 shrink-0" />
              }
              if (!seat.seat_number) {
                return <div key={colIdx} className="w-10 h-11 shrink-0" />
              }

              const status = getSeatStatus(seat)
              const fare = typeof seat.fare === 'number' ? Math.round(seat.fare) : 0
              const isAvailable = status === 'available'
              const isLadies = status === 'ladies'

              const matchesFareFilter = fareFilter === null || (isAvailable && fare === fareFilter)
              const dimmed = fareFilter !== null && !matchesFareFilter

              // Calculate recommended fare for this specific seat
              const seatRecFare = isAvailable && fare > 0 && fareRatio
                ? Math.round(fare * fareRatio)
                : null
              const hasRecommendation = seatRecFare !== null && seatRecFare !== fare

              let seatClasses: string
              if (isAvailable) {
                seatClasses = dimmed
                  ? 'bg-gray-50 border-gray-200 opacity-40'
                  : hasRecommendation && recommendation?.action === 'RAISE'
                    ? 'bg-green-50 border-green-500 border-2'
                    : hasRecommendation && recommendation?.action === 'LOWER'
                      ? 'bg-orange-50 border-orange-400 border-2'
                      : 'bg-green-50 border-green-400'
              } else if (isLadies) {
                seatClasses = 'bg-pink-400 border-pink-500'
              } else {
                seatClasses = 'bg-red-500 border-red-600'
              }

              // Taller seat cell when showing recommendation
              const heightClass = hasRecommendation && !dimmed ? 'h-14' : 'h-11'

              return (
                <div
                  key={colIdx}
                  className={`w-10 ${heightClass} shrink-0 rounded border-[1.5px] flex flex-col items-center justify-center transition-opacity ${seatClasses}`}
                  title={`${seat.seat_number}${isAvailable && fare > 0 ? ` — Current: ₹${fare}` : isLadies ? ' — Ladies' : ' — Booked'}${hasRecommendation ? ` → Rec: ₹${seatRecFare}` : ''}`}
                >
                  <span className={`text-[9px] font-bold leading-none ${isAvailable ? 'text-gray-700' : 'text-white'}`}>
                    {seat.seat_number}
                  </span>
                  {isAvailable && fare > 0 && (
                    <span className={`text-[8px] font-semibold leading-none mt-0.5 ${
                      hasRecommendation ? 'text-gray-400 line-through' : 'text-green-700'
                    }`}>
                      ₹{fare}
                    </span>
                  )}
                  {hasRecommendation && !dimmed && (
                    <span className={`text-[8px] font-bold leading-none mt-0.5 ${
                      recommendation?.action === 'RAISE' ? 'text-green-700' : 'text-orange-700'
                    }`}>
                      ₹{seatRecFare}
                    </span>
                  )}
                </div>
              )
            })}
          </div>
        )
      })}
    </div>
  )
}
