import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * GET /api/pricing/competitor/analytics
 * Historical/calendar analytics data for competitor pricing sub-tabs.
 *
 * Query params:
 *   ?view=calendar|history|overview   Which analytics view to return (required)
 *   ?route=From|To                    Filter by route (optional)
 *   ?days=30                          Number of days to look ahead/back (default: 30)
 */
export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)

    const view = searchParams.get('view')
    const route = searchParams.get('route')
    const days = parseInt(searchParams.get('days') || '30', 10)

    if (!view || !['calendar', 'history', 'overview'].includes(view)) {
      return NextResponse.json(
        { success: false, error: 'Invalid or missing "view" param. Use: calendar, history, or overview.' },
        { status: 400 }
      )
    }

    // Parse route filter
    let routeFrom: string | null = null
    let routeTo: string | null = null
    if (route) {
      const parts = route.split('|')
      routeFrom = parts[0] || null
      routeTo = parts[1] || null
    }

    switch (view) {
      case 'calendar':
        return await handleCalendarView(supabase, routeFrom, routeTo, days)
      case 'history':
        return await handleHistoryView(supabase, routeFrom, routeTo, days)
      case 'overview':
        return await handleOverviewView(supabase, routeFrom, routeTo, days)
      default:
        return NextResponse.json({ success: false, error: 'Unknown view' }, { status: 400 })
    }
  } catch (error: any) {
    console.error('[Competitor Analytics API] Error:', error.message)
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    )
  }
}

/**
 * Calendar view: competitor fares for the next N days, grouped by date.
 * Returns per-date summaries with each operator's min/avg fare and seat availability.
 */
async function handleCalendarView(
  supabase: any,
  routeFrom: string | null,
  routeTo: string | null,
  days: number
) {
  const today = new Date().toISOString().split('T')[0]
  const endDate = new Date()
  endDate.setDate(endDate.getDate() + days)
  const end = endDate.toISOString().split('T')[0]

  let query = supabase
    .from('jc_competitor_fares')
    .select('travel_date, operator, min_fare, max_fare, avg_fare, available_seats, total_seats, bus_type, service_number, departure_time')
    .gte('travel_date', today)
    .lte('travel_date', end)
    .order('travel_date', { ascending: true })

  if (routeFrom) query = query.eq('route_from', routeFrom)
  if (routeTo) query = query.eq('route_to', routeTo)

  const { data: fares, error } = await query

  if (error) {
    throw new Error(`Calendar query failed: ${error.message}`)
  }

  // Group by date
  const dateMap: Record<string, any> = {}

  for (const fare of (fares || [])) {
    const date = fare.travel_date
    if (!dateMap[date]) {
      dateMap[date] = {
        date,
        operators: {},
        total_services: 0,
        total_available_seats: 0,
      }
    }

    const entry = dateMap[date]
    const op = fare.operator || 'unknown'

    if (!entry.operators[op]) {
      entry.operators[op] = {
        operator: op,
        min_fare: Infinity,
        max_fare: -Infinity,
        fare_sum: 0,
        fare_count: 0,
        available_seats: 0,
        total_seats: 0,
        service_count: 0,
        services: [],
      }
    }

    const opEntry = entry.operators[op]
    opEntry.min_fare = Math.min(opEntry.min_fare, fare.min_fare || Infinity)
    opEntry.max_fare = Math.max(opEntry.max_fare, fare.max_fare || -Infinity)
    opEntry.fare_sum += fare.avg_fare || 0
    opEntry.fare_count += 1
    opEntry.available_seats += fare.available_seats || 0
    opEntry.total_seats += fare.total_seats || 0
    opEntry.service_count += 1
    opEntry.services.push({
      service_number: fare.service_number,
      bus_type: fare.bus_type,
      departure_time: fare.departure_time,
      min_fare: fare.min_fare,
      max_fare: fare.max_fare,
      avg_fare: fare.avg_fare,
      available_seats: fare.available_seats,
    })

    entry.total_services += 1
    entry.total_available_seats += fare.available_seats || 0
  }

  // Finalize operator averages and convert to array
  const calendar = Object.values(dateMap).map((entry: any) => {
    const operators = Object.values(entry.operators).map((op: any) => ({
      operator: op.operator,
      min_fare: op.min_fare === Infinity ? null : op.min_fare,
      max_fare: op.max_fare === -Infinity ? null : op.max_fare,
      avg_fare: op.fare_count > 0 ? Math.round((op.fare_sum / op.fare_count) * 100) / 100 : null,
      available_seats: op.available_seats,
      total_seats: op.total_seats,
      service_count: op.service_count,
      services: op.services,
    }))

    return {
      date: entry.date,
      operators,
      total_services: entry.total_services,
      total_available_seats: entry.total_available_seats,
    }
  })

  return NextResponse.json({
    success: true,
    view: 'calendar',
    days,
    count: calendar.length,
    calendar,
  })
}

/**
 * History view: recent competitor fare scrapes grouped by batch,
 * showing price changes over time for each operator.
 */
async function handleHistoryView(
  supabase: any,
  routeFrom: string | null,
  routeTo: string | null,
  days: number
) {
  const cutoff = new Date()
  cutoff.setDate(cutoff.getDate() - days)
  const cutoffDate = cutoff.toISOString()

  let query = supabase
    .from('jc_competitor_fares')
    .select('id, scrape_batch_id, operator, route_from, route_to, travel_date, service_number, bus_type, departure_time, min_fare, max_fare, avg_fare, available_seats, total_seats, fare_by_seat_type, scraped_at')
    .gte('scraped_at', cutoffDate)
    .order('scraped_at', { ascending: false })
    .limit(500)

  if (routeFrom) query = query.eq('route_from', routeFrom)
  if (routeTo) query = query.eq('route_to', routeTo)

  const { data: fares, error } = await query

  if (error) {
    throw new Error(`History query failed: ${error.message}`)
  }

  // Group by scrape_batch_id
  const batchMap: Record<string, any> = {}

  for (const fare of (fares || [])) {
    const batchId = fare.scrape_batch_id || 'unknown'
    if (!batchMap[batchId]) {
      batchMap[batchId] = {
        scrape_batch_id: batchId,
        scraped_at: fare.scraped_at,
        operators: {},
        total_entries: 0,
      }
    }

    const batch = batchMap[batchId]
    const op = fare.operator || 'unknown'

    if (!batch.operators[op]) {
      batch.operators[op] = {
        operator: op,
        entries: [],
        min_fare: Infinity,
        max_fare: -Infinity,
        fare_sum: 0,
        fare_count: 0,
      }
    }

    const opEntry = batch.operators[op]
    opEntry.entries.push({
      id: fare.id,
      service_number: fare.service_number,
      bus_type: fare.bus_type,
      departure_time: fare.departure_time,
      travel_date: fare.travel_date,
      route_from: fare.route_from,
      route_to: fare.route_to,
      min_fare: fare.min_fare,
      max_fare: fare.max_fare,
      avg_fare: fare.avg_fare,
      available_seats: fare.available_seats,
      total_seats: fare.total_seats,
      fare_by_seat_type: fare.fare_by_seat_type,
    })
    opEntry.min_fare = Math.min(opEntry.min_fare, fare.min_fare || Infinity)
    opEntry.max_fare = Math.max(opEntry.max_fare, fare.max_fare || -Infinity)
    opEntry.fare_sum += fare.avg_fare || 0
    opEntry.fare_count += 1

    batch.total_entries += 1
    // Keep the earliest scraped_at per batch for accurate ordering
    if (fare.scraped_at < batch.scraped_at) {
      batch.scraped_at = fare.scraped_at
    }
  }

  // Finalize and convert to sorted array (most recent first)
  const batches = Object.values(batchMap)
    .map((batch: any) => ({
      scrape_batch_id: batch.scrape_batch_id,
      scraped_at: batch.scraped_at,
      total_entries: batch.total_entries,
      operators: Object.values(batch.operators).map((op: any) => ({
        operator: op.operator,
        entry_count: op.entries.length,
        min_fare: op.min_fare === Infinity ? null : op.min_fare,
        max_fare: op.max_fare === -Infinity ? null : op.max_fare,
        avg_fare: op.fare_count > 0 ? Math.round((op.fare_sum / op.fare_count) * 100) / 100 : null,
        entries: op.entries,
      })),
    }))
    .sort((a: any, b: any) => new Date(b.scraped_at).getTime() - new Date(a.scraped_at).getTime())
    .slice(0, 50)

  return NextResponse.json({
    success: true,
    view: 'history',
    days,
    count: batches.length,
    batches,
  })
}

/**
 * Overview view: route-level stats from fare comparisons.
 * Average fares by operator, comparison counts, and fare trends.
 */
async function handleOverviewView(
  supabase: any,
  routeFrom: string | null,
  routeTo: string | null,
  days: number
) {
  const cutoff = new Date()
  cutoff.setDate(cutoff.getDate() - days)
  const cutoffDate = cutoff.toISOString().split('T')[0]

  let query = supabase
    .from('jc_fare_comparisons')
    .select('route_from, route_to, travel_date, mythri_min_fare, mythri_max_fare, mythri_avg_fare, mythri_available_seats, vikram_min_fare, vikram_max_fare, vikram_avg_fare, vikram_available_seats, fare_difference, recommended_mythri_fare, undercut_amount, savings_percent, status, competitor_operator, created_at')
    .gte('travel_date', cutoffDate)
    .order('created_at', { ascending: false })

  if (routeFrom) query = query.eq('route_from', routeFrom)
  if (routeTo) query = query.eq('route_to', routeTo)

  const { data: comparisons, error } = await query

  if (error) {
    throw new Error(`Overview query failed: ${error.message}`)
  }

  // Aggregate by route
  const routeMap: Record<string, any> = {}

  for (const c of (comparisons || [])) {
    const routeKey = `${c.route_from}|${c.route_to}`
    if (!routeMap[routeKey]) {
      routeMap[routeKey] = {
        route_from: c.route_from,
        route_to: c.route_to,
        comparison_count: 0,
        status_counts: { pending: 0, applied: 0, dismissed: 0 },
        mythri: { fare_sum: 0, fare_count: 0, min_fare: Infinity, max_fare: -Infinity, total_seats: 0 },
        competitors: {} as Record<string, { fare_sum: number; fare_count: number; min_fare: number; max_fare: number; total_seats: number }>,
        fare_differences: [] as number[],
        undercut_amounts: [] as number[],
        savings_percents: [] as number[],
        date_range: { earliest: c.travel_date, latest: c.travel_date },
      }
    }

    const entry = routeMap[routeKey]
    entry.comparison_count += 1

    // Status counts
    const status = c.status || 'pending'
    entry.status_counts[status] = (entry.status_counts[status] || 0) + 1

    // Mythri stats
    if (c.mythri_avg_fare) {
      entry.mythri.fare_sum += c.mythri_avg_fare
      entry.mythri.fare_count += 1
      entry.mythri.min_fare = Math.min(entry.mythri.min_fare, c.mythri_min_fare || Infinity)
      entry.mythri.max_fare = Math.max(entry.mythri.max_fare, c.mythri_max_fare || -Infinity)
    }
    entry.mythri.total_seats += c.mythri_available_seats || 0

    // Competitor stats (grouped by operator)
    const compOp = c.competitor_operator || 'competitor'
    if (!entry.competitors[compOp]) {
      entry.competitors[compOp] = { fare_sum: 0, fare_count: 0, min_fare: Infinity, max_fare: -Infinity, total_seats: 0 }
    }
    const comp = entry.competitors[compOp]
    if (c.vikram_avg_fare) {
      comp.fare_sum += c.vikram_avg_fare
      comp.fare_count += 1
      comp.min_fare = Math.min(comp.min_fare, c.vikram_min_fare || Infinity)
      comp.max_fare = Math.max(comp.max_fare, c.vikram_max_fare || -Infinity)
    }
    comp.total_seats += c.vikram_available_seats || 0

    // Differences
    if (c.fare_difference != null) entry.fare_differences.push(c.fare_difference)
    if (c.undercut_amount != null) entry.undercut_amounts.push(c.undercut_amount)
    if (c.savings_percent != null) entry.savings_percents.push(c.savings_percent)

    // Date range
    if (c.travel_date < entry.date_range.earliest) entry.date_range.earliest = c.travel_date
    if (c.travel_date > entry.date_range.latest) entry.date_range.latest = c.travel_date
  }

  // Finalize route stats
  const routes = Object.values(routeMap).map((entry: any) => {
    const avg = (arr: number[]) =>
      arr.length > 0 ? Math.round((arr.reduce((s: number, v: number) => s + v, 0) / arr.length) * 100) / 100 : null

    return {
      route_from: entry.route_from,
      route_to: entry.route_to,
      comparison_count: entry.comparison_count,
      status_counts: entry.status_counts,
      date_range: entry.date_range,
      mythri: {
        avg_fare: entry.mythri.fare_count > 0 ? Math.round((entry.mythri.fare_sum / entry.mythri.fare_count) * 100) / 100 : null,
        min_fare: entry.mythri.min_fare === Infinity ? null : entry.mythri.min_fare,
        max_fare: entry.mythri.max_fare === -Infinity ? null : entry.mythri.max_fare,
        total_available_seats: entry.mythri.total_seats,
      },
      competitors: Object.entries(entry.competitors).map(([op, stats]: [string, any]) => ({
        operator: op,
        avg_fare: stats.fare_count > 0 ? Math.round((stats.fare_sum / stats.fare_count) * 100) / 100 : null,
        min_fare: stats.min_fare === Infinity ? null : stats.min_fare,
        max_fare: stats.max_fare === -Infinity ? null : stats.max_fare,
        total_available_seats: stats.total_seats,
        comparison_count: stats.fare_count,
      })),
      trends: {
        avg_fare_difference: avg(entry.fare_differences),
        avg_undercut_amount: avg(entry.undercut_amounts),
        avg_savings_percent: avg(entry.savings_percents),
        costlier_count: entry.fare_differences.filter((d: number) => d > 0).length,
        cheaper_count: entry.fare_differences.filter((d: number) => d < 0).length,
        equal_count: entry.fare_differences.filter((d: number) => d === 0).length,
      },
    }
  })

  // Global summary
  const allDiffs = (comparisons || []).filter((c: any) => c.fare_difference != null).map((c: any) => c.fare_difference)
  const totalComparisons = (comparisons || []).length
  const globalAvgDiff = allDiffs.length > 0
    ? Math.round((allDiffs.reduce((s: number, v: number) => s + v, 0) / allDiffs.length) * 100) / 100
    : null

  return NextResponse.json({
    success: true,
    view: 'overview',
    days,
    summary: {
      total_comparisons: totalComparisons,
      routes_count: routes.length,
      avg_fare_difference: globalAvgDiff,
      costlier_count: allDiffs.filter((d: number) => d > 0).length,
      cheaper_count: allDiffs.filter((d: number) => d < 0).length,
    },
    routes,
  })
}
