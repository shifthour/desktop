import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'
import {
  scrapeMythriTrips,
  scrapeVikramTrips,
  scrapeVkaveriTrips,
  compareTrips,
  getUpcomingDates,
  type RouteConfig,
  type ScrapedTrip,
  type OperatorName,
} from '@/lib/competitor-scraper'

export const maxDuration = 120

const CRON_SECRET = process.env.CRON_SECRET || 'mythri-cron-secret-2024'

/**
 * GET /api/cron/competitor-scrape
 * Scrapes fare data from mythribus.com, vikramtravels.com, and vkaveribus.com
 * for all configured overlapping routes, compares fares,
 * and generates undercut recommendations.
 *
 * Optional query params:
 *   ?days=1        Number of days ahead to scrape (default: 1, max 30)
 *   ?undercut=30   Undercut amount in ₹ (default: 30)
 */
export async function GET(request: NextRequest) {
  const startTime = Date.now()

  try {
    const cronSecret = request.headers.get('x-cron-secret')
    if (cronSecret !== CRON_SECRET) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const { searchParams } = new URL(request.url)
    const specificDate = searchParams.get('date') // e.g. 2026-02-20
    const daysAhead = Math.min(parseInt(searchParams.get('days') || '1', 10), 30)
    const undercutAmount = parseInt(searchParams.get('undercut') || '30', 10)

    const supabase = createServerClient()
    const batchId = crypto.randomUUID()
    const dates = specificDate ? [specificDate] : getUpcomingDates(daysAhead)

    console.log(`[Competitor Scrape] Starting batch ${batchId} for ${daysAhead} days, undercut=₹${undercutAmount}`)

    // Fetch active route configurations
    const { data: routeConfigs, error: routeError } = await supabase
      .from('jc_competitor_route_config')
      .select('*')
      .eq('is_active', true)

    if (routeError) {
      throw new Error(`Failed to fetch route configs: ${routeError.message}`)
    }

    if (!routeConfigs || routeConfigs.length === 0) {
      return NextResponse.json({
        success: true,
        message: 'No active route configurations found.',
        scraped: 0,
      })
    }

    console.log(`[Competitor Scrape] Found ${routeConfigs.length} active routes, ${dates.length} dates`)

    let totalMythriTrips = 0
    let totalVikramTrips = 0
    let totalVkaveriTrips = 0
    let totalComparisons = 0
    let totalErrors = 0
    const allFareRecords: any[] = []
    const allComparisonRecords: any[] = []

    // Build all route-date combinations
    const tasks: Array<{ route: RouteConfig; date: string }> = []
    for (const route of routeConfigs as RouteConfig[]) {
      for (const date of dates) {
        tasks.push({ route, date })
      }
    }

    // Process in parallel batches of 5
    const BATCH_SIZE = 5
    for (let i = 0; i < tasks.length; i += BATCH_SIZE) {
      const batch = tasks.slice(i, i + BATCH_SIZE)

      const results = await Promise.allSettled(
        batch.map(async ({ route, date }) => {
          // Scrape all operators in parallel
          const scrapePromises: Promise<ScrapedTrip[]>[] = [
            scrapeMythriTrips(route.mythri_from_id, route.mythri_to_id, date, route.route_from, route.route_to),
          ]

          // Only scrape Vikram if codes are configured
          const hasVikram = route.vikram_from_code && route.vikram_to_code
          if (hasVikram) {
            scrapePromises.push(
              scrapeVikramTrips(route.vikram_from_code, route.vikram_to_code, date, route.route_from, route.route_to)
            )
          }

          // Only scrape VKaveri if IDs are configured
          const hasVkaveri = route.vkaveri_from_id && route.vkaveri_to_id
          if (hasVkaveri) {
            scrapePromises.push(
              scrapeVkaveriTrips(route.vkaveri_from_id, route.vkaveri_to_id, date, route.route_from, route.route_to)
            )
          }

          const scrapeResults = await Promise.all(scrapePromises)

          const mythriTrips = scrapeResults[0]
          let idx = 1
          const vikramTrips = hasVikram ? scrapeResults[idx++] : []
          const vkaveriTrips = hasVkaveri ? scrapeResults[idx++] : []

          return { route, date, mythriTrips, vikramTrips, vkaveriTrips }
        })
      )

      for (const result of results) {
        if (result.status === 'rejected') {
          console.error(`[Competitor Scrape] Batch error: ${result.reason}`)
          totalErrors++
          continue
        }

        const { route, date, mythriTrips, vikramTrips, vkaveriTrips } = result.value
        totalMythriTrips += mythriTrips.length
        totalVikramTrips += vikramTrips.length
        totalVkaveriTrips += vkaveriTrips.length

        // Store raw fare records
        const toFareRecord = (trip: ScrapedTrip) => ({
          scrape_batch_id: batchId,
          operator: trip.operator,
          route_from: trip.route_from,
          route_to: trip.route_to,
          travel_date: trip.travel_date,
          service_number: trip.service_number,
          bus_type: trip.bus_type,
          departure_time: trip.departure_time,
          arrival_time: trip.arrival_time,
          trip_identifier: trip.trip_identifier,
          total_seats: trip.total_seats,
          available_seats: trip.available_seats,
          min_fare: trip.min_fare,
          max_fare: trip.max_fare,
          avg_fare: trip.avg_fare,
          fare_by_seat_type: trip.fare_by_seat_type,
          seat_fares: trip.seat_fares,
          scraped_at: new Date().toISOString(),
        })

        allFareRecords.push(...mythriTrips.map(toFareRecord))
        allFareRecords.push(...vikramTrips.map(toFareRecord))
        allFareRecords.push(...vkaveriTrips.map(toFareRecord))

        // Compare Mythri vs each competitor
        const competitors: Array<{ trips: ScrapedTrip[]; operator: OperatorName }> = []
        if (vikramTrips.length > 0) competitors.push({ trips: vikramTrips, operator: 'vikram' })
        if (vkaveriTrips.length > 0) competitors.push({ trips: vkaveriTrips, operator: 'vkaveri' })

        for (const { trips: compTrips, operator: compOp } of competitors) {
          const comparisons = compareTrips(mythriTrips, compTrips, compOp, undercutAmount)
          totalComparisons += comparisons.length

          for (const comp of comparisons) {
            allComparisonRecords.push({
              scrape_batch_id: batchId,
              competitor_operator: comp.competitor_operator,
              route_from: comp.route_from,
              route_to: comp.route_to,
              travel_date: comp.travel_date,
              mythri_service: comp.mythri.service_number,
              mythri_departure: comp.mythri.departure_time,
              mythri_bus_type: comp.mythri.bus_type,
              mythri_min_fare: comp.mythri.min_fare,
              mythri_max_fare: comp.mythri.max_fare,
              mythri_avg_fare: comp.mythri.avg_fare,
              mythri_available_seats: comp.mythri.available_seats,
              vikram_service: comp.competitor.service_number,
              vikram_departure: comp.competitor.departure_time,
              vikram_bus_type: comp.competitor.bus_type,
              vikram_min_fare: comp.competitor.min_fare,
              vikram_max_fare: comp.competitor.max_fare,
              vikram_avg_fare: comp.competitor.avg_fare,
              vikram_available_seats: comp.competitor.available_seats,
              fare_difference: comp.fare_difference,
              recommended_mythri_fare: comp.recommended_mythri_fare,
              undercut_amount: comp.undercut_amount,
              savings_percent: comp.savings_percent,
              seat_type_comparison: comp.seat_type_comparison,
              mythri_seat_type_fares: comp.mythri.fare_by_seat_type,
              competitor_seat_type_fares: comp.competitor.fare_by_seat_type,
              mythri_trip_id: comp.mythri.trip_identifier,
              competitor_trip_id: comp.competitor.trip_identifier,
              status: 'pending',
            })
          }
        }
      }
    }

    // Batch insert fare records
    let faresInserted = 0
    for (let i = 0; i < allFareRecords.length; i += 50) {
      const batch = allFareRecords.slice(i, i + 50)
      const { error: insertErr } = await supabase.from('jc_competitor_fares').insert(batch)
      if (insertErr) {
        console.error(`[Competitor Scrape] Fare insert error: ${insertErr.message}`)
        totalErrors++
      } else {
        faresInserted += batch.length
      }
    }

    // Expire ALL old pending comparisons for the scraped dates
    for (const date of dates) {
      await supabase
        .from('jc_fare_comparisons')
        .update({ status: 'expired' })
        .eq('travel_date', date)
        .eq('status', 'pending')
    }

    // Batch insert comparison records
    let comparisonsInserted = 0
    for (let i = 0; i < allComparisonRecords.length; i += 50) {
      const batch = allComparisonRecords.slice(i, i + 50)
      const { error: insertErr } = await supabase.from('jc_fare_comparisons').insert(batch)
      if (insertErr) {
        console.error(`[Competitor Scrape] Comparison insert error: ${insertErr.message}`)
        totalErrors++
      } else {
        comparisonsInserted += batch.length
      }
    }

    const latency = Date.now() - startTime

    console.log(
      `[Competitor Scrape] Done: ${faresInserted} fares, ${comparisonsInserted} comparisons, ${totalErrors} errors in ${latency}ms`
    )

    await supabase.from('jc_sync_jobs').insert({
      sync_type: 'competitor_scrape',
      target_date: dates[0],
      status: totalErrors > 0 ? 'completed_with_errors' : 'completed',
      started_at: new Date(startTime).toISOString(),
      completed_at: new Date().toISOString(),
      records_fetched: totalMythriTrips + totalVikramTrips + totalVkaveriTrips,
      records_created: comparisonsInserted,
      records_failed: totalErrors,
      details: {
        batch_id: batchId,
        routes_scraped: routeConfigs.length,
        dates_scraped: dates,
        mythri_trips: totalMythriTrips,
        vikram_trips: totalVikramTrips,
        vkaveri_trips: totalVkaveriTrips,
        fares_stored: faresInserted,
        comparisons_generated: comparisonsInserted,
        undercut_amount: undercutAmount,
        latency_ms: latency,
      },
    })

    return NextResponse.json({
      success: true,
      batch_id: batchId,
      routes_scraped: routeConfigs.length,
      dates_scraped: dates,
      mythri_trips: totalMythriTrips,
      vikram_trips: totalVikramTrips,
      vkaveri_trips: totalVkaveriTrips,
      fares_stored: faresInserted,
      comparisons_generated: comparisonsInserted,
      errors: totalErrors,
      latency_ms: latency,
      timestamp: new Date().toISOString(),
    })
  } catch (error: any) {
    console.error('[Competitor Scrape] Error:', error.message)
    return NextResponse.json(
      { success: false, error: error.message, timestamp: new Date().toISOString() },
      { status: 500 }
    )
  }
}

export async function POST(request: NextRequest) {
  return GET(request)
}
