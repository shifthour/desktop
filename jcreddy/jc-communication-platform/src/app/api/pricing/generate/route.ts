import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'
import {
  generateRecommendation,
  DEFAULT_CONFIG,
  type GlobalConfig,
  type PricingRuleInput,
  type DemandSignals,
} from '@/lib/pricing-engine'

/**
 * POST /api/pricing/generate
 * Triggers recommendation generation for upcoming trips.
 * Optional query: ?trip_id=xxx to generate for a single trip.
 */
export async function POST(request: NextRequest) {
  const startTime = Date.now()

  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)
    const singleTripId = searchParams.get('trip_id')

    console.log('[Pricing Generate] Starting recommendation generation...')

    // 1. Fetch global config
    const { data: configRow } = await supabase
      .from('jc_pricing_config')
      .select('config_value')
      .eq('config_key', 'global_settings')
      .eq('is_active', true)
      .single()

    const config: GlobalConfig = configRow?.config_value
      ? { ...DEFAULT_CONFIG, ...configRow.config_value }
      : DEFAULT_CONFIG

    if (!config.pricing_enabled) {
      return NextResponse.json({
        success: false,
        message: 'Dynamic pricing is disabled. Enable it in config to generate recommendations.',
        generated: 0,
      })
    }

    // 2. Fetch active rules
    const { data: rulesData, error: rulesError } = await supabase
      .from('jc_pricing_rules')
      .select('*')
      .eq('is_active', true)
      .order('priority', { ascending: true })

    if (rulesError) throw new Error(`Failed to fetch rules: ${rulesError.message}`)

    const rules: PricingRuleInput[] = (rulesData || []).map((r: any) => ({
      id: r.id,
      rule_name: r.rule_name,
      rule_type: r.rule_type,
      condition: r.condition,
      multiplier: r.multiplier,
      priority: r.priority,
      applies_to_routes: r.applies_to_routes || [],
    }))

    if (rules.length === 0) {
      return NextResponse.json({
        success: false,
        message: 'No active pricing rules found. Create rules first.',
        generated: 0,
      })
    }

    // 3. Fetch latest demand snapshots per trip
    // Use a subquery approach: get snapshots ordered by snapshot_at desc, pick first per trip
    let snapshotQuery = supabase
      .from('jc_demand_snapshots')
      .select('*')
      .order('snapshot_at', { ascending: false })
      .range(0, 9999)

    if (singleTripId) {
      snapshotQuery = snapshotQuery.eq('trip_id', singleTripId)
    }

    const { data: allSnapshots, error: snapshotError } = await snapshotQuery

    if (snapshotError) throw new Error(`Failed to fetch snapshots: ${snapshotError.message}`)

    // Deduplicate: keep only the latest snapshot per trip
    const latestSnapshots = new Map<string, any>()
    for (const snap of allSnapshots || []) {
      if (!latestSnapshots.has(snap.trip_id)) {
        latestSnapshots.set(snap.trip_id, snap)
      }
    }

    if (latestSnapshots.size === 0) {
      return NextResponse.json({
        success: false,
        message: 'No demand snapshots found. Run the demand-snapshot cron job first.',
        generated: 0,
      })
    }

    console.log(`[Pricing Generate] Processing ${latestSnapshots.size} trips with ${rules.length} active rules`)

    // 4. Generate recommendations
    let generated = 0
    let skipped = 0
    let skippedNoFare = 0
    let skippedBelowThreshold = 0
    let superseded = 0
    const errors: string[] = []
    const debugSamples: any[] = []

    const tripIds = Array.from(latestSnapshots.keys())
    for (const tripId of tripIds) {
      const snapshot = latestSnapshots.get(tripId)!
      try {
        // Build demand signals from snapshot
        // Use avg_fare_booked as fallback base fare when route base_fare is not set
        const routeBaseFare = snapshot.route_base_fare || snapshot.avg_fare_booked || 0

        const signals: DemandSignals = {
          occupancy_percent: snapshot.occupancy_percent || 0,
          available_seats: snapshot.available_seats || 0,
          booked_seats: snapshot.booked_seats || 0,
          total_seats: snapshot.total_seats || 0,
          bookings_last_1h: snapshot.bookings_last_1h || 0,
          bookings_last_6h: snapshot.bookings_last_6h || 0,
          bookings_last_24h: snapshot.bookings_last_24h || 0,
          cancellations_last_24h: snapshot.cancellations_last_24h || 0,
          hours_to_departure: snapshot.hours_to_departure || 0,
          day_of_week: snapshot.day_of_week || 0,
          is_weekend: snapshot.is_weekend || false,
          avg_fare_booked: snapshot.avg_fare_booked,
          route_base_fare: routeBaseFare,
        }

        // Collect debug samples (first 5)
        if (debugSamples.length < 5) {
          debugSamples.push({
            trip_id: tripId,
            service_number: snapshot.service_number,
            route_base_fare: snapshot.route_base_fare,
            avg_fare_booked: snapshot.avg_fare_booked,
            used_base_fare: routeBaseFare,
            occupancy_percent: snapshot.occupancy_percent,
            available_seats: snapshot.available_seats,
            booked_seats: snapshot.booked_seats,
            hours_to_departure: snapshot.hours_to_departure,
            day_of_week: snapshot.day_of_week,
          })
        }

        // Skip trips with no base fare at all
        if (signals.route_base_fare <= 0) {
          skippedNoFare++
          skipped++
          continue
        }

        // Skip trips with 0 bookings that are still far from departure (>24h)
        // These haven't started selling yet - no point recommending discounts
        if (signals.booked_seats === 0 && signals.hours_to_departure > 24) {
          skippedBelowThreshold++
          skipped++
          continue
        }

        const serviceNumber = snapshot.service_number || ''

        // Generate recommendation
        const result = generateRecommendation(signals, rules, config, serviceNumber)

        // Skip if change is below threshold
        if (Math.abs(result.fare_change_percent) < config.min_change_threshold_percent) {
          skippedBelowThreshold++
          skipped++
          continue
        }

        // Supersede existing pending recommendations for this trip
        const { data: existingPending } = await supabase
          .from('jc_pricing_recommendations')
          .select('id')
          .eq('trip_id', tripId)
          .eq('status', 'pending')

        if (existingPending && existingPending.length > 0) {
          await supabase
            .from('jc_pricing_recommendations')
            .update({ status: 'superseded' })
            .eq('trip_id', tripId)
            .eq('status', 'pending')
          superseded += existingPending.length
        }

        // Calculate expiry (2 hours before departure, or now + 1 hour if departure is very soon)
        let expiresAt: string | null = null
        if (snapshot.hours_to_departure > 2) {
          const expiryDate = new Date(Date.now() + (snapshot.hours_to_departure - 2) * 60 * 60 * 1000)
          expiresAt = expiryDate.toISOString()
        } else {
          const expiryDate = new Date(Date.now() + 60 * 60 * 1000)
          expiresAt = expiryDate.toISOString()
        }

        // Insert new recommendation
        const matchedRules = result.applied_rules
          .filter((r) => r.matched)
          .map((r) => ({
            rule_id: r.rule_id,
            rule_name: r.rule_name,
            multiplier: r.multiplier,
          }))

        const { error: insertError } = await supabase
          .from('jc_pricing_recommendations')
          .insert({
            trip_id: tripId,
            route_id: snapshot.route_id,
            snapshot_id: snapshot.id,
            service_number: snapshot.service_number,
            travel_date: snapshot.travel_date,
            current_base_fare: signals.route_base_fare,
            recommended_fare: result.recommended_fare,
            fare_change_percent: result.fare_change_percent,
            fare_multiplier: result.fare_multiplier,
            demand_score: result.demand_score,
            occupancy_percent: signals.occupancy_percent,
            hours_to_departure: signals.hours_to_departure,
            booking_velocity_1h: signals.bookings_last_1h,
            applied_rules: matchedRules,
            status: 'pending',
            generated_at: new Date().toISOString(),
            expires_at: expiresAt,
            estimated_revenue_impact: result.estimated_revenue_impact,
          })

        if (insertError) {
          errors.push(`Trip ${tripId}: ${insertError.message}`)
        } else {
          generated++
        }
      } catch (err: any) {
        errors.push(`Trip ${tripId}: ${err.message}`)
      }
    }

    const latency = Date.now() - startTime

    console.log(`[Pricing Generate] Done: ${generated} generated, ${skipped} skipped, ${superseded} superseded, ${errors.length} errors in ${latency}ms`)

    return NextResponse.json({
      success: true,
      message: `Generated ${generated} pricing recommendations`,
      generated,
      trips_analyzed: latestSnapshots.size,
      skipped,
      skipped_reasons: {
        no_base_fare: skippedNoFare,
        below_threshold: skippedBelowThreshold,
      },
      superseded,
      errors: errors.length,
      error_details: errors.length > 0 ? errors.slice(0, 10) : undefined,
      debug_samples: debugSamples,
      latency_ms: latency,
      timestamp: new Date().toISOString(),
    })
  } catch (error: any) {
    console.error('[Pricing Generate] Error:', error.message)
    return NextResponse.json(
      { success: false, error: error.message, timestamp: new Date().toISOString() },
      { status: 500 }
    )
  }
}
