/**
 * Dynamic Pricing Engine
 * Pure computation module - no database side effects.
 * Takes demand signals and pricing rules, returns fare recommendations.
 */

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

export interface DemandSignals {
  occupancy_percent: number
  available_seats: number
  booked_seats: number
  total_seats: number
  bookings_last_1h: number
  bookings_last_6h: number
  bookings_last_24h: number
  cancellations_last_24h: number
  hours_to_departure: number
  day_of_week: number // 0=Sunday, 6=Saturday
  is_weekend: boolean
  avg_fare_booked: number | null
  route_base_fare: number
}

export interface RuleCondition {
  field: string
  operator: 'gt' | 'gte' | 'lt' | 'lte' | 'eq' | 'in'
  value: number | number[]
  max_exclusive?: number
  additional?: RuleCondition
}

export interface PricingRuleInput {
  id: string
  rule_name: string
  rule_type: string
  condition: RuleCondition
  multiplier: number
  priority: number
  applies_to_routes: string[]
}

export interface GlobalConfig {
  pricing_enabled: boolean
  min_fare_multiplier: number
  max_fare_multiplier: number
  rule_combination_strategy: 'multiplicative' | 'max_wins' | 'average'
  min_change_threshold_percent: number
  estimated_conversion_rate: number
  demand_score_weights: {
    occupancy: number
    velocity: number
    time_to_departure: number
    historical: number
  }
}

export interface RuleEvaluationResult {
  rule_id: string
  rule_name: string
  multiplier: number
  matched: boolean
}

export interface PricingRecommendationResult {
  recommended_fare: number
  fare_change_percent: number
  fare_multiplier: number
  demand_score: number
  applied_rules: RuleEvaluationResult[]
  estimated_revenue_impact: number
}

// ─────────────────────────────────────────────────────────────────────────────
// Default config (fallback if DB config is missing)
// ─────────────────────────────────────────────────────────────────────────────

export const DEFAULT_CONFIG: GlobalConfig = {
  pricing_enabled: true,
  min_fare_multiplier: 0.80,
  max_fare_multiplier: 1.40,
  rule_combination_strategy: 'multiplicative',
  min_change_threshold_percent: 2,
  estimated_conversion_rate: 0.6,
  demand_score_weights: {
    occupancy: 0.35,
    velocity: 0.25,
    time_to_departure: 0.20,
    historical: 0.20,
  },
}

// ─────────────────────────────────────────────────────────────────────────────
// Core Functions
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Evaluates a single rule condition against demand signals.
 * Supports nested `additional` conditions for compound rules.
 */
export function evaluateCondition(
  signals: DemandSignals,
  condition: RuleCondition
): boolean {
  const fieldValue = getSignalValue(signals, condition.field)
  if (fieldValue === null || fieldValue === undefined) return false

  let primaryMatch = false

  switch (condition.operator) {
    case 'gt':
      primaryMatch = fieldValue > (condition.value as number)
      break
    case 'gte':
      primaryMatch = fieldValue >= (condition.value as number)
      break
    case 'lt':
      primaryMatch = fieldValue < (condition.value as number)
      break
    case 'lte':
      primaryMatch = fieldValue <= (condition.value as number)
      break
    case 'eq':
      primaryMatch = fieldValue === (condition.value as number)
      break
    case 'in':
      primaryMatch = Array.isArray(condition.value) && condition.value.includes(fieldValue)
      break
    default:
      return false
  }

  // Check max_exclusive for range rules (e.g., 60 <= occupancy < 80)
  if (primaryMatch && condition.max_exclusive !== undefined) {
    primaryMatch = fieldValue < condition.max_exclusive
  }

  // Check nested additional condition (e.g., occupancy < 30% AND hours_to_departure < 48)
  if (primaryMatch && condition.additional) {
    return evaluateCondition(signals, condition.additional)
  }

  return primaryMatch
}

/**
 * Gets a signal value by field name from the DemandSignals object.
 */
function getSignalValue(signals: DemandSignals, field: string): number | null {
  const value = (signals as Record<string, any>)[field]
  if (value === null || value === undefined) return null
  return typeof value === 'number' ? value : null
}

/**
 * Evaluates all active rules against demand signals for a given service.
 * Rules are evaluated in priority order (lower priority number = evaluated first).
 * Returns all rules with their match status.
 */
export function evaluateRules(
  signals: DemandSignals,
  rules: PricingRuleInput[],
  serviceNumber: string
): RuleEvaluationResult[] {
  // Sort by priority (lower = higher priority)
  const sortedRules = [...rules].sort((a, b) => a.priority - b.priority)

  return sortedRules.map((rule) => {
    // Check if rule applies to this route
    const appliesToRoute =
      rule.applies_to_routes.length === 0 ||
      rule.applies_to_routes.includes(serviceNumber)

    if (!appliesToRoute) {
      return {
        rule_id: rule.id,
        rule_name: rule.rule_name,
        multiplier: rule.multiplier,
        matched: false,
      }
    }

    const matched = evaluateCondition(signals, rule.condition)

    return {
      rule_id: rule.id,
      rule_name: rule.rule_name,
      multiplier: rule.multiplier,
      matched,
    }
  })
}

/**
 * Combines multipliers from matched rules based on the configured strategy.
 */
export function combineMultipliers(
  results: RuleEvaluationResult[],
  strategy: GlobalConfig['rule_combination_strategy']
): number {
  const matchedRules = results.filter((r) => r.matched)

  if (matchedRules.length === 0) return 1.0

  switch (strategy) {
    case 'multiplicative': {
      // Multiply all matched multipliers: 1.15 * 1.10 = 1.265
      return matchedRules.reduce((acc, r) => acc * r.multiplier, 1.0)
    }
    case 'max_wins': {
      // Use the single most impactful multiplier (furthest from 1.0)
      return matchedRules.reduce((best, r) => {
        return Math.abs(r.multiplier - 1.0) > Math.abs(best - 1.0)
          ? r.multiplier
          : best
      }, 1.0)
    }
    case 'average': {
      // Average all matched multipliers
      const sum = matchedRules.reduce((acc, r) => acc + r.multiplier, 0)
      return sum / matchedRules.length
    }
    default:
      return 1.0
  }
}

/**
 * Calculates a composite demand score (0-100) from multiple signals.
 */
export function calculateDemandScore(
  signals: DemandSignals,
  weights: GlobalConfig['demand_score_weights']
): number {
  // Occupancy component (0-100): direct mapping
  const occupancyScore = Math.min(signals.occupancy_percent, 100)

  // Velocity component (0-100): normalized booking rate
  // 5+ bookings/hour = 100, 0 = 0
  const velocityScore = Math.min(signals.bookings_last_1h * 20, 100)

  // Time urgency component (0-100): inverse of hours to departure
  // 0 hours = 100, 100+ hours = 0
  const timeScore = Math.max(0, 100 - signals.hours_to_departure)

  // Historical component (0-100): use current occupancy as proxy
  // In future, this can compare against route-average for same day-of-week
  const historicalScore = Math.min(signals.occupancy_percent * 1.2, 100)

  const score =
    occupancyScore * weights.occupancy +
    velocityScore * weights.velocity +
    timeScore * weights.time_to_departure +
    historicalScore * weights.historical

  return Math.round(Math.min(Math.max(score, 0), 100) * 100) / 100
}

/**
 * Main entry point: generates a pricing recommendation.
 * Evaluates rules, combines multipliers, applies guardrails, returns recommendation.
 */
export function generateRecommendation(
  signals: DemandSignals,
  rules: PricingRuleInput[],
  config: GlobalConfig,
  serviceNumber: string
): PricingRecommendationResult {
  const baseFare = signals.route_base_fare

  // Evaluate all rules
  const ruleResults = evaluateRules(signals, rules, serviceNumber)

  // Combine matched multipliers
  let fareMultiplier = combineMultipliers(ruleResults, config.rule_combination_strategy)

  // Apply min/max guardrails
  fareMultiplier = Math.max(config.min_fare_multiplier, fareMultiplier)
  fareMultiplier = Math.min(config.max_fare_multiplier, fareMultiplier)

  // Round multiplier to 4 decimal places
  fareMultiplier = Math.round(fareMultiplier * 10000) / 10000

  // Calculate recommended fare
  const recommendedFare = Math.round(baseFare * fareMultiplier)

  // Calculate change percent
  const fareChangePercent =
    baseFare > 0
      ? Math.round(((recommendedFare - baseFare) / baseFare) * 10000) / 100
      : 0

  // Calculate demand score
  const demandScore = calculateDemandScore(signals, config.demand_score_weights)

  // Estimate revenue impact
  // (recommended - current) * remaining seats * conversion rate
  const estimatedRevenueImpact =
    (recommendedFare - baseFare) *
    signals.available_seats *
    config.estimated_conversion_rate

  return {
    recommended_fare: recommendedFare,
    fare_change_percent: fareChangePercent,
    fare_multiplier: fareMultiplier,
    demand_score: demandScore,
    applied_rules: ruleResults,
    estimated_revenue_impact: Math.round(estimatedRevenueImpact * 100) / 100,
  }
}
