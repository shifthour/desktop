import { describe, it, expect } from 'vitest'
import { scoreLead, scoreLeadWithWeights, DEFAULT_WEIGHTS } from '../src/lib/scoring'

describe('Lead Scoring', () => {
  it('should return 0 for empty lead', () => {
    const score = scoreLead({})
    expect(score).toBe(0)
  })

  it('should score bedrooms preference correctly', () => {
    const lead2BHK = { bedroomsPref: 2 }
    const lead3BHK = { bedroomsPref: 3 }
    const lead4BHK = { bedroomsPref: 4 }
    
    expect(scoreLead(lead2BHK)).toBeGreaterThan(0)
    expect(scoreLead(lead3BHK)).toBeGreaterThan(0)
    expect(scoreLead(lead4BHK)).toBe(0) // Only 2-3 BHK get points
  })

  it('should score budget correctly', () => {
    const highBudgetLead = { budgetMax: 10000000 } // 1 Crore
    const lowBudgetLead = { budgetMax: 5000000 } // 50 Lakhs
    
    expect(scoreLead(highBudgetLead)).toBeGreaterThan(scoreLead(lowBudgetLead))
  })

  it('should score location preference', () => {
    const whitefieldLead = { locationPref: 'Whitefield' }
    const otherLocationLead = { locationPref: 'Koramangala' }
    
    expect(scoreLead(whitefieldLead)).toBeGreaterThan(scoreLead(otherLocationLead))
  })

  it('should score source correctly', () => {
    const googleAdsLead = { source: 'google_ads' }
    const metaLead = { source: 'meta' }
    
    expect(scoreLead(googleAdsLead)).toBeGreaterThan(scoreLead(metaLead))
  })

  it('should penalize duplicates', () => {
    const originalLead = { source: 'google_ads', bedroomsPref: 3 }
    const duplicateLead = { ...originalLead, duplicateOf: 'lead123' }
    
    expect(scoreLead(originalLead)).toBeGreaterThan(scoreLead(duplicateLead))
  })

  it('should give bonus for complete information', () => {
    const completeLeadScore = scoreLead({
      bedroomsPref: 3,
      budgetMax: 12000000,
      locationPref: 'Whitefield',
      source: 'google_ads',
      email: 'test@email.com',
      name: 'John Doe'
    })
    
    const incompleteLeadScore = scoreLead({
      bedroomsPref: 3,
      budgetMax: 12000000
    })
    
    expect(completeLeadScore).toBeGreaterThan(incompleteLeadScore)
  })

  it('should work with custom weights', () => {
    const lead = { bedroomsPref: 2, source: 'google_ads' }
    const customWeights = { ...DEFAULT_WEIGHTS, bedroomsPref: 50 }
    
    const defaultScore = scoreLead(lead)
    const customScore = scoreLeadWithWeights(lead, customWeights)
    
    expect(customScore).toBeGreaterThan(defaultScore)
  })

  it('should never return negative scores', () => {
    const heavilyPenalizedLead = {
      duplicateOf: 'original123',
      source: 'csv', // Low scoring source
      budgetMax: 1000000 // Low budget
    }
    
    const score = scoreLead(heavilyPenalizedLead)
    expect(score).toBeGreaterThanOrEqual(0)
  })

  it('should handle case-insensitive location matching', () => {
    const uppercaseLead = { locationPref: 'WHITEFIELD' }
    const lowercaseLead = { locationPref: 'whitefield' }
    const mixedCaseLead = { locationPref: 'WhiteField' }
    
    expect(scoreLead(uppercaseLead)).toBeGreaterThan(0)
    expect(scoreLead(lowercaseLead)).toBeGreaterThan(0)
    expect(scoreLead(mixedCaseLead)).toBeGreaterThan(0)
  })

  it('should score premium campaigns higher', () => {
    const premiumCampaign = { campaign: 'Premium Apartments' }
    const luxuryCampaign = { campaign: 'Luxury Villas' }
    const regularCampaign = { campaign: 'Regular Properties' }
    
    expect(scoreLead(premiumCampaign)).toBeGreaterThan(scoreLead(regularCampaign))
    expect(scoreLead(luxuryCampaign)).toBeGreaterThan(scoreLead(regularCampaign))
  })
})