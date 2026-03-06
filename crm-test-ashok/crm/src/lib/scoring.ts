import { Lead } from '@prisma/client'

export function scoreLead(lead: Partial<Lead>): number {
  let score = 0
  
  // Bedroom preference scoring (higher score for 2-3 BHK)
  if ([2, 3].includes(lead.bedroomsPref ?? 0)) {
    score += 20
  }
  
  // Budget scoring (higher score for >=80L budget)
  if ((lead.budgetMax ?? 0) >= 8000000) {
    score += 15
  }
  
  // Location preference scoring (higher score for Whitefield)
  if ((lead.locationPref ?? '').toLowerCase().includes('whitefield')) {
    score += 10
  }
  
  // Source scoring (Google Ads typically higher quality)
  if ((lead.source ?? '') === 'google_ads') {
    score += 15
  }
  
  // Campaign specific scoring
  if ((lead.campaign ?? '').toLowerCase().includes('premium') || 
      (lead.campaign ?? '').toLowerCase().includes('luxury')) {
    score += 5
  }
  
  // UTM source scoring
  if ((lead.utmSource ?? '') === 'google') {
    score += 5
  }
  
  // Penalty for duplicates
  if (lead.duplicateOf) {
    score -= 10
  }
  
  // Email presence bonus
  if (lead.email) {
    score += 5
  }
  
  // Name presence bonus
  if (lead.name) {
    score += 5
  }
  
  return Math.max(0, score) // Ensure non-negative score
}

export interface ScoringWeights {
  bedroomsPref: number
  budgetMin: number
  locationPref: number
  sourceGoogle: number
  premiumCampaign: number
  emailPresent: number
  namePresent: number
  duplicatePenalty: number
}

export const DEFAULT_WEIGHTS: ScoringWeights = {
  bedroomsPref: 20,
  budgetMin: 15,
  locationPref: 10,
  sourceGoogle: 15,
  premiumCampaign: 5,
  emailPresent: 5,
  namePresent: 5,
  duplicatePenalty: -10
}

export function scoreLeadWithWeights(lead: Partial<Lead>, weights: ScoringWeights): number {
  let score = 0
  
  if ([2, 3].includes(lead.bedroomsPref ?? 0)) {
    score += weights.bedroomsPref
  }
  
  if ((lead.budgetMax ?? 0) >= 8000000) {
    score += weights.budgetMin
  }
  
  if ((lead.locationPref ?? '').toLowerCase().includes('whitefield')) {
    score += weights.locationPref
  }
  
  if ((lead.source ?? '') === 'google_ads') {
    score += weights.sourceGoogle
  }
  
  if ((lead.campaign ?? '').toLowerCase().includes('premium') || 
      (lead.campaign ?? '').toLowerCase().includes('luxury')) {
    score += weights.premiumCampaign
  }
  
  if (lead.email) {
    score += weights.emailPresent
  }
  
  if (lead.name) {
    score += weights.namePresent
  }
  
  if (lead.duplicateOf) {
    score += weights.duplicatePenalty
  }
  
  return Math.max(0, score)
}