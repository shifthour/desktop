export interface LeadData {
  id: string
  leadName: string
  location: string
  contactName: string
  product: string
  salesStage: string
  priority: string
  assignedTo: string
  date: string
  contactNo?: string
  buyerRef?: string
  closingDate?: string
}

export interface OpportunityData {
  id: string
  accountName: string
  contactName: string
  product: string
  value: string
  stage: string
  probability: string
  expectedClose: string
  source: string
}

export interface ComplaintData {
  id: string
  accountName: string
  contactName: string
  productService: string
  complaintType: string
  severity: string
  status: string
  description: string
  date: string
}

export interface AIInsight {
  type: 'lead-score' | 'next-action' | 'prediction' | 'recommendation'
  confidence: number
  title: string
  description: string
  actionable: boolean
  priority: 'low' | 'medium' | 'high' | 'critical'
}

export class AILeadScoringService {
  static calculateLeadScore(lead: LeadData): number {
    let score = 0
    
    // Company type scoring based on research domain
    const researchIndicators = ['university', 'college', 'research', 'pharma', 'biotech', 'laboratory', 'institute']
    const companyName = lead.leadName?.toLowerCase() || ''
    if (researchIndicators.some(indicator => companyName.includes(indicator))) {
      score += 25
    }
    
    // Product category scoring
    const highValueProducts = ['freeze dryer', 'lyophilizer', 'spectrophotometer', 'autoclave', 'biosafety cabinet']
    const product = lead.product?.toLowerCase() || ''
    if (product && highValueProducts.some(productType => product.includes(productType))) {
      score += 20
    }
    
    // Location scoring (tier-1 cities and research hubs)
    const tier1Cities = ['bangalore', 'mumbai', 'delhi', 'chennai', 'hyderabad', 'pune', 'kolkata']
    const location = lead.location?.toLowerCase() || ''
    if (location && tier1Cities.some(city => location.includes(city))) {
      score += 15
    }
    
    // Contact information completeness
    if (lead.contactNo && lead.contactNo.length > 0) score += 10
    if (lead.contactName && lead.contactName.length > 0) score += 10
    
    // Sales stage progression
    const stageScores = {
      'prospecting': 5,
      'qualified': 15,
      'proposal': 25,
      'negotiation': 35
    }
    score += stageScores[lead.salesStage?.toLowerCase() || ''] || 0
    
    // Priority boost
    if (lead.priority === 'high') score += 10
    
    return Math.min(score, 100)
  }
  
  static getLeadScoreInsight(score: number): AIInsight {
    let priority: 'low' | 'medium' | 'high' | 'critical'
    let description: string
    
    if (score >= 80) {
      priority = 'critical'
      description = 'Excellent lead with high conversion potential. Prioritize immediate follow-up.'
    } else if (score >= 60) {
      priority = 'high'
      description = 'Strong lead with good conversion potential. Schedule follow-up within 24 hours.'
    } else if (score >= 40) {
      priority = 'medium'
      description = 'Moderate lead potential. Monitor and nurture with regular follow-ups.'
    } else {
      priority = 'low'
      description = 'Low priority lead. Include in nurturing campaigns.'
    }
    
    return {
      type: 'lead-score',
      confidence: Math.min(score / 100, 0.95),
      title: `Lead Score: ${score}/100`,
      description,
      actionable: true,
      priority
    }
  }
}

export class AIPredictiveService {
  static predictConversionProbability(opportunity: OpportunityData): number {
    let probability = 0
    
    // Stage-based probability
    const stageProbabilities = {
      'prospecting': 0.1,
      'qualified': 0.3,
      'proposal': 0.6,
      'negotiation': 0.8,
      'closed won': 1.0
    }
    probability += (stageProbabilities[opportunity.stage.toLowerCase()] || 0.1) * 60
    
    // Value-based scoring (higher value deals have different dynamics)
    const value = parseFloat(opportunity.value.replace(/[₹,]/g, ''))
    if (value > 1000000) probability += 15 // High-value deals
    else if (value > 500000) probability += 10
    else probability += 5
    
    // Source reliability
    const sourceScores = {
      'referral': 20,
      'website': 15,
      'trade show': 12,
      'partner': 10,
      'cold call': 5
    }
    probability += sourceScores[opportunity.source.toLowerCase()] || 5
    
    // Account type (research institutions tend to have longer cycles but higher conversion)
    const researchKeywords = ['university', 'institute', 'research', 'college']
    if (researchKeywords.some(keyword => opportunity.accountName.toLowerCase().includes(keyword))) {
      probability += 5
    }
    
    return Math.min(probability, 95)
  }
  
  static predictMaintenanceNeeds(productService: string, installationDate?: string): AIInsight[] {
    const insights: AIInsight[] = []
    
    const highMaintenanceEquipment = ['freeze dryer', 'autoclave', 'spectrophotometer', 'biosafety cabinet']
    const product = productService.toLowerCase()
    
    if (highMaintenanceEquipment.some(equip => product.includes(equip))) {
      insights.push({
        type: 'prediction',
        confidence: 0.75,
        title: 'Maintenance Due Soon',
        description: `${productService} typically requires maintenance every 6 months. Schedule preventive maintenance to avoid downtime.`,
        actionable: true,
        priority: 'medium'
      })
    }
    
    return insights
  }
}

export class AIRecommendationService {
  static getProductRecommendations(currentProduct: string, customerType: string): string[] {
    const recommendations: string[] = []
    const product = currentProduct.toLowerCase()
    
    // Cross-sell recommendations based on product categories
    if (product.includes('freeze dryer') || product.includes('lyophilizer')) {
      recommendations.push('Laboratory Freeze Drying Accessories')
      recommendations.push('Sample Preparation Equipment')
      recommendations.push('Temperature Monitoring System')
    }
    
    if (product.includes('spectrophotometer')) {
      recommendations.push('Cuvettes and Accessories')
      recommendations.push('Sample Preparation Kit')
      recommendations.push('Analytical Balance')
    }
    
    if (product.includes('autoclave')) {
      recommendations.push('Sterilization Indicators')
      recommendations.push('Laboratory Glassware')
      recommendations.push('Biosafety Cabinet')
    }
    
    if (product.includes('biosafety cabinet')) {
      recommendations.push('HEPA Filters')
      recommendations.push('Laboratory Furniture')
      recommendations.push('Personal Protective Equipment')
    }
    
    // Customer type specific recommendations
    if (customerType.toLowerCase().includes('university') || customerType.toLowerCase().includes('research')) {
      recommendations.push('Educational Training Programs')
      recommendations.push('Extended Warranty Plans')
    }
    
    return recommendations
  }
  
  static getNextBestAction(lead: LeadData): AIInsight {
    const score = this.calculateActionPriority(lead)
    let action: string
    let priority: 'low' | 'medium' | 'high' | 'critical'
    
    if (lead.salesStage?.toLowerCase() === 'prospecting') {
      action = `Schedule a technical discussion with ${lead.contactName} about ${lead.product} requirements`
      priority = 'high'
    } else if (lead.salesStage?.toLowerCase() === 'qualified') {
      action = `Prepare detailed quotation for ${lead.product} and schedule presentation`
      priority = 'high'
    } else if (lead.salesStage?.toLowerCase() === 'proposal') {
      action = `Follow up on submitted proposal and address any technical questions`
      priority = 'medium'
    } else {
      action = `Make follow-up call to ${lead.contactName} to understand current requirements`
      priority = 'medium'
    }
    
    return {
      type: 'next-action',
      confidence: score / 100,
      title: 'Recommended Next Action',
      description: action,
      actionable: true,
      priority
    }
  }
  
  private static calculateActionPriority(lead: LeadData): number {
    let priority = 50
    
    if (lead.priority === 'high') priority += 25
    if (lead.contactNo && lead.contactNo.length > 0) priority += 15
    if (lead.closingDate) priority += 10
    
    return Math.min(priority, 100)
  }
}

export class AIEmailService {
  static generateFollowUpEmail(lead: LeadData): string {
    const templates = {
      prospecting: `Subject: Technical Discussion - ${lead.product} for ${lead.leadName}

Dear ${lead.contactName || 'Sir/Madam'},

Thank you for your interest in our ${lead.product}. I would like to schedule a technical discussion to better understand your specific requirements and laboratory setup.

Our ${lead.product} is widely used in research institutions and has proven to be highly reliable and efficient. I would be happy to provide you with:
- Detailed technical specifications
- Installation requirements
- Training and support options
- Customized pricing

Would you be available for a 30-minute technical discussion this week?

Best regards,
${lead.assignedTo}`,

      qualified: `Subject: Detailed Quotation - ${lead.product} Solution

Dear ${lead.contactName},

Following our discussion about your ${lead.product} requirements, I am pleased to prepare a detailed quotation tailored to your laboratory needs.

Based on our conversation, I understand that you require:
- High precision and reliability
- Comprehensive training and support
- Long-term maintenance coverage

I will send you a comprehensive proposal within 24 hours including all technical specifications, pricing, and implementation timeline.

Best regards,
${lead.assignedTo}`,

      proposal: `Subject: Follow-up on ${lead.product} Proposal

Dear ${lead.contactName},

I wanted to follow up on the proposal we submitted for the ${lead.product} last week. 

Do you have any questions about:
- Technical specifications
- Installation requirements
- Training programs
- Pricing and payment terms

I am available to discuss any aspects of the proposal and can arrange a demonstration if that would be helpful.

Best regards,
${lead.assignedTo}`
    }
    
    const stage = lead.salesStage?.toLowerCase() || ''
    return templates[stage] || templates.prospecting
  }
  
  static generateMaintenanceReminder(customerName: string, productName: string, lastService: string): string {
    return `Subject: Preventive Maintenance Due - ${productName}

Dear ${customerName},

Our records indicate that your ${productName} is due for preventive maintenance. Regular maintenance ensures:
- Optimal performance and accuracy
- Extended equipment life
- Compliance with laboratory standards
- Prevention of costly breakdowns

We would like to schedule a convenient time for our certified technician to visit your facility.

Please reply with your preferred dates and times, and we will confirm the appointment.

Best regards,
Service Team`
  }
}

export class AIInsightsService {
  static generateDashboardInsights(
    leads: LeadData[],
    opportunities: OpportunityData[],
    complaints: ComplaintData[]
  ): AIInsight[] {
    const insights: AIInsight[] = []
    
    // Lead quality insights
    const highScoreLeads = leads.filter(lead => AILeadScoringService.calculateLeadScore(lead) >= 70)
    if (highScoreLeads.length > 0) {
      insights.push({
        type: 'recommendation',
        confidence: 0.9,
        title: `${highScoreLeads.length} High-Quality Leads Need Attention`,
        description: `You have ${highScoreLeads.length} leads with scores above 70. Prioritize these for immediate follow-up to maximize conversion.`,
        actionable: true,
        priority: 'high'
      })
    }
    
    // Opportunity pipeline insights
    const negotiationOpps = opportunities.filter(opp => opp.stage.toLowerCase() === 'negotiation')
    if (negotiationOpps.length > 0) {
      const totalValue = negotiationOpps.reduce((sum, opp) => sum + parseFloat(opp.value.replace(/[₹,]/g, '')), 0)
      insights.push({
        type: 'prediction',
        confidence: 0.85,
        title: `₹${(totalValue / 100000).toFixed(1)}L in Advanced Negotiations`,
        description: `${negotiationOpps.length} opportunities worth ₹${(totalValue / 100000).toFixed(1)}L are in negotiation stage. Focus on closing these deals this month.`,
        actionable: true,
        priority: 'critical'
      })
    }
    
    // Complaint pattern insights
    const highSeverityComplaints = complaints.filter(complaint => 
      complaint.severity.toLowerCase() === 'high' && 
      complaint.status.toLowerCase() !== 'closed'
    )
    if (highSeverityComplaints.length > 0) {
      insights.push({
        type: 'recommendation',
        confidence: 0.95,
        title: `${highSeverityComplaints.length} High-Severity Complaints Open`,
        description: `Urgent attention needed for high-severity complaints to prevent customer escalation and maintain satisfaction.`,
        actionable: true,
        priority: 'critical'
      })
    }
    
    return insights
  }
}
