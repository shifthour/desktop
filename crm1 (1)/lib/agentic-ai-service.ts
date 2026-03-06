// Agentic AI Service - 3 Core Intelligent Agents
// Simplified, focused AI that works autonomously with real CRM data

interface LeadData {
  id: string
  account_name: string
  contact_name: string
  phone?: string
  email?: string
  product_name: string
  budget?: number
  lead_source: string
  lead_status: string
  priority: string
  city?: string
  state?: string
  created_at: string
  next_followup_date?: string
  assigned_to?: string
  department?: string
}

interface DealData {
  id: string
  account_name: string
  contact_person: string
  product: string
  value: number
  stage: string
  probability: number
  expected_close_date: string
  assigned_to: string
  priority: string
  status: string
  created_at: string
  next_followup_date?: string
  phone?: string
  email?: string
}

interface AccountData {
  id: string
  account_name: string
  industry?: string
  annual_revenue?: number
  employee_count?: number
  city?: string
  state?: string
}

interface AIAction {
  id: string
  type: 'call' | 'email' | 'whatsapp' | 'demo' | 'proposal' | 'follow-up'
  priority: 'high' | 'medium' | 'low'
  title: string
  description: string
  entityId: string
  entityType: 'lead' | 'deal' | 'account'
  confidence: number
  estimatedImpact: string
  suggestedTime?: string
}

interface AgentInsight {
  agent: 'sales-intelligence' | 'follow-up-automation' | 'revenue-optimizer'
  type: 'opportunity' | 'risk' | 'optimization' | 'alert'
  title: string
  description: string
  actions: AIAction[]
  confidence: number
  impact: 'high' | 'medium' | 'low'
  urgency: 'immediate' | 'today' | 'this-week' | 'next-week'
}

class AgenticAIService {
  
  // AGENT 1: Sales Intelligence Agent
  static generateSalesIntelligence(leads: LeadData[], deals: DealData[], accounts: AccountData[]): AgentInsight[] {
    const insights: AgentInsight[] = []
    
    // Hot lead detection
    const hotLeads = leads.filter(lead => {
      const score = this.calculateLeadScore(lead)
      const isRecent = this.isDaysSince(lead.created_at) <= 3
      const hasContact = lead.phone || lead.email
      return score >= 75 && isRecent && hasContact && lead.lead_status === 'New'
    })
    
    if (hotLeads.length > 0) {
      insights.push({
        agent: 'sales-intelligence',
        type: 'opportunity',
        title: `${hotLeads.length} Hot Leads Detected`,
        description: `High-scoring leads that need immediate attention for maximum conversion`,
        actions: hotLeads.slice(0, 3).map(lead => ({
          id: `hot-lead-${lead.id}`,
          type: 'call',
          priority: 'high',
          title: `Contact ${lead.contact_name}`,
          description: `Score: ${this.calculateLeadScore(lead)}/100 - ${lead.product_name} inquiry`,
          entityId: lead.id,
          entityType: 'lead',
          confidence: 85,
          estimatedImpact: '60% conversion chance',
          suggestedTime: this.getBestCallTime(lead)
        })),
        confidence: 85,
        impact: 'high',
        urgency: 'immediate'
      })
    }
    
    // Deal probability analysis
    const highProbDeals = deals.filter(deal => deal.probability >= 80 && deal.status === 'Active')
    if (highProbDeals.length > 0) {
      insights.push({
        agent: 'sales-intelligence',
        type: 'opportunity',
        title: `${highProbDeals.length} High-Probability Deals`,
        description: `Ready-to-close deals that need immediate proposal preparation`,
        actions: highProbDeals.map(deal => ({
          id: `close-deal-${deal.id}`,
          type: 'proposal',
          priority: 'high',
          title: `Prepare proposal for ${deal.account_name}`,
          description: `${deal.probability}% probability - ₹${deal.value.toLocaleString()} value`,
          entityId: deal.id,
          entityType: 'deal',
          confidence: deal.probability,
          estimatedImpact: `₹${deal.value.toLocaleString()} potential revenue`
        })),
        confidence: 90,
        impact: 'high',
        urgency: 'immediate'
      })
    }
    
    return insights
  }
  
  // AGENT 2: Follow-up Automation Agent
  static generateFollowUpAutomation(leads: LeadData[], deals: DealData[]): AgentInsight[] {
    const insights: AgentInsight[] = []
    
    // Overdue follow-ups
    const overdueLeads = leads.filter(lead => {
      if (!lead.next_followup_date) return false
      const followupDate = new Date(lead.next_followup_date)
      return followupDate < new Date() && lead.lead_status !== 'Qualified'
    })
    
    const overdueDeals = deals.filter(deal => {
      if (!deal.next_followup_date) return false
      const followupDate = new Date(deal.next_followup_date)
      return followupDate < new Date() && deal.status === 'Active'
    })
    
    const totalOverdue = overdueLeads.length + overdueDeals.length
    
    if (totalOverdue > 0) {
      const actions: AIAction[] = []
      
      // Process overdue leads
      overdueLeads.slice(0, 3).forEach(lead => {
        actions.push({
          id: `overdue-lead-${lead.id}`,
          type: this.getBestContactMethod(lead),
          priority: 'high',
          title: `Follow up ${lead.contact_name}`,
          description: `${this.getDaysOverdue(lead.next_followup_date!)} days overdue - ${lead.product_name}`,
          entityId: lead.id,
          entityType: 'lead',
          confidence: 70,
          estimatedImpact: 'Prevent lead from going cold'
        })
      })
      
      // Process overdue deals
      overdueDeals.slice(0, 2).forEach(deal => {
        actions.push({
          id: `overdue-deal-${deal.id}`,
          type: this.getBestContactMethod(deal),
          priority: 'high', 
          title: `Follow up ${deal.contact_person}`,
          description: `${this.getDaysOverdue(deal.next_followup_date!)} days overdue - ${deal.stage} stage`,
          entityId: deal.id,
          entityType: 'deal',
          confidence: 80,
          estimatedImpact: `Risk: ₹${deal.value.toLocaleString()} deal`
        })
      })
      
      insights.push({
        agent: 'follow-up-automation',
        type: 'risk',
        title: `${totalOverdue} Overdue Follow-ups`,
        description: 'Critical follow-ups that need immediate attention to prevent pipeline leakage',
        actions,
        confidence: 85,
        impact: 'high',
        urgency: 'immediate'
      })
    }
    
    // Optimal timing recommendations
    const todayFollowups = [...leads, ...deals].filter(item => {
      if (!item.next_followup_date) return false
      const followupDate = new Date(item.next_followup_date)
      const today = new Date()
      return followupDate.toDateString() === today.toDateString()
    })
    
    if (todayFollowups.length > 0) {
      insights.push({
        agent: 'follow-up-automation',
        type: 'optimization',
        title: `${todayFollowups.length} Follow-ups Scheduled Today`,
        description: 'Optimized timing for maximum response rates based on contact behavior',
        actions: todayFollowups.slice(0, 5).map(item => ({
          id: `today-followup-${item.id}`,
          type: this.getBestContactMethod(item),
          priority: 'medium',
          title: `Contact ${item.contact_name || item.contact_person}`,
          description: `Optimal time: ${this.getBestCallTime(item)}`,
          entityId: item.id,
          entityType: 'product_name' in item ? 'lead' : 'deal',
          confidence: 70,
          estimatedImpact: '40% better response rate',
          suggestedTime: this.getBestCallTime(item)
        })),
        confidence: 75,
        impact: 'medium',
        urgency: 'today'
      })
    }
    
    return insights
  }
  
  // AGENT 3: Revenue Optimizer Agent  
  static generateRevenueOptimization(deals: DealData[], accounts: AccountData[]): AgentInsight[] {
    const insights: AgentInsight[] = []
    
    // Pipeline risk analysis
    const stalledDeals = deals.filter(deal => {
      const daysSinceCreated = this.isDaysSince(deal.created_at)
      const isStalled = daysSinceCreated > 30 && 
                      ['Demo', 'Proposal', 'Negotiation'].includes(deal.stage)
      return isStalled && deal.status === 'Active'
    })
    
    if (stalledDeals.length > 0) {
      const totalValue = stalledDeals.reduce((sum, deal) => sum + deal.value, 0)
      
      insights.push({
        agent: 'revenue-optimizer',
        type: 'risk',
        title: `₹${(totalValue/100000).toFixed(1)}L at Risk`,
        description: `${stalledDeals.length} deals stalled for 30+ days need intervention`,
        actions: stalledDeals.slice(0, 3).map(deal => ({
          id: `stalled-deal-${deal.id}`,
          type: 'call',
          priority: 'high',
          title: `Rescue ${deal.account_name} deal`,
          description: `₹${deal.value.toLocaleString()} - Stalled in ${deal.stage} for ${this.isDaysSince(deal.created_at)} days`,
          entityId: deal.id,
          entityType: 'deal',
          confidence: 60,
          estimatedImpact: 'Prevent deal loss'
        })),
        confidence: 80,
        impact: 'high',
        urgency: 'immediate'
      })
    }
    
    // Cross-sell opportunities
    const crossSellOpportunities = this.identifyCrossSellOpportunities(deals, accounts)
    if (crossSellOpportunities.length > 0) {
      insights.push({
        agent: 'revenue-optimizer',
        type: 'opportunity',
        title: `${crossSellOpportunities.length} Cross-sell Opportunities`,
        description: 'Existing customers with high potential for additional products',
        actions: crossSellOpportunities.map(opp => ({
          id: `cross-sell-${opp.accountId}`,
          type: 'email',
          priority: 'medium',
          title: `Propose ${opp.recommendedProduct}`,
          description: `${opp.accountName} - ${opp.reason}`,
          entityId: opp.accountId,
          entityType: 'account',
          confidence: opp.confidence,
          estimatedImpact: `Potential ₹${opp.estimatedValue.toLocaleString()}`
        })),
        confidence: 70,
        impact: 'medium',
        urgency: 'this-week'
      })
    }
    
    return insights
  }
  
  // Unified agent orchestrator
  static async generateAllAgentInsights(
    leads: LeadData[], 
    deals: DealData[], 
    accounts: AccountData[]
  ): Promise<AgentInsight[]> {
    const allInsights = [
      ...this.generateSalesIntelligence(leads, deals, accounts),
      ...this.generateFollowUpAutomation(leads, deals),
      ...this.generateRevenueOptimization(deals, accounts)
    ]
    
    // Sort by urgency and impact
    return allInsights.sort((a, b) => {
      const urgencyWeight = { immediate: 4, today: 3, 'this-week': 2, 'next-week': 1 }
      const impactWeight = { high: 3, medium: 2, low: 1 }
      
      const scoreA = urgencyWeight[a.urgency] * impactWeight[a.impact]
      const scoreB = urgencyWeight[b.urgency] * impactWeight[b.impact]
      
      return scoreB - scoreA
    })
  }
  
  // Utility functions
  static calculateLeadScore(lead: LeadData): number {
    let score = 0
    
    // Company type scoring
    if (lead.account_name.toLowerCase().includes('university') || 
        lead.account_name.toLowerCase().includes('research')) score += 25
    if (lead.account_name.toLowerCase().includes('lab')) score += 20
    
    // Department scoring
    if (lead.department?.toLowerCase().includes('laboratory')) score += 15
    if (lead.department?.toLowerCase().includes('quality')) score += 10
    
    // Contact completeness
    if (lead.phone) score += 10
    if (lead.email) score += 10
    if (lead.contact_name) score += 10
    
    // Budget indication
    if (lead.budget && lead.budget > 100000) score += 20
    if (lead.budget && lead.budget > 500000) score += 10
    
    // Geographic scoring (Tier-1 cities)
    const tier1Cities = ['bangalore', 'mumbai', 'delhi', 'chennai', 'hyderabad', 'pune', 'kolkata']
    if (lead.city && tier1Cities.includes(lead.city.toLowerCase())) score += 15
    
    // Priority and status
    if (lead.priority === 'High') score += 10
    if (lead.lead_status === 'Contacted') score += 5
    
    return Math.min(score, 100)
  }
  
  static getBestContactMethod(item: any): AIAction['type'] {
    // Prefer phone for high-value opportunities
    if (item.value > 500000 || item.budget > 500000) return 'call'
    if (item.phone) return 'call'
    if (item.email) return 'email'
    if (item.whatsapp) return 'whatsapp'
    return 'email'
  }
  
  static getBestCallTime(item: any): string {
    // Business hours optimization based on contact type
    const morningSlots = ['10:00 AM', '10:30 AM', '11:00 AM']
    const afternoonSlots = ['2:00 PM', '2:30 PM', '3:00 PM']
    
    // Research institutions prefer morning calls
    if (item.account_name?.toLowerCase().includes('university') ||
        item.account_name?.toLowerCase().includes('research')) {
      return morningSlots[Math.floor(Math.random() * morningSlots.length)]
    }
    
    // Commercial accounts prefer afternoon
    return afternoonSlots[Math.floor(Math.random() * afternoonSlots.length)]
  }
  
  static isDaysSince(dateString: string): number {
    const date = new Date(dateString)
    const now = new Date()
    return Math.floor((now.getTime() - date.getTime()) / (1000 * 60 * 60 * 24))
  }
  
  static getDaysOverdue(dateString: string): number {
    const date = new Date(dateString)
    const now = new Date()
    return Math.max(0, Math.floor((now.getTime() - date.getTime()) / (1000 * 60 * 60 * 24)))
  }
  
  static identifyCrossSellOpportunities(deals: DealData[], accounts: AccountData[]): Array<{
    accountId: string
    accountName: string
    recommendedProduct: string
    reason: string
    confidence: number
    estimatedValue: number
  }> {
    const opportunities: any[] = []
    
    // Find accounts with successful deals that could buy complementary products
    const successfulAccounts = accounts.filter(account => {
      const accountDeals = deals.filter(deal => 
        deal.account_name === account.account_name && deal.status === 'Won'
      )
      return accountDeals.length > 0
    })
    
    successfulAccounts.forEach(account => {
      const wonDeals = deals.filter(deal => 
        deal.account_name === account.account_name && deal.status === 'Won'
      )
      
      wonDeals.forEach(deal => {
        // Product category analysis for recommendations
        if (deal.product.toLowerCase().includes('spectrophotometer')) {
          opportunities.push({
            accountId: account.id,
            accountName: account.account_name,
            recommendedProduct: 'HPLC System',
            reason: 'Customers with spectrophotometers often need HPLC systems',
            confidence: 75,
            estimatedValue: 1500000
          })
        }
        
        if (deal.product.toLowerCase().includes('freeze dryer')) {
          opportunities.push({
            accountId: account.id,
            accountName: account.account_name,
            recommendedProduct: 'Vacuum Pump',
            reason: 'Essential accessory for freeze dryer systems',
            confidence: 85,
            estimatedValue: 500000
          })
        }
      })
    })
    
    return opportunities.slice(0, 5)
  }
  
  // Generate smart email content
  static generateSmartEmail(lead: LeadData, type: 'initial' | 'follow-up' | 'proposal'): string {
    const templates = {
      initial: `Hi ${lead.contact_name},

I hope this email finds you well. I understand ${lead.account_name} is looking for ${lead.product_name}.

Based on your requirements, I'd like to schedule a brief 15-minute call to understand your specific needs and share how we can help.

Are you available for a quick discussion this week?

Best regards,
${lead.assigned_to}`,

      'follow-up': `Hi ${lead.contact_name},

Following up on our discussion about ${lead.product_name} for ${lead.account_name}.

I wanted to check if you had a chance to review our initial proposal and if you have any questions.

Would it be helpful if I arranged a product demonstration for your team?

Best regards,
${lead.assigned_to}`,

      proposal: `Dear ${lead.contact_name},

Thank you for your interest in ${lead.product_name}.

Based on our discussion about ${lead.account_name}'s requirements, I've prepared a customized proposal that addresses your specific needs.

The solution includes:
- ${lead.product_name} with full specifications
- Installation and training support
- 1-year comprehensive warranty
- Flexible payment terms

I'd love to schedule a call to walk through the proposal and answer any questions.

Best regards,
${lead.assigned_to}`
    }
    
    return templates[type]
  }
}

export default AgenticAIService