import { NextRequest, NextResponse } from 'next/server'
import { z } from 'zod'
import { db } from '@/lib/db'
import { requireAuth } from '@/lib/auth'
import { Stage } from '@prisma/client'

const querySchema = z.object({
  from: z.string().optional(),
  to: z.string().optional(),
  source: z.string().optional(),
  campaign: z.string().optional()
})

interface SourceMetric {
  source: string
  campaign: string | null
  leads: number
  siteVisits: number
  bookings: number
  cpl: number | null // Cost Per Lead
  cpsv: number | null // Cost Per Site Visit  
  cpb: number | null // Cost Per Booking
  conversionRate: number
  visitRate: number
  bookingRate: number
}

export async function GET(request: NextRequest) {
  try {
    const session = await requireAuth()
    const { searchParams } = new URL(request.url)
    const params = querySchema.parse(Object.fromEntries(searchParams))

    // Build date filter
    const dateFilter: any = {}
    if (params.from || params.to) {
      dateFilter.createdAt = {}
      if (params.from) {
        dateFilter.createdAt.gte = new Date(params.from)
      }
      if (params.to) {
        dateFilter.createdAt.lte = new Date(params.to)
      }
    }

    // Build where clause
    const where: any = {
      orgId: session.orgId,
      ...dateFilter
    }

    if (params.source) {
      where.source = params.source
    }

    if (params.campaign) {
      where.campaign = {
        contains: params.campaign,
        mode: 'insensitive'
      }
    }

    // Get all leads with the filters
    const leads = await db.lead.findMany({
      where,
      select: {
        source: true,
        campaign: true,
        stage: true,
        siteVisits: {
          select: {
            status: true
          }
        }
      }
    })

    // Group by source and campaign
    const metricsMap = new Map<string, SourceMetric>()

    leads.forEach(lead => {
      const key = `${lead.source}|${lead.campaign || ''}`
      
      if (!metricsMap.has(key)) {
        metricsMap.set(key, {
          source: lead.source,
          campaign: lead.campaign,
          leads: 0,
          siteVisits: 0,
          bookings: 0,
          cpl: null,
          cpsv: null,
          cpb: null,
          conversionRate: 0,
          visitRate: 0,
          bookingRate: 0
        })
      }

      const metric = metricsMap.get(key)!
      metric.leads++

      // Count site visits (scheduled or done)
      if (lead.siteVisits.length > 0) {
        metric.siteVisits++
      }

      // Count bookings
      if (lead.stage === Stage.BOOKED) {
        metric.bookings++
      }
    })

    // Calculate rates and mock costs (in a real app, you'd have actual spend data)
    const metrics = Array.from(metricsMap.values()).map(metric => {
      // Mock cost data - in production, integrate with ad platform APIs
      const mockCostPerLead = {
        'google_ads': 250, // ₹250 per lead
        'meta': 180,       // ₹180 per lead
        'website': 0,      // Organic
        'csv': 0          // No cost
      }

      const baseCost = mockCostPerLead[metric.source as keyof typeof mockCostPerLead] || 0
      const totalSpend = baseCost * metric.leads

      return {
        ...metric,
        cpl: metric.leads > 0 ? Math.round(totalSpend / metric.leads) : null,
        cpsv: metric.siteVisits > 0 ? Math.round(totalSpend / metric.siteVisits) : null,
        cpb: metric.bookings > 0 ? Math.round(totalSpend / metric.bookings) : null,
        conversionRate: metric.leads > 0 ? Math.round((metric.bookings / metric.leads) * 100 * 100) / 100 : 0,
        visitRate: metric.leads > 0 ? Math.round((metric.siteVisits / metric.leads) * 100 * 100) / 100 : 0,
        bookingRate: metric.siteVisits > 0 ? Math.round((metric.bookings / metric.siteVisits) * 100 * 100) / 100 : 0,
        totalSpend: Math.round(totalSpend)
      }
    })

    // Sort by leads descending
    metrics.sort((a, b) => b.leads - a.leads)

    // Calculate totals
    const totals = {
      leads: metrics.reduce((sum, m) => sum + m.leads, 0),
      siteVisits: metrics.reduce((sum, m) => sum + m.siteVisits, 0),
      bookings: metrics.reduce((sum, m) => sum + m.bookings, 0),
      totalSpend: metrics.reduce((sum, m) => sum + (m.totalSpend || 0), 0)
    }

    const totalCPL = totals.totalSpend > 0 && totals.leads > 0 ? Math.round(totals.totalSpend / totals.leads) : null
    const totalCPSV = totals.totalSpend > 0 && totals.siteVisits > 0 ? Math.round(totals.totalSpend / totals.siteVisits) : null
    const totalCPB = totals.totalSpend > 0 && totals.bookings > 0 ? Math.round(totals.totalSpend / totals.bookings) : null

    return NextResponse.json({
      metrics,
      totals: {
        ...totals,
        cpl: totalCPL,
        cpsv: totalCPSV,
        cpb: totalCPB,
        conversionRate: totals.leads > 0 ? Math.round((totals.bookings / totals.leads) * 100 * 100) / 100 : 0,
        visitRate: totals.leads > 0 ? Math.round((totals.siteVisits / totals.leads) * 100 * 100) / 100 : 0,
        bookingRate: totals.siteVisits > 0 ? Math.round((totals.bookings / totals.siteVisits) * 100 * 100) / 100 : 0
      },
      dateRange: {
        from: params.from,
        to: params.to
      }
    })

  } catch (error) {
    if (error instanceof z.ZodError) {
      return NextResponse.json(
        { message: 'Invalid query parameters', errors: error.errors },
        { status: 400 }
      )
    }

    console.error('Source metrics error:', error)
    return NextResponse.json(
      { message: 'Internal server error' },
      { status: 500 }
    )
  }
}