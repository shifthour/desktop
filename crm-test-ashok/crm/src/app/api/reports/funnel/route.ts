import { NextRequest, NextResponse } from 'next/server'
import { z } from 'zod'
import { db } from '@/lib/db'
import { requireAuth } from '@/lib/auth'
import { Stage } from '@prisma/client'

const querySchema = z.object({
  from: z.string().optional(),
  to: z.string().optional(),
  source: z.string().optional()
})

interface FunnelStage {
  stage: Stage
  count: number
  percentage: number
  dropoffRate?: number
}

export async function GET(request: NextRequest) {
  try {
    const session = await requireAuth()
    const { searchParams } = new URL(request.url)
    const params = querySchema.parse(Object.fromEntries(searchParams))

    // Build where clause
    const where: any = {
      orgId: session.orgId
    }

    if (params.from || params.to) {
      where.createdAt = {}
      if (params.from) {
        where.createdAt.gte = new Date(params.from)
      }
      if (params.to) {
        where.createdAt.lte = new Date(params.to)
      }
    }

    if (params.source) {
      where.source = params.source
    }

    // Get stage counts
    const stageCounts = await db.lead.groupBy({
      by: ['stage'],
      where,
      _count: {
        stage: true
      }
    })

    // Create a map for easy lookup
    const stageCountMap = new Map<Stage, number>()
    stageCounts.forEach(item => {
      stageCountMap.set(item.stage, item._count.stage)
    })

    // Define the funnel order
    const funnelOrder: Stage[] = [
      Stage.NEW,
      Stage.CONTACTED, 
      Stage.SCHEDULED,
      Stage.VISITED,
      Stage.NEGOTIATION,
      Stage.BOOKED,
      Stage.DROPPED
    ]

    // Calculate total leads (excluding DROPPED for percentage calculations)
    const totalLeads = Array.from(stageCountMap.entries())
      .filter(([stage]) => stage !== Stage.DROPPED)
      .reduce((sum, [, count]) => sum + count, 0)

    const droppedCount = stageCountMap.get(Stage.DROPPED) || 0

    // Build funnel data
    const funnelData: FunnelStage[] = []
    let previousCount = totalLeads

    funnelOrder.forEach((stage, index) => {
      const count = stageCountMap.get(stage) || 0
      
      let percentage = 0
      let dropoffRate: number | undefined = undefined

      if (stage === Stage.DROPPED) {
        // Special handling for dropped leads
        percentage = totalLeads > 0 ? Math.round((count / (totalLeads + count)) * 100 * 100) / 100 : 0
      } else {
        percentage = totalLeads > 0 ? Math.round((count / totalLeads) * 100 * 100) / 100 : 0
        
        // Calculate dropoff rate (how many leads didn't make it to the next stage)
        if (index > 0 && previousCount > 0) {
          const currentStageLeads = count
          const nextStageLeads = funnelOrder.slice(index + 1)
            .filter(s => s !== Stage.DROPPED)
            .reduce((sum, s) => sum + (stageCountMap.get(s) || 0), 0)
          
          if (currentStageLeads > nextStageLeads) {
            dropoffRate = Math.round(((currentStageLeads - nextStageLeads) / currentStageLeads) * 100 * 100) / 100
          }
        }
      }

      funnelData.push({
        stage,
        count,
        percentage,
        dropoffRate
      })

      if (stage !== Stage.DROPPED) {
        previousCount = count
      }
    })

    // Calculate conversion rates
    const newCount = stageCountMap.get(Stage.NEW) || 0
    const contactedCount = stageCountMap.get(Stage.CONTACTED) || 0
    const scheduledCount = stageCountMap.get(Stage.SCHEDULED) || 0
    const visitedCount = stageCountMap.get(Stage.VISITED) || 0
    const negotiationCount = stageCountMap.get(Stage.NEGOTIATION) || 0
    const bookedCount = stageCountMap.get(Stage.BOOKED) || 0

    const conversionRates = {
      newToContacted: newCount > 0 ? Math.round((contactedCount / newCount) * 100 * 100) / 100 : 0,
      contactedToScheduled: contactedCount > 0 ? Math.round((scheduledCount / contactedCount) * 100 * 100) / 100 : 0,
      scheduledToVisited: scheduledCount > 0 ? Math.round((visitedCount / scheduledCount) * 100 * 100) / 100 : 0,
      visitedToNegotiation: visitedCount > 0 ? Math.round((negotiationCount / visitedCount) * 100 * 100) / 100 : 0,
      negotiationToBooked: negotiationCount > 0 ? Math.round((bookedCount / negotiationCount) * 100 * 100) / 100 : 0,
      overallConversion: totalLeads > 0 ? Math.round((bookedCount / totalLeads) * 100 * 100) / 100 : 0
    }

    // Summary stats
    const summary = {
      totalLeads: totalLeads + droppedCount,
      activeLeads: totalLeads,
      droppedLeads: droppedCount,
      bookedLeads: bookedCount,
      conversionRate: conversionRates.overallConversion,
      dropoutRate: (totalLeads + droppedCount) > 0 ? Math.round((droppedCount / (totalLeads + droppedCount)) * 100 * 100) / 100 : 0
    }

    return NextResponse.json({
      funnel: funnelData,
      conversionRates,
      summary,
      dateRange: {
        from: params.from,
        to: params.to
      },
      source: params.source || 'all'
    })

  } catch (error) {
    if (error instanceof z.ZodError) {
      return NextResponse.json(
        { message: 'Invalid query parameters', errors: error.errors },
        { status: 400 }
      )
    }

    console.error('Funnel report error:', error)
    return NextResponse.json(
      { message: 'Internal server error' },
      { status: 500 }
    )
  }
}