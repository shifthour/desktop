import { NextRequest, NextResponse } from 'next/server'
import { db } from '@/lib/db'
import { requireAuth } from '@/lib/auth'
import { Stage } from '@prisma/client'

export async function GET(request: NextRequest) {
  try {
    const session = await requireAuth()
    const { searchParams } = new URL(request.url)
    const timeFilter = searchParams.get('timeFilter')
    const projectId = searchParams.get('projectId')

    // Build date filter based on timeFilter
    let dateFilter: any = {}
    const now = new Date()
    
    switch (timeFilter) {
      case 'weekly':
        const startOfWeek = new Date(now)
        startOfWeek.setDate(now.getDate() - now.getDay())
        startOfWeek.setHours(0, 0, 0, 0)
        dateFilter = { gte: startOfWeek }
        break
      case 'monthly':
        const startOfMonth = new Date(now.getFullYear(), now.getMonth(), 1)
        dateFilter = { gte: startOfMonth }
        break
      case 'quarterly':
        const currentQuarter = Math.floor(now.getMonth() / 3)
        const startOfQuarter = new Date(now.getFullYear(), currentQuarter * 3, 1)
        dateFilter = { gte: startOfQuarter }
        break
      case 'half_yearly':
        const currentHalf = Math.floor(now.getMonth() / 6)
        const startOfHalf = new Date(now.getFullYear(), currentHalf * 6, 1)
        dateFilter = { gte: startOfHalf }
        break
      case 'yearly':
        const startOfYear = new Date(now.getFullYear(), 0, 1)
        dateFilter = { gte: startOfYear }
        break
      default:
        // 'all' - no date filter
        break
    }

    // Build where clause
    const whereClause: any = {
      orgId: session.orgId,
      ...(Object.keys(dateFilter).length > 0 && { createdAt: dateFilter }),
      ...(projectId && projectId !== 'all' && { projectId })
    }

    // Get filtered leads for the organization
    const leads = await db.lead.findMany({
      where: whereClause,
      select: {
        id: true,
        source: true,
        stage: true,
        score: true,
        createdAt: true,
        project: {
          select: {
            id: true,
            name: true
          }
        }
      }
    })

    const totalLeads = leads.length
    const newLeads = leads.filter(l => l.stage === Stage.NEW).length
    const contactedLeads = leads.filter(l => l.stage === Stage.CONTACTED).length
    const scheduledLeads = leads.filter(l => l.stage === Stage.SCHEDULED).length
    const visitedLeads = leads.filter(l => l.stage === Stage.VISITED).length
    const bookedLeads = leads.filter(l => l.stage === Stage.BOOKED).length

    const avgLeadScore = totalLeads > 0 
      ? leads.reduce((sum, lead) => sum + lead.score, 0) / totalLeads 
      : 0

    // Source breakdown
    const sourceBreakdown: { [key: string]: number } = {}
    leads.forEach(lead => {
      sourceBreakdown[lead.source] = (sourceBreakdown[lead.source] || 0) + 1
    })

    // Stage breakdown
    const stageBreakdown: { [key: string]: number } = {}
    Object.values(Stage).forEach(stage => {
      stageBreakdown[stage] = leads.filter(l => l.stage === stage).length
    })

    // Project breakdown
    const projectBreakdown: { [key: string]: number } = {}
    leads.forEach(lead => {
      const projectName = lead.project?.name || 'No Project'
      projectBreakdown[projectName] = (projectBreakdown[projectName] || 0) + 1
    })

    return NextResponse.json({
      totalLeads,
      newLeads,
      contactedLeads,
      scheduledLeads,
      visitedLeads,
      bookedLeads,
      avgLeadScore,
      sourceBreakdown,
      stageBreakdown,
      projectBreakdown
    })

  } catch (error) {
    console.error('Get reports error:', error)
    return NextResponse.json(
      { message: 'Internal server error' },
      { status: 500 }
    )
  }
}