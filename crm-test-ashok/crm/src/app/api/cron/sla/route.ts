import { NextResponse } from 'next/server'
import { db } from '@/lib/db'
import { sendDailyDigest } from '@/lib/email'
import { Stage, UserRole } from '@prisma/client'

export async function GET() {
  try {
    const now = new Date()
    const fifteenMinutesAgo = new Date(now.getTime() - 15 * 60 * 1000)

    // Find overdue NEW leads (older than 15 minutes)
    const overdueLeads = await db.lead.findMany({
      where: {
        stage: Stage.NEW,
        createdAt: {
          lt: fifteenMinutesAgo
        }
      },
      include: {
        assignedTo: {
          select: {
            id: true,
            name: true,
            email: true
          }
        },
        org: {
          select: {
            id: true,
            name: true
          }
        }
      }
    })

    const processedCount = overdueLeads.length
    const agentNotifications = new Map<string, Array<typeof overdueLeads[0]>>()

    // Group overdue leads by assigned agent
    overdueLeads.forEach(lead => {
      if (lead.assignedTo) {
        const agentId = lead.assignedTo.id
        if (!agentNotifications.has(agentId)) {
          agentNotifications.set(agentId, [])
        }
        agentNotifications.get(agentId)!.push(lead)
      }
    })

    // Send notifications to agents with overdue leads
    for (const [agentId, leads] of agentNotifications) {
      const agent = leads[0].assignedTo!
      
      // Get pending leads for this agent (for daily digest)
      const pendingLeads = await db.lead.findMany({
        where: {
          assignedToId: agentId,
          stage: {
            in: [Stage.CONTACTED, Stage.SCHEDULED]
          }
        },
        orderBy: {
          createdAt: 'asc'
        },
        take: 10 // Limit for email readability
      })

      // Send daily digest email with overdue and pending leads
      try {
        await sendDailyDigest(agent, leads, pendingLeads)
      } catch (error) {
        console.error(`Failed to send digest to ${agent.email}:`, error)
      }
    }

    // Also check for upcoming site visit reminders (24h and 2h)
    const twentyFourHoursFromNow = new Date(now.getTime() + 24 * 60 * 60 * 1000)
    const twoHoursFromNow = new Date(now.getTime() + 2 * 60 * 60 * 1000)

    const upcomingSiteVisits = await db.siteVisit.findMany({
      where: {
        status: 'SCHEDULED',
        scheduledAt: {
          gte: now,
          lte: twentyFourHoursFromNow
        }
      },
      include: {
        lead: {
          include: {
            assignedTo: true,
            project: true
          }
        },
        project: true
      }
    })

    const remindersSent = []

    for (const visit of upcomingSiteVisits) {
      if (!visit.lead.assignedTo) continue

      const hoursUntilVisit = Math.round((visit.scheduledAt.getTime() - now.getTime()) / (1000 * 60 * 60))
      
      // Send reminders at 24h and 2h marks (with 30-minute tolerance)
      const shouldSendReminder = 
        (hoursUntilVisit >= 23 && hoursUntilVisit <= 25) || // 24h reminder
        (hoursUntilVisit >= 1.5 && hoursUntilVisit <= 2.5)   // 2h reminder

      if (shouldSendReminder) {
        try {
          // Import here to avoid circular dependency issues
          const { sendVisitReminderEmail } = await import('@/lib/email')
          
          await sendVisitReminderEmail(
            visit.lead.assignedTo,
            visit.lead,
            visit,
            hoursUntilVisit
          )
          
          remindersSent.push({
            visitId: visit.id,
            leadName: visit.lead.name,
            agentEmail: visit.lead.assignedTo.email,
            hoursUntil: hoursUntilVisit
          })
        } catch (error) {
          console.error(`Failed to send visit reminder:`, error)
        }
      }
    }

    // Log the SLA check results
    console.log(`SLA Check completed at ${now.toISOString()}:`, {
      overdueLeads: processedCount,
      agentsNotified: agentNotifications.size,
      visitReminders: remindersSent.length
    })

    return NextResponse.json({
      success: true,
      timestamp: now.toISOString(),
      processed: {
        overdueLeads: processedCount,
        agentsNotified: agentNotifications.size,
        visitReminders: remindersSent.length
      },
      overdueLeads: overdueLeads.map(lead => ({
        id: lead.id,
        name: lead.name,
        phone: lead.phone.slice(-4), // Mask for logs
        assignedTo: lead.assignedTo?.name,
        createdAt: lead.createdAt,
        minutesOverdue: Math.round((now.getTime() - lead.createdAt.getTime()) / (1000 * 60))
      })),
      remindersSent
    })

  } catch (error) {
    console.error('SLA cron job error:', error)
    return NextResponse.json(
      { 
        success: false, 
        error: 'Internal server error',
        timestamp: new Date().toISOString()
      },
      { status: 500 }
    )
  }
}