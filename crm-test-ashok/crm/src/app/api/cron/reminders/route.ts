import { NextResponse } from 'next/server'
import { db } from '@/lib/db'
import { sendVisitReminderEmail } from '@/lib/email'

export async function GET() {
  try {
    const now = new Date()
    const twentyFourHoursFromNow = new Date(now.getTime() + 24 * 60 * 60 * 1000)
    const twoHoursFromNow = new Date(now.getTime() + 2 * 60 * 60 * 1000)
    const twentyThreeHoursFromNow = new Date(now.getTime() + 23 * 60 * 60 * 1000)
    const oneAndHalfHoursFromNow = new Date(now.getTime() + 1.5 * 60 * 60 * 1000)

    // Find site visits that need reminders
    const upcomingVisits24h = await db.siteVisit.findMany({
      where: {
        status: 'SCHEDULED',
        scheduledAt: {
          gte: twentyThreeHoursFromNow,
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

    const upcomingVisits2h = await db.siteVisit.findMany({
      where: {
        status: 'SCHEDULED',
        scheduledAt: {
          gte: oneAndHalfHoursFromNow,
          lte: twoHoursFromNow
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

    const reminders24h = []
    const reminders2h = []

    // Send 24-hour reminders
    for (const visit of upcomingVisits24h) {
      if (visit.lead.assignedTo) {
        try {
          const hoursUntilVisit = Math.round((visit.scheduledAt.getTime() - now.getTime()) / (1000 * 60 * 60))
          
          await sendVisitReminderEmail(
            visit.lead.assignedTo,
            visit.lead,
            visit,
            hoursUntilVisit
          )
          
          reminders24h.push({
            visitId: visit.id,
            leadId: visit.lead.id,
            leadName: visit.lead.name,
            agentName: visit.lead.assignedTo.name,
            agentEmail: visit.lead.assignedTo.email,
            scheduledAt: visit.scheduledAt,
            hoursUntil: hoursUntilVisit
          })
        } catch (error) {
          console.error(`Failed to send 24h reminder for visit ${visit.id}:`, error)
        }
      }
    }

    // Send 2-hour reminders
    for (const visit of upcomingVisits2h) {
      if (visit.lead.assignedTo) {
        try {
          const hoursUntilVisit = Math.round((visit.scheduledAt.getTime() - now.getTime()) / (1000 * 60 * 60 * 10)) / 10 // One decimal place
          
          await sendVisitReminderEmail(
            visit.lead.assignedTo,
            visit.lead,
            visit,
            hoursUntilVisit
          )
          
          reminders2h.push({
            visitId: visit.id,
            leadId: visit.lead.id,
            leadName: visit.lead.name,
            agentName: visit.lead.assignedTo.name,
            agentEmail: visit.lead.assignedTo.email,
            scheduledAt: visit.scheduledAt,
            hoursUntil: hoursUntilVisit
          })
        } catch (error) {
          console.error(`Failed to send 2h reminder for visit ${visit.id}:`, error)
        }
      }
    }

    // Log the reminder results
    console.log(`Visit reminders sent at ${now.toISOString()}:`, {
      reminders24h: reminders24h.length,
      reminders2h: reminders2h.length,
      totalReminders: reminders24h.length + reminders2h.length
    })

    return NextResponse.json({
      success: true,
      timestamp: now.toISOString(),
      processed: {
        reminders24h: reminders24h.length,
        reminders2h: reminders2h.length,
        totalReminders: reminders24h.length + reminders2h.length
      },
      reminders: {
        twentyFourHour: reminders24h,
        twoHour: reminders2h
      }
    })

  } catch (error) {
    console.error('Reminders cron job error:', error)
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