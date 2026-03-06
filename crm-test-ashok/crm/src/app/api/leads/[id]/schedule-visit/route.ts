import { NextRequest, NextResponse } from 'next/server'
import { z } from 'zod'
import { db } from '@/lib/db'
import { requireAuth } from '@/lib/auth'
import { ActivityType, Stage } from '@prisma/client'

const scheduleVisitSchema = z.object({
  scheduledAt: z.string().refine((date) => !isNaN(Date.parse(date)), {
    message: 'Invalid date format'
  }),
  projectId: z.string().optional(),
  note: z.string().optional()
})

export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const session = await requireAuth()
    const { id: leadId } = await params
    const body = await request.json()
    const { scheduledAt, projectId, note } = scheduleVisitSchema.parse(body)

    // Verify lead exists and belongs to user's org
    const lead = await db.lead.findUnique({
      where: {
        id: leadId,
        orgId: session.orgId
      },
      include: {
        project: true
      }
    })

    if (!lead) {
      return NextResponse.json(
        { message: 'Lead not found' },
        { status: 404 }
      )
    }

    const visitDate = new Date(scheduledAt)
    const now = new Date()

    if (visitDate <= now) {
      return NextResponse.json(
        { message: 'Visit cannot be scheduled in the past' },
        { status: 400 }
      )
    }

    // Use provided projectId or fallback to lead's project
    const finalProjectId = projectId || lead.projectId

    if (finalProjectId) {
      // Verify project exists and belongs to org
      const project = await db.project.findUnique({
        where: {
          id: finalProjectId,
          orgId: session.orgId
        }
      })

      if (!project) {
        return NextResponse.json(
          { message: 'Project not found' },
          { status: 404 }
        )
      }
    }

    // Create site visit and update lead stage in a transaction
    const result = await db.$transaction(async (tx) => {
      // Create the site visit
      const siteVisit = await tx.siteVisit.create({
        data: {
          leadId,
          projectId: finalProjectId,
          scheduledAt: visitDate
        },
        include: {
          project: {
            select: {
              id: true,
              name: true,
              location: true
            }
          }
        }
      })

      // Update lead stage to SCHEDULED
      await tx.lead.update({
        where: { id: leadId },
        data: { stage: Stage.SCHEDULED }
      })

      // Create activity
      const activity = await tx.activity.create({
        data: {
          leadId,
          userId: session.id,
          type: ActivityType.STATUS_CHANGE,
          note: note || `Site visit scheduled for ${visitDate.toLocaleString('en-IN', { 
            timeZone: 'Asia/Kolkata',
            dateStyle: 'medium',
            timeStyle: 'short'
          })}`,
          payload: {
            visitId: siteVisit.id,
            scheduledAt: visitDate,
            projectId: finalProjectId,
            projectName: siteVisit.project?.name
          }
        }
      })

      return { siteVisit, activity }
    })

    return NextResponse.json({
      ...result.siteVisit,
      activity: result.activity,
      message: 'Site visit scheduled successfully'
    })

  } catch (error) {
    if (error instanceof z.ZodError) {
      return NextResponse.json(
        { message: 'Invalid input', errors: error.errors },
        { status: 400 }
      )
    }

    console.error('Schedule visit error:', error)
    return NextResponse.json(
      { message: 'Internal server error' },
      { status: 500 }
    )
  }
}