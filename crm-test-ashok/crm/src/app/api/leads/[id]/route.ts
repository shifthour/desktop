import { NextRequest, NextResponse } from 'next/server'
import { z } from 'zod'
import { db } from '@/lib/db'
import { requireAuth } from '@/lib/auth'
import { scoreLead } from '@/lib/scoring'
import { ActivityType, Stage } from '@prisma/client'

const updateLeadSchema = z.object({
  name: z.string().optional(),
  phone: z.string().optional(),
  email: z.string().email().optional().or(z.literal('')),
  stage: z.nativeEnum(Stage).optional(),
  bedroomsPref: z.number().int().positive().optional(),
  budgetMin: z.number().int().positive().optional(),
  budgetMax: z.number().int().positive().optional(),
  locationPref: z.string().optional(),
  projectId: z.string().optional(),
  assignedToId: z.string().optional()
})

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const session = await requireAuth()
    const { id } = await params

    const lead = await db.lead.findUnique({
      where: {
        id,
        orgId: session.orgId
      },
      include: {
        project: true,
        assignedTo: {
          select: {
            id: true,
            name: true,
            email: true
          }
        },
        activities: {
          include: {
            user: {
              select: {
                id: true,
                name: true
              }
            }
          },
          orderBy: {
            at: 'desc'
          }
        },
        siteVisits: {
          include: {
            project: {
              select: {
                id: true,
                name: true,
                location: true
              }
            }
          },
          orderBy: {
            scheduledAt: 'desc'
          }
        }
      }
    })

    if (!lead) {
      return NextResponse.json(
        { message: 'Lead not found' },
        { status: 404 }
      )
    }

    // Don't mask PII in detail view, user has permission to see full details
    return NextResponse.json(lead)

  } catch (error) {
    console.error('Get lead error:', error)
    return NextResponse.json(
      { message: 'Internal server error' },
      { status: 500 }
    )
  }
}

export async function PATCH(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const session = await requireAuth()
    const { id } = await params
    const body = await request.json()
    const updateData = updateLeadSchema.parse(body)

    // Get the existing lead
    const existingLead = await db.lead.findUnique({
      where: {
        id,
        orgId: session.orgId
      },
      include: {
        assignedTo: true
      }
    })

    if (!existingLead) {
      return NextResponse.json(
        { message: 'Lead not found' },
        { status: 404 }
      )
    }

    // Recalculate score if relevant fields changed
    const newScore = scoreLead({
      ...existingLead,
      ...updateData
    })

    // Update the lead
    const updatedLead = await db.lead.update({
      where: { id },
      data: {
        ...updateData,
        email: updateData.email || null,
        score: newScore
      },
      include: {
        project: true,
        assignedTo: {
          select: {
            id: true,
            name: true,
            email: true
          }
        }
      }
    })

    // Create activities for significant changes
    const activities = []

    if (updateData.stage && updateData.stage !== existingLead.stage) {
      activities.push({
        leadId: id,
        userId: session.id,
        type: ActivityType.STATUS_CHANGE,
        note: `Stage changed from ${existingLead.stage} to ${updateData.stage}`,
        payload: {
          oldStage: existingLead.stage,
          newStage: updateData.stage
        }
      })
    }

    if (updateData.assignedToId && updateData.assignedToId !== existingLead.assignedToId) {
      const newAssignee = updateData.assignedToId ? 
        await db.user.findUnique({
          where: { id: updateData.assignedToId },
          select: { name: true }
        }) : null

      activities.push({
        leadId: id,
        userId: session.id,
        type: ActivityType.ASSIGNMENT,
        note: `Lead reassigned to ${newAssignee?.name || 'Unassigned'}`,
        payload: {
          oldAssignee: existingLead.assignedTo?.name,
          newAssignee: newAssignee?.name || null
        }
      })
    }

    if (activities.length > 0) {
      await db.activity.createMany({
        data: activities
      })
    }

    return NextResponse.json({
      ...updatedLead,
      message: 'Lead updated successfully'
    })

  } catch (error) {
    if (error instanceof z.ZodError) {
      return NextResponse.json(
        { message: 'Invalid input', errors: error.errors },
        { status: 400 }
      )
    }

    console.error('Update lead error:', error)
    return NextResponse.json(
      { message: 'Internal server error' },
      { status: 500 }
    )
  }
}