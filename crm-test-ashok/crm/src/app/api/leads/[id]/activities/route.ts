import { NextRequest, NextResponse } from 'next/server'
import { z } from 'zod'
import { db } from '@/lib/db'
import { requireAuth } from '@/lib/auth'
import { ActivityType } from '@prisma/client'

const createActivitySchema = z.object({
  type: z.nativeEnum(ActivityType),
  note: z.string().min(1, 'Note is required'),
  payload: z.record(z.any()).optional()
})

export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const session = await requireAuth()
    const { id: leadId } = await params
    const body = await request.json()
    const { type, note, payload } = createActivitySchema.parse(body)

    // Verify lead exists and belongs to user's org
    const lead = await db.lead.findUnique({
      where: {
        id: leadId,
        orgId: session.orgId
      }
    })

    if (!lead) {
      return NextResponse.json(
        { message: 'Lead not found' },
        { status: 404 }
      )
    }

    // Create the activity
    const activity = await db.activity.create({
      data: {
        leadId,
        userId: session.id,
        type,
        note,
        payload: payload || {}
      },
      include: {
        user: {
          select: {
            id: true,
            name: true
          }
        }
      }
    })

    return NextResponse.json({
      ...activity,
      message: 'Activity added successfully'
    })

  } catch (error) {
    if (error instanceof z.ZodError) {
      return NextResponse.json(
        { message: 'Invalid input', errors: error.errors },
        { status: 400 }
      )
    }

    console.error('Create activity error:', error)
    return NextResponse.json(
      { message: 'Internal server error' },
      { status: 500 }
    )
  }
}