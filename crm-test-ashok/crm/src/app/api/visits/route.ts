import { NextRequest, NextResponse } from 'next/server'
import { db } from '@/lib/db'
import { requireAuth } from '@/lib/auth'

export async function GET(request: NextRequest) {
  try {
    const session = await requireAuth()

    const visits = await db.siteVisit.findMany({
      where: {
        lead: {
          orgId: session.orgId
        }
      },
      include: {
        lead: {
          select: {
            id: true,
            name: true,
            phone: true,
            email: true,
            source: true,
            stage: true,
            score: true
          }
        },
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
    })

    return NextResponse.json({
      visits: visits || []
    })

  } catch (error) {
    console.error('Get visits error:', error)
    return NextResponse.json(
      { message: 'Internal server error' },
      { status: 500 }
    )
  }
}