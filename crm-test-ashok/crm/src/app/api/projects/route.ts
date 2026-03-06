import { NextRequest, NextResponse } from 'next/server'
import { db } from '@/lib/db'
import { requireAuth } from '@/lib/auth'

export async function GET(request: NextRequest) {
  try {
    const session = await requireAuth()

    const projects = await db.project.findMany({
      where: {
        orgId: session.orgId
      },
      select: {
        id: true,
        name: true,
        location: true
      },
      orderBy: {
        name: 'asc'
      }
    })

    return NextResponse.json({
      projects: projects || []
    })

  } catch (error) {
    console.error('Get projects error:', error)
    return NextResponse.json(
      { message: 'Internal server error' },
      { status: 500 }
    )
  }
}