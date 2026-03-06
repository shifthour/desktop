import { NextRequest, NextResponse } from 'next/server'
import { db } from '@/lib/db'
import { requireRole } from '@/lib/auth'
import { UserRole } from '@prisma/client'

export async function GET(request: NextRequest) {
  try {
    const session = await requireRole([UserRole.ADMIN, UserRole.MANAGER])

    // Get system statistics
    const [totalUsers, totalLeads, totalActivities, totalVisits] = await Promise.all([
      db.user.count({
        where: { orgId: session.orgId }
      }),
      db.lead.count({
        where: { orgId: session.orgId }
      }),
      db.activity.count({
        where: {
          lead: { orgId: session.orgId }
        }
      }),
      db.siteVisit.count({
        where: {
          lead: { orgId: session.orgId }
        }
      })
    ])

    return NextResponse.json({
      totalUsers,
      totalLeads,
      totalActivities,
      totalVisits,
      systemHealth: 'healthy' as const,
      lastBackup: null // Would be populated from actual backup system
    })

  } catch (error) {
    console.error('Get admin stats error:', error)
    return NextResponse.json(
      { message: 'Internal server error' },
      { status: 500 }
    )
  }
}