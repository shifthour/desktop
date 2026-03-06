import { NextRequest, NextResponse } from 'next/server'
import { db } from '@/lib/database'

// GET /api/notifications - Get user notifications
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const userId = searchParams.get('userId')
    const isSuperAdmin = searchParams.get('isSuperAdmin') === 'true'
    const limit = parseInt(searchParams.get('limit') || '20')

    if (!userId) {
      return NextResponse.json(
        { error: 'User ID is required' },
        { status: 400 }
      )
    }

    let result
    if (isSuperAdmin) {
      result = await db.getSuperAdminNotifications(limit)
    } else {
      result = await db.getUserNotifications(userId, limit)
    }

    if (!result.success) {
      return NextResponse.json(
        { error: result.error },
        { status: 400 }
      )
    }

    return NextResponse.json(result.data)
  } catch (error) {
    console.error('Get notifications error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}

// PUT /api/notifications - Mark notification as read
export async function PUT(request: NextRequest) {
  try {
    const { notificationId, markAllAsRead, userId } = await request.json()

    let result
    if (markAllAsRead && userId) {
      result = await db.markAllNotificationsAsRead(userId)
    } else if (notificationId) {
      result = await db.markNotificationAsRead(notificationId)
    } else {
      return NextResponse.json(
        { error: 'Either notificationId or markAllAsRead with userId is required' },
        { status: 400 }
      )
    }

    if (!result.success) {
      return NextResponse.json(
        { error: result.error },
        { status: 400 }
      )
    }

    return NextResponse.json({ message: 'Notification(s) marked as read' })
  } catch (error) {
    console.error('Update notification error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}