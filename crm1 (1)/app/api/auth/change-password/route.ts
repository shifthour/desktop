import { NextRequest, NextResponse } from 'next/server'
import { db } from '@/lib/database'

export async function POST(request: NextRequest) {
  try {
    const { userId, currentPassword, newPassword, isAdminUpdate } = await request.json()

    // Validate input - for admin updates, currentPassword is not required
    if (!userId || !newPassword) {
      return NextResponse.json(
        { success: false, error: 'User ID and new password are required' },
        { status: 400 }
      )
    }

    // For non-admin updates, currentPassword is required
    if (!isAdminUpdate && !currentPassword) {
      return NextResponse.json(
        { success: false, error: 'Current password is required for user updates' },
        { status: 400 }
      )
    }

    // Validate password strength
    if (newPassword.length < 8) {
      return NextResponse.json(
        { success: false, error: 'New password must be at least 8 characters long' },
        { status: 400 }
      )
    }

    // For demo purposes, handle the demo admin user
    if (userId === 'demo-admin-id') {
      // Verify current password
      if (currentPassword !== 'Admin@123' && currentPassword !== 'User@123' && currentPassword !== 'demo123') {
        return NextResponse.json(
          { success: false, error: 'Current password is incorrect' },
          { status: 401 }
        )
      }

      // In a real application, you would update the password in Supabase Auth
      // For now, we'll just return success
      return NextResponse.json({
        success: true,
        message: 'Password changed successfully'
      })
    }

    // For real database users, update the password
    try {
      // Get user to verify existence
      const user = await db.getUserById(userId)
      
      if (!user.success || !user.data) {
        return NextResponse.json(
          { success: false, error: 'User not found' },
          { status: 404 }
        )
      }

      // For non-admin updates, verify current password
      if (!isAdminUpdate) {
        if (user.data.password !== currentPassword) {
          return NextResponse.json(
            { success: false, error: 'Current password is incorrect' },
            { status: 401 }
          )
        }
      }

      // Update password in database
      const updateResult = await db.updateUserPassword(userId, newPassword)
      
      if (updateResult.success) {
        return NextResponse.json({
          success: true,
          message: isAdminUpdate ? 'Password updated by admin' : 'Password changed successfully'
        })
      } else {
        return NextResponse.json(
          { success: false, error: 'Failed to update password' },
          { status: 500 }
        )
      }
    } catch (dbError) {
      console.error('Database error during password change:', dbError)
      return NextResponse.json(
        { success: false, error: 'Database error occurred' },
        { status: 500 }
      )
    }
  } catch (error) {
    console.error('Password change error:', error)
    return NextResponse.json(
      { success: false, error: 'Internal server error' },
      { status: 500 }
    )
  }
}