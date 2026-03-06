import { NextRequest, NextResponse } from 'next/server'
import { db } from '@/lib/database'

// Super Admin login (Product Owners only)
export async function POST(request: NextRequest) {
  try {
    const { email, password } = await request.json()

    // Validate input
    if (!email || !password) {
      return NextResponse.json(
        { success: false, error: 'Email and password are required' },
        { status: 400 }
      )
    }

    // Get the real super admin user from database
    const userResult = await db.getUserByEmail(email)
    
    if (userResult.success && userResult.data) {
      const user = userResult.data
      
      // Check if this is the super admin with correct password
      if (user.is_super_admin && user.email === 'superadmin@labgig.com' && password === 'LabGig@2025!' && user.password === 'LabGig@2025!') {
        // Update last login timestamp
        await db.updateLastLogin(user.id)
        
        return NextResponse.json({
          success: true,
          user,
          message: 'Super Admin login successful'
        })
      }
    }

    return NextResponse.json(
      { success: false, error: 'Invalid super admin credentials' },
      { status: 401 }
    )
  } catch (error) {
    console.error('Admin login error:', error)
    return NextResponse.json(
      { success: false, error: 'Internal server error' },
      { status: 500 }
    )
  }
}