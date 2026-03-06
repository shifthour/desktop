import { NextRequest, NextResponse } from 'next/server'
import { db } from '@/lib/database'

// Mock authentication for now (will use Supabase Auth later)
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

    // Block super admin from using this endpoint
    if (email === 'superadmin@labgig.com') {
      return NextResponse.json(
        { success: false, error: 'Super admins must use /adminlogin' },
        { status: 403 }
      )
    }

    // Demo company user credentials (until database is ready)
    if (email === 'admin@acme-instruments.com' && password === 'demo123') {
      const user = {
        id: 'demo-admin-id',
        email: 'admin@acme-instruments.com',
        full_name: 'John Admin',
        is_super_admin: false,
        is_admin: true,
        is_active: true,
        company_id: 'demo-company-id',
        password_changed: true, // Demo user has already changed password
        role: {
          name: 'company_admin',
          description: 'Company Administrator'
        },
        company: {
          name: 'Acme Instruments Ltd.',
          domain: 'acme-instruments'
        }
      }

      return NextResponse.json({
        success: true,
        user,
        message: 'Company admin login successful'
      })
    }


    // Check for new company admin with default password
    try {
      const result = await db.getUserByEmail(email)
      
      if (result.success && result.data) {
        const user = result.data

        // Check if user is trying to login with default password
        if ((password === 'Admin@123' && user.password === 'Admin@123') || (password === 'User@123' && user.password === 'User@123')) {
          // Update last login timestamp even for first time login
          await db.updateLastLogin(user.id)
          
          // First time login with default password
          return NextResponse.json({
            success: true,
            user: { ...user, password_changed: false },
            requirePasswordChange: true,
            message: 'First time login detected - password change required'
          })
        }

        // Check if password matches (in production, this would be properly hashed)
        if (user.password === password) {
          // Update last login timestamp
          await db.updateLastLogin(user.id)
          
          return NextResponse.json({
            success: true,
            user,
            message: 'Login successful'
          })
        } else {
          return NextResponse.json(
            { success: false, error: 'Invalid email or password' },
            { status: 401 }
          )
        }
      }
    } catch (dbError) {
      // Database not ready yet, that's okay for now
      console.log('Database not ready, using mock auth')
    }


    return NextResponse.json(
      { success: false, error: 'Invalid email or password' },
      { status: 401 }
    )
  } catch (error) {
    console.error('Login error:', error)
    return NextResponse.json(
      { success: false, error: 'Internal server error' },
      { status: 500 }
    )
  }
}