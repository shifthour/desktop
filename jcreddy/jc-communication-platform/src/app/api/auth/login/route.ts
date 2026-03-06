import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'
import { cookies } from 'next/headers'

export async function POST(request: NextRequest) {
  try {
    const { username, password } = await request.json()

    if (!username || !password) {
      return NextResponse.json(
        { error: 'Username and password are required' },
        { status: 400 }
      )
    }

    const supabase = createServerClient()

    // Verify credentials using the SQL function
    const { data: users, error } = await supabase
      .rpc('verify_jc_user', {
        p_username: username,
        p_password: password
      })

    if (error) {
      console.error('Login query error:', error)
      return NextResponse.json(
        { error: 'An error occurred during login' },
        { status: 500 }
      )
    }

    if (!users || users.length === 0) {
      return NextResponse.json(
        { error: 'Invalid username or password' },
        { status: 401 }
      )
    }

    const user = users[0]

    if (!user.is_active) {
      return NextResponse.json(
        { error: 'Account is disabled' },
        { status: 403 }
      )
    }

    // Update last login
    await supabase
      .from('jc_users')
      .update({ last_login_at: new Date().toISOString() })
      .eq('id', user.id)

    // Create session data
    const sessionData = {
      id: user.id,
      username: user.username,
      email: user.email,
      displayName: user.display_name,
      role: user.role,
      loginAt: new Date().toISOString()
    }

    // Set session cookie
    const cookieStore = await cookies()
    cookieStore.set('jc_session', JSON.stringify(sessionData), {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax',
      maxAge: 60 * 60 * 24 * 7, // 7 days
      path: '/'
    })

    return NextResponse.json({
      success: true,
      user: {
        id: user.id,
        username: user.username,
        displayName: user.display_name,
        role: user.role
      }
    })
  } catch (error: any) {
    console.error('Login error:', error)
    return NextResponse.json(
      { error: 'An error occurred during login' },
      { status: 500 }
    )
  }
}
