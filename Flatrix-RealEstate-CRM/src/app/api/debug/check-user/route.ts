import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'
import bcrypt from 'bcryptjs'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!
const supabase = createClient(supabaseUrl, supabaseServiceKey)

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams
    const email = searchParams.get('email') || 'dinki@flatrix.com'
    const testPassword = searchParams.get('password') || 'Dinki@123'

    console.log('Checking user:', email)

    // Get user from database
    const { data: user, error } = await supabase
      .from('flatrix_users')
      .select('*')
      .eq('email', email)
      .single()

    if (error || !user) {
      return NextResponse.json({
        success: false,
        error: 'User not found',
        details: error
      })
    }

    console.log('User found:', {
      id: user.id,
      email: user.email,
      name: user.name,
      role: user.role,
      hasPassword: !!user.password,
      passwordLength: user.password?.length || 0,
      passwordStartsWith: user.password?.substring(0, 10) || 'N/A'
    })

    // Test password verification
    const isValid = await bcrypt.compare(testPassword, user.password)

    console.log('Password verification result:', isValid)

    // Also test if the password in database is already hashed
    const isBcryptHash = user.password?.startsWith('$2a$') || user.password?.startsWith('$2b$')

    // Try to create a hash and compare
    const newHash = await bcrypt.hash(testPassword, 12)
    const newHashWorks = await bcrypt.compare(testPassword, newHash)

    return NextResponse.json({
      success: true,
      user: {
        id: user.id,
        email: user.email,
        name: user.name,
        role: user.role
      },
      passwordInfo: {
        hasPassword: !!user.password,
        passwordLength: user.password?.length || 0,
        passwordStartsWith: user.password?.substring(0, 10) || 'N/A',
        isBcryptHash,
        passwordVerificationResult: isValid,
        testHashGeneration: newHashWorks
      }
    })

  } catch (error: any) {
    console.error('Error checking user:', error)
    return NextResponse.json(
      {
        success: false,
        error: error.message
      },
      { status: 500 }
    )
  }
}
