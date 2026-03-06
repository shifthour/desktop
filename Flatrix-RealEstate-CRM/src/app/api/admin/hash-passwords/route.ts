import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'
import bcrypt from 'bcryptjs'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!
const supabase = createClient(supabaseUrl, supabaseServiceKey)

export async function POST(request: NextRequest) {
  try {
    // Get all users with plain text passwords
    const { data: users, error } = await supabase
      .from('flatrix_users')
      .select('id, email, password')

    if (error) throw error

    const results = []

    for (const user of users || []) {
      // Check if password is already hashed
      const isBcryptHash = user.password?.startsWith('$2a$') || user.password?.startsWith('$2b$')

      if (!isBcryptHash && user.password) {
        // Password is plain text, hash it
        const hashedPassword = await bcrypt.hash(user.password, 12)

        // Update in database
        const { error: updateError } = await supabase
          .from('flatrix_users')
          .update({ password: hashedPassword })
          .eq('id', user.id)

        if (updateError) {
          results.push({
            email: user.email,
            success: false,
            error: updateError.message
          })
        } else {
          results.push({
            email: user.email,
            success: true,
            message: 'Password hashed successfully'
          })
        }
      } else {
        results.push({
          email: user.email,
          success: true,
          message: 'Password already hashed, skipped'
        })
      }
    }

    return NextResponse.json({
      success: true,
      totalUsers: users?.length || 0,
      results
    })

  } catch (error: any) {
    console.error('Error hashing passwords:', error)
    return NextResponse.json(
      {
        success: false,
        error: error.message
      },
      { status: 500 }
    )
  }
}
