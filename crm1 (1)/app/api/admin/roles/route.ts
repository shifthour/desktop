import { NextRequest, NextResponse } from 'next/server'
import { db } from '@/lib/database'

// GET /api/admin/roles - Get all user roles
export async function GET(request: NextRequest) {
  try {
    const result = await db.getAllRoles()
    
    if (!result.success) {
      return NextResponse.json(
        { error: result.error },
        { status: 400 }
      )
    }

    return NextResponse.json(result.data)
  } catch (error) {
    console.error('Get roles error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}