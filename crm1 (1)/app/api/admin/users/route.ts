import { NextRequest, NextResponse } from 'next/server'
import { db } from '@/lib/database'

// GET /api/admin/users - Get users for a company
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const companyId = searchParams.get('companyId')

    if (!companyId) {
      return NextResponse.json(
        { error: 'Company ID is required' },
        { status: 400 }
      )
    }

    const result = await db.getCompanyUsers(companyId)
    
    if (!result.success) {
      return NextResponse.json(
        { error: result.error },
        { status: 400 }
      )
    }

    return NextResponse.json(result.data)
  } catch (error) {
    console.error('Get users error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}

// POST /api/admin/users - Create new user
export async function POST(request: NextRequest) {
  try {
    const { companyId, email, fullName, roleId, password, isAdmin } = await request.json()

    // Validate input
    if (!companyId || !email || !fullName || !roleId || !password) {
      return NextResponse.json(
        { error: 'All fields are required' },
        { status: 400 }
      )
    }

    // Validate email format
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/
    if (!emailRegex.test(email)) {
      return NextResponse.json(
        { error: 'Invalid email format' },
        { status: 400 }
      )
    }

    const result = await db.createUser({
      companyId,
      email,
      fullName,
      roleId,
      password,
      isAdmin: isAdmin || false
    })

    if (!result.success) {
      return NextResponse.json(
        { error: result.error },
        { status: 400 }
      )
    }

    // Create notification for company admins about new user
    await db.createNotification({
      companyId: companyId,
      title: 'New User Added',
      message: `${fullName} (${email}) has been added to your company`,
      type: 'info',
      entityType: 'user',
      entityId: result.data.id
    })

    // Create notification for super admin if it's a company admin being created
    if (isAdmin) {
      await db.createNotification({
        title: 'New Company Admin Created',
        message: `${fullName} has been assigned as company admin for ${result.data.company?.name}`,
        type: 'info',
        entityType: 'user',
        entityId: result.data.id
      })
    }

    return NextResponse.json(result.data, { status: 201 })
  } catch (error) {
    console.error('Create user error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}

// PUT /api/admin/users - Update user
export async function PUT(request: NextRequest) {
  try {
    const { userId, email, fullName, roleId, isActive, isAdmin } = await request.json()

    if (!userId) {
      return NextResponse.json(
        { error: 'User ID is required' },
        { status: 400 }
      )
    }

    const result = await db.updateUser(userId, {
      email,
      fullName,
      roleId,
      isActive,
      isAdmin
    })

    if (!result.success) {
      return NextResponse.json(
        { error: result.error },
        { status: 400 }
      )
    }

    return NextResponse.json(result.data)
  } catch (error) {
    console.error('Update user error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}

// DELETE /api/admin/users - Delete user
export async function DELETE(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const userId = searchParams.get('userId')

    if (!userId) {
      return NextResponse.json(
        { error: 'User ID is required' },
        { status: 400 }
      )
    }

    // Get user details before deletion for notification
    const userResult = await db.getUserById(userId)
    const userToDelete = userResult.success ? userResult.data : null
    
    const result = await db.deleteUser(userId)

    if (!result.success) {
      return NextResponse.json(
        { error: result.error },
        { status: 400 }
      )
    }

    // Create notification about user deletion
    if (userToDelete) {
      await db.createNotification({
        companyId: userToDelete.company_id,
        title: 'User Removed',
        message: `${userToDelete.full_name} (${userToDelete.email}) has been removed from the system`,
        type: 'warning',
        entityType: 'user',
        entityId: userId
      })

      // Also notify super admin if it was a company admin
      if (userToDelete.is_admin) {
        await db.createNotification({
          title: 'Company Admin Removed',
          message: `Company admin ${userToDelete.full_name} has been removed from ${userToDelete.company?.name}`,
          type: 'warning',
          entityType: 'user',
          entityId: userId
        })
      }
    }

    return NextResponse.json({ message: 'User deleted successfully' })
  } catch (error) {
    console.error('Delete user error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}