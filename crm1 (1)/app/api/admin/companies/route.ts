import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'
import { DatabaseService } from '@/lib/database'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
const supabase = createClient(supabaseUrl, supabaseServiceKey)
const db = new DatabaseService()

// GET /api/admin/companies - Get all companies (Super Admin only) or single company by ID
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const companyId = searchParams.get('companyId')
    
    if (companyId) {
      // Get single company by ID
      const result = await db.getCompanyById(companyId)
      
      if (!result.success) {
        return NextResponse.json(
          { error: result.error },
          { status: 400 }
        )
      }

      return NextResponse.json(result.data)
    } else {
      // Get all companies
      const result = await db.getAllCompanies()
      
      if (!result.success) {
        return NextResponse.json(
          { error: result.error },
          { status: 400 }
        )
      }

      return NextResponse.json(result.data)
    }
  } catch (error) {
    console.error('Get companies error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}

// POST /api/admin/companies - Create new company with admin user (Super Admin only)
export async function POST(request: NextRequest) {
  try {
    const { name, adminName, adminEmail, adminPassword, maxUsers } = await request.json()

    // Validate input
    if (!name || !adminName || !adminEmail || !adminPassword || !maxUsers || maxUsers < 5) {
      return NextResponse.json(
        { error: 'Invalid input. Name, admin name, admin email, password required. Minimum 5 users.' },
        { status: 400 }
      )
    }

    // Create company with admin user
    const result = await db.createCompanyWithAdmin({
      name,
      adminName,
      adminEmail,
      adminPassword,
      maxUsers: parseInt(maxUsers)
    })

    if (!result.success) {
      return NextResponse.json(
        { error: result.error },
        { status: 400 }
      )
    }

    // Create notification for super admin about new company
    await db.createNotification({
      title: 'New Company Created',
      message: `Company "${name}" has been created with admin ${adminName} (${adminEmail})`,
      type: 'success',
      entityType: 'company',
      entityId: result.data.company.id
    })

    return NextResponse.json(result.data, { status: 201 })
  } catch (error) {
    console.error('Create company error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}

// PUT /api/admin/companies - Update company
export async function PUT(request: NextRequest) {
  try {
    const body = await request.json()
    const { companyId, name, adminEmail, maxUsers } = body

    if (!companyId) {
      return NextResponse.json(
        { error: 'Company ID is required' },
        { status: 400 }
      )
    }

    // Map camelCase to snake_case for database columns
    const updateData: Record<string, any> = {}
    if (name !== undefined && name !== '') updateData.name = name
    if (maxUsers !== undefined) updateData.max_users = parseInt(maxUsers)
    if (adminEmail !== undefined && adminEmail !== '') updateData.admin_email = adminEmail

    // Only update company table if there are fields to update
    if (Object.keys(updateData).length > 0) {
      const { error } = await supabase
        .from('companies')
        .update(updateData)
        .eq('id', companyId)

      if (error) {
        console.error('Error updating company:', error)
        return NextResponse.json(
          { error: 'Failed to update company: ' + error.message },
          { status: 500 }
        )
      }
    }

    // If admin email provided, also update the admin user's email in users table
    if (adminEmail) {
      const { error: userError } = await supabase
        .from('users')
        .update({ email: adminEmail })
        .eq('company_id', companyId)
        .eq('is_admin', true)

      if (userError) {
        console.error('Error updating admin user email:', userError)
        // Don't fail the whole request if user update fails, company was already updated
      }
    }

    // Fetch updated company with admin email
    const result = await db.getCompanyById(companyId)
    if (result.success) {
      return NextResponse.json(result.data)
    }

    return NextResponse.json({ success: true })
  } catch (error: any) {
    console.error('Update company error:', error)
    return NextResponse.json(
      { error: 'Internal server error: ' + (error.message || 'Unknown error') },
      { status: 500 }
    )
  }
}

// DELETE /api/admin/companies - Delete company
export async function DELETE(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const companyId = searchParams.get('companyId')

    if (!companyId) {
      return NextResponse.json(
        { error: 'Company ID is required' },
        { status: 400 }
      )
    }

    // Get company details before deletion for notification
    const { data: companies } = await db.getAllCompanies()
    const companyToDelete = companies?.find(c => c.id === companyId)
    
    const result = await db.deleteCompany(companyId)

    if (!result.success) {
      return NextResponse.json(
        { error: result.error },
        { status: 400 }
      )
    }

    // Create notification for super admin about company deletion
    if (companyToDelete) {
      await db.createNotification({
        title: 'Company Deleted',
        message: `Company "${companyToDelete.name}" has been permanently deleted`,
        type: 'warning',
        entityType: 'company',
        entityId: companyId
      })
    }

    return NextResponse.json({ message: 'Company deleted successfully' })
  } catch (error) {
    console.error('Delete company error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}