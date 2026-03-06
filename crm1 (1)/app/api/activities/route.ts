import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
)

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const companyId = searchParams.get('companyId')
    const entityType = searchParams.get('entityType')
    const entityId = searchParams.get('entityId')
    const status = searchParams.get('status')
    const activityType = searchParams.get('activityType')
    const assignedTo = searchParams.get('assignedTo')
    const limit = searchParams.get('limit') || '100'

    if (!companyId) {
      return NextResponse.json({ error: 'Company ID is required' }, { status: 400 })
    }

    let query = supabase
      .from('activities')
      .select('*')
      .eq('company_id', companyId)
      .order('created_at', { ascending: false })
      .limit(parseInt(limit))

    // Apply filters
    if (entityType) query = query.eq('entity_type', entityType)
    if (entityId) query = query.eq('entity_id', entityId)  
    if (status) query = query.eq('status', status)
    if (activityType) query = query.eq('activity_type', activityType)
    if (assignedTo) query = query.eq('assigned_to', assignedTo)

    const { data: activities, error } = await query

    if (error) {
      console.error('Error fetching activities:', error)
      return NextResponse.json({ error: 'Failed to fetch activities' }, { status: 500 })
    }

    // Also get dashboard stats
    const { data: dashboard, error: dashError } = await supabase
      .from('activity_dashboard')
      .select('*')
      .eq('company_id', companyId)
      .single()

    return NextResponse.json({ 
      activities: activities || [],
      dashboard: dashboard || {
        total_activities: 0,
        today_scheduled: 0,
        completed_today: 0,
        overdue: 0,
        pending: 0,
        follow_ups_due: 0
      }
    })

  } catch (error) {
    console.error('Activities API Error:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const { 
      companyId, 
      userId,
      activityType,
      title,
      description,
      entityType,
      entityId,
      entityName,
      contactName,
      contactPhone,
      contactEmail,
      contactWhatsapp,
      scheduledDate,
      dueDate,
      priority,
      assignedTo,
      productInterest,
      estimatedValue
    } = body

    if (!companyId || !activityType || !title) {
      return NextResponse.json({ 
        error: 'Missing required fields: companyId, activityType, title' 
      }, { status: 400 })
    }

    const { data: activity, error } = await supabase
      .from('activities')
      .insert({
        company_id: companyId,
        user_id: userId,
        activity_type: activityType,
        title,
        description,
        entity_type: entityType,
        entity_id: entityId,
        entity_name: entityName,
        contact_name: contactName,
        contact_phone: contactPhone,
        contact_email: contactEmail,
        contact_whatsapp: contactWhatsapp,
        scheduled_date: scheduledDate,
        due_date: dueDate,
        priority: priority || 'medium',
        assigned_to: assignedTo,
        product_interest: productInterest,
        estimated_value: estimatedValue,
        status: 'pending'
      })
      .select()
      .single()

    if (error) {
      console.error('Error creating activity:', error)
      return NextResponse.json({ error: 'Failed to create activity' }, { status: 500 })
    }

    return NextResponse.json({ activity })

  } catch (error) {
    console.error('Activities POST Error:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

export async function PUT(request: NextRequest) {
  try {
    const body = await request.json()
    const { id, status, outcome, outcomeNotes, nextAction, followUpRequired, followUpDate, completedDate } = body

    if (!id) {
      return NextResponse.json({ error: 'Activity ID is required' }, { status: 400 })
    }

    const updateData: any = {}
    if (status) updateData.status = status
    if (outcome) updateData.outcome = outcome
    if (outcomeNotes) updateData.outcome_notes = outcomeNotes
    if (nextAction) updateData.next_action = nextAction
    if (followUpRequired !== undefined) updateData.follow_up_required = followUpRequired
    if (followUpDate) updateData.follow_up_date = followUpDate
    if (completedDate) updateData.completed_date = completedDate
    if (status === 'completed' && !completedDate) {
      updateData.completed_date = new Date().toISOString()
    }

    const { data: activity, error } = await supabase
      .from('activities')
      .update(updateData)
      .eq('id', id)
      .select()
      .single()

    if (error) {
      console.error('Error updating activity:', error)
      return NextResponse.json({ error: 'Failed to update activity' }, { status: 500 })
    }

    return NextResponse.json({ activity })

  } catch (error) {
    console.error('Activities PUT Error:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

export async function DELETE(request: NextRequest) {
  try {
    const body = await request.json()
    const { companyId, deleteAll } = body

    if (!companyId) {
      return NextResponse.json({ error: 'Company ID is required' }, { status: 400 })
    }

    if (deleteAll) {
      const { error } = await supabase
        .from('activities')
        .delete()
        .eq('company_id', companyId)

      if (error) {
        console.error('Error deleting activities:', error)
        return NextResponse.json({ error: 'Failed to delete activities' }, { status: 500 })
      }

      return NextResponse.json({ success: true })
    }

    return NextResponse.json({ error: 'Invalid delete operation' }, { status: 400 })
  } catch (error) {
    console.error('Activities DELETE Error:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}