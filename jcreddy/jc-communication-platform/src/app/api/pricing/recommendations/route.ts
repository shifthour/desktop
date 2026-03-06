import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * GET /api/pricing/recommendations
 * List pricing recommendations with filters and trip context.
 * Query params: status, date, service_number, page, limit
 */
export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)

    const status = searchParams.get('status') || 'pending'
    const date = searchParams.get('date') || ''
    const serviceNumber = searchParams.get('service_number') || ''
    const page = parseInt(searchParams.get('page') || '1')
    const limit = parseInt(searchParams.get('limit') || '50')
    const offset = (page - 1) * limit

    let query = supabase
      .from('jc_pricing_recommendations')
      .select(`
        *,
        jc_trips (
          id,
          service_number,
          travel_date,
          departure_time,
          arrival_time,
          total_seats,
          available_seats,
          booked_seats,
          blocked_seats,
          trip_status,
          origin,
          destination
        ),
        jc_routes (
          id,
          route_name,
          origin,
          destination,
          base_fare,
          service_number,
          coach_type
        )
      `, { count: 'exact' })

    // Apply filters
    if (status && status !== 'all') {
      query = query.eq('status', status)
    }

    if (date) {
      query = query.eq('travel_date', date)
    }

    if (serviceNumber) {
      query = query.eq('service_number', serviceNumber)
    }

    // Order by demand score (highest first) then travel date
    query = query
      .order('demand_score', { ascending: false })
      .order('travel_date', { ascending: true })
      .range(offset, offset + limit - 1)

    const { data: recommendations, count, error } = await query

    if (error) throw error

    // Calculate stats
    const allPending = recommendations?.filter((r: any) => r.status === 'pending') || []
    const increases = allPending.filter((r: any) => r.fare_change_percent > 0)
    const decreases = allPending.filter((r: any) => r.fare_change_percent < 0)

    const avgChangePercent = allPending.length > 0
      ? Math.round(allPending.reduce((sum: number, r: any) => sum + r.fare_change_percent, 0) / allPending.length * 100) / 100
      : 0

    const totalRevenueImpact = allPending.reduce(
      (sum: number, r: any) => sum + (r.estimated_revenue_impact || 0), 0
    )

    // Count approved today
    const todayIST = new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' })
    const approvedToday = (recommendations || []).filter(
      (r: any) => r.status === 'approved' && r.reviewed_at?.startsWith(todayIST)
    ).length

    const stats = {
      total_pending: allPending.length,
      increases: increases.length,
      decreases: decreases.length,
      avg_change_percent: avgChangePercent,
      total_revenue_impact: Math.round(totalRevenueImpact * 100) / 100,
      approved_today: approvedToday,
    }

    return NextResponse.json({
      recommendations: recommendations || [],
      stats,
      pagination: {
        page,
        limit,
        total: count || 0,
        totalPages: Math.ceil((count || 0) / limit),
      },
    })
  } catch (error: any) {
    console.error('[Pricing Recommendations] GET error:', error.message)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

/**
 * PATCH /api/pricing/recommendations
 * Approve or reject a recommendation.
 * Body: { id, action: 'approve'|'reject', reviewed_by?, review_notes? }
 */
export async function PATCH(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const body = await request.json()

    const { id, action, reviewed_by, review_notes } = body

    if (!id || !action) {
      return NextResponse.json(
        { error: 'id and action are required' },
        { status: 400 }
      )
    }

    if (!['approve', 'reject'].includes(action)) {
      return NextResponse.json(
        { error: 'action must be "approve" or "reject"' },
        { status: 400 }
      )
    }

    // Verify recommendation exists and is pending
    const { data: existing, error: fetchError } = await supabase
      .from('jc_pricing_recommendations')
      .select('id, status')
      .eq('id', id)
      .single()

    if (fetchError || !existing) {
      return NextResponse.json(
        { error: 'Recommendation not found' },
        { status: 404 }
      )
    }

    if (existing.status !== 'pending') {
      return NextResponse.json(
        { error: `Cannot ${action} a recommendation with status "${existing.status}"` },
        { status: 400 }
      )
    }

    const newStatus = action === 'approve' ? 'approved' : 'rejected'

    const { data, error } = await supabase
      .from('jc_pricing_recommendations')
      .update({
        status: newStatus,
        reviewed_by: reviewed_by || 'operator',
        reviewed_at: new Date().toISOString(),
        review_notes: review_notes || null,
      })
      .eq('id', id)
      .select()
      .single()

    if (error) throw error

    return NextResponse.json({ success: true, recommendation: data })
  } catch (error: any) {
    console.error('[Pricing Recommendations] PATCH error:', error.message)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
