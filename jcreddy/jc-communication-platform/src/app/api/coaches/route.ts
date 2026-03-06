import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * Coaches API - List and manage coach/bus details
 */
export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)

    const page = parseInt(searchParams.get('page') || '1')
    const limit = parseInt(searchParams.get('limit') || '50')
    const search = searchParams.get('search') || ''
    const status = searchParams.get('status') || ''
    const coachType = searchParams.get('coachType') || ''
    const city = searchParams.get('city') || ''

    let query = supabase
      .from('jc_coaches')
      .select('*', { count: 'exact' })
      .eq('is_active', true)
      .order('created_at', { ascending: false })

    // Apply filters
    if (search) {
      query = query.or(`coach_number.ilike.%${search}%,coach_name.ilike.%${search}%,make.ilike.%${search}%,model.ilike.%${search}%,chassis_number.ilike.%${search}%`)
    }

    if (status) {
      query = query.eq('status', status)
    }

    if (coachType) {
      query = query.eq('coach_type', coachType)
    }

    if (city) {
      query = query.ilike('home_city', `%${city}%`)
    }

    // Pagination
    const offset = (page - 1) * limit
    query = query.range(offset, offset + limit - 1)

    const { data: coaches, error, count } = await query

    if (error) {
      console.error('Error fetching coaches:', error)
      return NextResponse.json({ error: error.message }, { status: 500 })
    }

    // Get stats
    const { data: statsData } = await supabase
      .from('jc_coaches')
      .select('status')
      .eq('is_active', true)

    const stats = {
      total: statsData?.length || 0,
      available: statsData?.filter(c => c.status === 'available').length || 0,
      in_service: statsData?.filter(c => c.status === 'in_service').length || 0,
      maintenance: statsData?.filter(c => c.status === 'maintenance').length || 0,
      out_of_service: statsData?.filter(c => c.status === 'out_of_service').length || 0,
      breakdown: statsData?.filter(c => c.status === 'breakdown').length || 0,
    }

    return NextResponse.json({
      coaches: coaches || [],
      stats,
      pagination: {
        page,
        limit,
        total: count || 0,
        totalPages: Math.ceil((count || 0) / limit),
      },
    })
  } catch (error: any) {
    console.error('Coaches API error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

/**
 * Create a new coach
 */
export async function POST(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const body = await request.json()

    // Validate required fields
    if (!body.coach_number) {
      return NextResponse.json(
        { error: 'Coach number is required' },
        { status: 400 }
      )
    }

    // Check for duplicate coach number
    const { data: existing } = await supabase
      .from('jc_coaches')
      .select('id')
      .eq('coach_number', body.coach_number)
      .single()

    if (existing) {
      return NextResponse.json(
        { error: 'Coach with this number already exists' },
        { status: 400 }
      )
    }

    const { data: coach, error } = await supabase
      .from('jc_coaches')
      .insert({
        coach_number: body.coach_number,
        coach_name: body.coach_name || null,
        coach_type: body.coach_type || 'Regular',
        bus_type: body.bus_type || null,
        permit_type: body.permit_type || null,
        make: body.make || null,
        model: body.model || null,
        color: body.color || null,
        year_of_manufacture: body.year_of_manufacture || null,
        chassis_number: body.chassis_number || null,
        engine_number: body.engine_number || null,
        motor_serial_number: body.motor_serial_number || null,
        licence_plate: body.licence_plate || null,
        total_seats: body.total_seats || 40,
        seating_capacity: body.seating_capacity || null,
        sleeping_capacity: body.sleeping_capacity || null,
        fuel_capacity: body.fuel_capacity || null,
        fuel_type: body.fuel_type || 'Diesel',
        master_odometer: body.master_odometer || 0,
        current_odometer: body.current_odometer || 0,
        home_city: body.home_city || null,
        current_location: body.current_location || null,
        coach_mobile_number: body.coach_mobile_number || null,
        status: body.status || 'available',
        rto_status: body.rto_status || 'operational',
        insurance_expiry: body.insurance_expiry || null,
        permit_expiry: body.permit_expiry || null,
        fitness_expiry: body.fitness_expiry || null,
        puc_expiry: body.puc_expiry || null,
        tax_expiry: body.tax_expiry || null,
        amenities: body.amenities || null,
        remarks: body.remarks || null,
      })
      .select()
      .single()

    if (error) {
      console.error('Error creating coach:', error)
      return NextResponse.json({ error: error.message }, { status: 500 })
    }

    return NextResponse.json({ coach, message: 'Coach created successfully' })
  } catch (error: any) {
    console.error('Create coach error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
