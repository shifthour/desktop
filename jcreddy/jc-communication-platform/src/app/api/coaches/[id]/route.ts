import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * Get a single coach by ID
 */
export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params
    const supabase = createServerClient()

    const { data: coach, error } = await supabase
      .from('jc_coaches')
      .select('*')
      .eq('id', id)
      .single()

    if (error) {
      return NextResponse.json({ error: error.message }, { status: 404 })
    }

    return NextResponse.json({ coach })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

/**
 * Update a coach
 */
export async function PUT(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params
    const supabase = createServerClient()
    const body = await request.json()

    // Check if coach exists
    const { data: existing } = await supabase
      .from('jc_coaches')
      .select('id')
      .eq('id', id)
      .single()

    if (!existing) {
      return NextResponse.json({ error: 'Coach not found' }, { status: 404 })
    }

    // If changing coach_number, check for duplicates
    if (body.coach_number) {
      const { data: duplicate } = await supabase
        .from('jc_coaches')
        .select('id')
        .eq('coach_number', body.coach_number)
        .neq('id', id)
        .single()

      if (duplicate) {
        return NextResponse.json(
          { error: 'Another coach with this number already exists' },
          { status: 400 }
        )
      }
    }

    const updateData: any = {}

    // Only include fields that are provided
    const allowedFields = [
      'coach_number', 'coach_name', 'coach_type', 'bus_type', 'permit_type',
      'make', 'model', 'color', 'year_of_manufacture',
      'chassis_number', 'engine_number', 'motor_serial_number', 'licence_plate',
      'total_seats', 'seating_capacity', 'sleeping_capacity', 'fuel_capacity', 'fuel_type',
      'master_odometer', 'current_odometer',
      'home_city', 'current_location', 'coach_mobile_number',
      'status', 'rto_status',
      'rc_document_url', 'insurance_document_url', 'permit_document_url',
      'fitness_document_url', 'puc_document_url',
      'insurance_expiry', 'permit_expiry', 'fitness_expiry', 'puc_expiry', 'tax_expiry',
      'amenities', 'remarks', 'layout_config'
    ]

    for (const field of allowedFields) {
      if (body[field] !== undefined) {
        updateData[field] = body[field]
      }
    }

    const { data: coach, error } = await supabase
      .from('jc_coaches')
      .update(updateData)
      .eq('id', id)
      .select()
      .single()

    if (error) {
      console.error('Error updating coach:', error)
      return NextResponse.json({ error: error.message }, { status: 500 })
    }

    return NextResponse.json({ coach, message: 'Coach updated successfully' })
  } catch (error: any) {
    console.error('Update coach error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

/**
 * Delete a coach (soft delete)
 */
export async function DELETE(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params
    const supabase = createServerClient()

    // Soft delete by setting is_active to false
    const { error } = await supabase
      .from('jc_coaches')
      .update({ is_active: false })
      .eq('id', id)

    if (error) {
      console.error('Error deleting coach:', error)
      return NextResponse.json({ error: error.message }, { status: 500 })
    }

    return NextResponse.json({ message: 'Coach deleted successfully' })
  } catch (error: any) {
    console.error('Delete coach error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
