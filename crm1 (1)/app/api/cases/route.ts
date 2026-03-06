import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
)

export async function GET(request: NextRequest) {
  try {
    console.log('GET /api/cases - Fetching cases...')
    
    const { data: cases, error } = await supabase
      .from('cases')
      .select('*')
      .order('created_at', { ascending: false })

    if (error) {
      console.error('Supabase error:', error)
      return NextResponse.json({ 
        error: 'Failed to fetch cases', 
        details: error.message 
      }, { status: 500 })
    }

    console.log(`Found ${cases?.length || 0} cases`)
    return NextResponse.json({ cases: cases || [] })

  } catch (error) {
    console.error('Error in GET /api/cases:', error)
    return NextResponse.json({ 
      error: 'Internal server error', 
      details: error instanceof Error ? error.message : 'Unknown error'
    }, { status: 500 })
  }
}

export async function POST(request: NextRequest) {
  try {
    console.log('POST /api/cases - Creating new case...')
    const body = await request.json()
    console.log('Request body:', body)

    // Generate case number if not provided
    if (!body.case_number) {
      const currentDate = new Date()
      const year = currentDate.getFullYear()
      const { data: existingCases } = await supabase
        .from('cases')
        .select('case_number')
        .like('case_number', `CASE-${year}-%`)
        .order('case_number', { ascending: false })
        .limit(1)

      let nextNumber = 1
      if (existingCases && existingCases.length > 0) {
        const lastNumber = existingCases[0].case_number.split('-')[2]
        nextNumber = parseInt(lastNumber) + 1
      }
      
      body.case_number = `CASE-${year}-${String(nextNumber).padStart(3, '0')}`
    }

    // Set default values
    body.case_date = body.case_date || new Date().toISOString().split('T')[0]
    body.company_id = body.company_id || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29' // Demo company ID
    
    // Handle empty date fields - convert empty strings to null
    if (body.follow_up_date === '') {
      body.follow_up_date = null
    }

    const { data: newCase, error } = await supabase
      .from('cases')
      .insert([body])
      .select()
      .single()

    if (error) {
      console.error('Supabase error:', error)
      return NextResponse.json({ 
        error: 'Failed to create case', 
        details: error.message 
      }, { status: 500 })
    }

    console.log('Created case:', newCase)
    return NextResponse.json({ case: newCase }, { status: 201 })

  } catch (error) {
    console.error('Error in POST /api/cases:', error)
    return NextResponse.json({ 
      error: 'Internal server error', 
      details: error instanceof Error ? error.message : 'Unknown error'
    }, { status: 500 })
  }
}

export async function PUT(request: NextRequest) {
  try {
    console.log('PUT /api/cases - Updating case...')
    const body = await request.json()
    const { id, ...updateData } = body

    if (!id) {
      return NextResponse.json({ error: 'Case ID is required' }, { status: 400 })
    }

    const { data: updatedCase, error } = await supabase
      .from('cases')
      .update(updateData)
      .eq('id', id)
      .select()
      .single()

    if (error) {
      console.error('Supabase error:', error)
      return NextResponse.json({ 
        error: 'Failed to update case', 
        details: error.message 
      }, { status: 500 })
    }

    console.log('Updated case:', updatedCase)
    return NextResponse.json({ case: updatedCase })

  } catch (error) {
    console.error('Error in PUT /api/cases:', error)
    return NextResponse.json({ 
      error: 'Internal server error', 
      details: error instanceof Error ? error.message : 'Unknown error'
    }, { status: 500 })
  }
}

export async function DELETE(request: NextRequest) {
  try {
    console.log('DELETE /api/cases - Deleting case...')
    const { searchParams } = new URL(request.url)
    const id = searchParams.get('id')

    if (!id) {
      return NextResponse.json({ error: 'Case ID is required' }, { status: 400 })
    }

    const { error } = await supabase
      .from('cases')
      .delete()
      .eq('id', id)

    if (error) {
      console.error('Supabase error:', error)
      return NextResponse.json({ 
        error: 'Failed to delete case', 
        details: error.message 
      }, { status: 500 })
    }

    console.log('Deleted case with ID:', id)
    return NextResponse.json({ message: 'Case deleted successfully' })

  } catch (error) {
    console.error('Error in DELETE /api/cases:', error)
    return NextResponse.json({ 
      error: 'Internal server error', 
      details: error instanceof Error ? error.message : 'Unknown error'
    }, { status: 500 })
  }
}