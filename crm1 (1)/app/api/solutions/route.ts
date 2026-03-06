import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
)

export async function GET(request: NextRequest) {
  try {
    console.log('GET /api/solutions - Fetching solutions...')
    
    const { data: solutions, error } = await supabase
      .from('solutions')
      .select(`
        *,
        case:case_id (
          case_number,
          title,
          customer_name
        )
      `)
      .order('created_at', { ascending: false })

    if (error) {
      console.error('Supabase error:', error)
      return NextResponse.json({ 
        error: 'Failed to fetch solutions', 
        details: error.message 
      }, { status: 500 })
    }

    console.log(`Found ${solutions?.length || 0} solutions`)
    return NextResponse.json({ solutions: solutions || [] })

  } catch (error) {
    console.error('Error in GET /api/solutions:', error)
    return NextResponse.json({ 
      error: 'Internal server error', 
      details: error instanceof Error ? error.message : 'Unknown error'
    }, { status: 500 })
  }
}

export async function POST(request: NextRequest) {
  try {
    console.log('POST /api/solutions - Creating new solution...')
    const body = await request.json()
    console.log('Request body:', body)

    // Generate solution number if not provided
    if (!body.solution_number) {
      const currentDate = new Date()
      const year = currentDate.getFullYear()
      const { data: existingSolutions } = await supabase
        .from('solutions')
        .select('solution_number')
        .like('solution_number', `SOL-${year}-%`)
        .order('solution_number', { ascending: false })
        .limit(1)

      let nextNumber = 1
      if (existingSolutions && existingSolutions.length > 0) {
        const lastNumber = existingSolutions[0].solution_number.split('-')[2]
        nextNumber = parseInt(lastNumber) + 1
      }
      
      body.solution_number = `SOL-${year}-${String(nextNumber).padStart(3, '0')}`
    }

    // Set default values
    body.solution_date = body.solution_date || new Date().toISOString().split('T')[0]
    body.company_id = body.company_id || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29' // Demo company ID

    const { data: newSolution, error } = await supabase
      .from('solutions')
      .insert([body])
      .select()
      .single()

    if (error) {
      console.error('Supabase error:', error)
      return NextResponse.json({ 
        error: 'Failed to create solution', 
        details: error.message 
      }, { status: 500 })
    }

    console.log('Created solution:', newSolution)
    return NextResponse.json({ solution: newSolution }, { status: 201 })

  } catch (error) {
    console.error('Error in POST /api/solutions:', error)
    return NextResponse.json({ 
      error: 'Internal server error', 
      details: error instanceof Error ? error.message : 'Unknown error'
    }, { status: 500 })
  }
}

export async function PUT(request: NextRequest) {
  try {
    console.log('PUT /api/solutions - Updating solution...')
    const body = await request.json()
    const { id, ...updateData } = body

    if (!id) {
      return NextResponse.json({ error: 'Solution ID is required' }, { status: 400 })
    }

    const { data: updatedSolution, error } = await supabase
      .from('solutions')
      .update(updateData)
      .eq('id', id)
      .select()
      .single()

    if (error) {
      console.error('Supabase error:', error)
      return NextResponse.json({ 
        error: 'Failed to update solution', 
        details: error.message 
      }, { status: 500 })
    }

    console.log('Updated solution:', updatedSolution)
    return NextResponse.json({ solution: updatedSolution })

  } catch (error) {
    console.error('Error in PUT /api/solutions:', error)
    return NextResponse.json({ 
      error: 'Internal server error', 
      details: error instanceof Error ? error.message : 'Unknown error'
    }, { status: 500 })
  }
}

export async function DELETE(request: NextRequest) {
  try {
    console.log('DELETE /api/solutions - Deleting solution...')
    const { searchParams } = new URL(request.url)
    const id = searchParams.get('id')

    if (!id) {
      return NextResponse.json({ error: 'Solution ID is required' }, { status: 400 })
    }

    const { error } = await supabase
      .from('solutions')
      .delete()
      .eq('id', id)

    if (error) {
      console.error('Supabase error:', error)
      return NextResponse.json({ 
        error: 'Failed to delete solution', 
        details: error.message 
      }, { status: 500 })
    }

    console.log('Deleted solution with ID:', id)
    return NextResponse.json({ message: 'Solution deleted successfully' })

  } catch (error) {
    console.error('Error in DELETE /api/solutions:', error)
    return NextResponse.json({ 
      error: 'Internal server error', 
      details: error instanceof Error ? error.message : 'Unknown error'
    }, { status: 500 })
  }
}