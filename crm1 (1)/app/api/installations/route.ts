import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

const supabase = createClient(supabaseUrl, supabaseServiceKey)

export async function GET(request: NextRequest) {
  try {
    let query = supabase
      .from('installations')
      .select('*')
      .order('created_at', { ascending: false })

    const { data: installations, error } = await query

    if (error) {
      console.error('Error fetching installations:', error)
      return NextResponse.json(
        { error: 'Failed to fetch installations' },
        { status: 500 }
      )
    }

    return NextResponse.json({ installations })
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    
    // Debug logging to see what data is being sent
    console.log('=== INSTALLATION DEBUG ===')
    console.log('Received body:', JSON.stringify(body, null, 2))
    console.log('Body keys:', Object.keys(body))
    
    const companyId = body.companyId || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'

    // Generate installation number if not provided
    const generateInstallationNumber = () => {
      const year = new Date().getFullYear()
      const randomNum = Math.floor(Math.random() * 9000) + 1000 // 4-digit random number
      return `INST-${year}-${randomNum}`
    }

    const installationData = {
      ...body,
      installation_number: body.installation_number || generateInstallationNumber(),
      company_id: companyId,
      created_by: 'f41e509a-4c92-4baa-bca2-ab1d3410e465',
      updated_by: 'f41e509a-4c92-4baa-bca2-ab1d3410e465'
    }
    
    console.log('Installation data to insert:', JSON.stringify(installationData, null, 2))
    console.log('Installation data keys:', Object.keys(installationData))

    const { data: installation, error } = await supabase
      .from('installations')
      .insert([installationData])
      .select()
      .single()

    if (error) {
      console.error('Error creating installation:', error)
      return NextResponse.json(
        { error: 'Failed to create installation' },
        { status: 500 }
      )
    }

    return NextResponse.json({ installation })
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}

export async function PUT(request: NextRequest) {
  try {
    const body = await request.json()
    const { id, ...updateData } = body

    const installationData = {
      ...updateData,
      updated_by: 'f41e509a-4c92-4baa-bca2-ab1d3410e465'
    }

    const { data: installation, error } = await supabase
      .from('installations')
      .update(installationData)
      .eq('id', id)
      .select()
      .single()

    if (error) {
      console.error('Error updating installation:', error)
      return NextResponse.json(
        { error: 'Failed to update installation' },
        { status: 500 }
      )
    }

    return NextResponse.json({ installation })
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}

export async function DELETE(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const id = searchParams.get('id')

    if (!id) {
      return NextResponse.json(
        { error: 'Installation ID is required' },
        { status: 400 }
      )
    }

    const { error } = await supabase
      .from('installations')
      .delete()
      .eq('id', id)

    if (error) {
      console.error('Error deleting installation:', error)
      return NextResponse.json(
        { error: 'Failed to delete installation' },
        { status: 500 }
      )
    }

    return NextResponse.json({ success: true })
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}