import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

const supabase = createClient(supabaseUrl, supabaseServiceKey)

// GET - Fetch subindustries for a given industry
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const industry = searchParams.get('industry')

    if (industry) {
      // Fetch subindustries for specific industry
      const { data, error } = await supabase
        .from('industry_subindustry_mapping')
        .select('subindustry, display_order')
        .eq('industry', industry)
        .eq('is_active', true)
        .order('display_order', { ascending: true })

      if (error) {
        console.error('Error fetching subindustries:', error)
        return NextResponse.json({ error: error.message }, { status: 500 })
      }

      // Return just the subindustry names as an array
      const subindustries = data?.map(item => item.subindustry) || []
      return NextResponse.json(subindustries)
    } else {
      // Fetch all industries (unique)
      const { data, error } = await supabase
        .from('industry_subindustry_mapping')
        .select('industry')
        .eq('is_active', true)

      if (error) {
        console.error('Error fetching industries:', error)
        return NextResponse.json({ error: error.message }, { status: 500 })
      }

      // Get unique industries
      const industries = [...new Set(data?.map(item => item.industry) || [])]
      return NextResponse.json(industries)
    }
  } catch (error: any) {
    console.error('GET industries error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

// POST - Add new industry-subindustry mapping (admin only)
export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const { industry, subindustry, displayOrder } = body

    if (!industry || !subindustry) {
      return NextResponse.json({ error: 'Industry and subindustry are required' }, { status: 400 })
    }

    const { data, error } = await supabase
      .from('industry_subindustry_mapping')
      .insert({
        industry,
        subindustry,
        display_order: displayOrder || 999,
        is_active: true
      })
      .select()
      .single()

    if (error) {
      console.error('Error creating industry mapping:', error)
      return NextResponse.json({ error: error.message }, { status: 500 })
    }

    return NextResponse.json(data, { status: 201 })
  } catch (error: any) {
    console.error('POST industry error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
