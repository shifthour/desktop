import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * Routes API - List all routes for filters
 */
export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()

    // NOTE: Supabase default limit is 1000, we need to explicitly set higher limit
    // Sort by bitla_route_id (route ID) for consistent ordering in dropdowns
    const { data: routes, error } = await supabase
      .from('jc_routes')
      .select('id, bitla_route_id, route_name, service_number, origin, destination')
      .order('bitla_route_id', { ascending: true })
      .range(0, 9999)

    if (error) {
      throw error
    }

    return NextResponse.json({ routes })
  } catch (error: any) {
    console.error('Routes API error:', error)
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    )
  }
}
