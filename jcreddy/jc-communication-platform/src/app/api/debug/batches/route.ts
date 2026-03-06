import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * GET /api/debug/batches
 * Debug endpoint to see all recent batches
 */
export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()

    // Get all recent batches
    const { data: batches, error } = await supabase
      .from('jc_message_batches')
      .select('id, trip_id, template_name, campaign_name, total_count, created_at')
      .order('created_at', { ascending: false })
      .limit(15)

    if (error) {
      return NextResponse.json({ error: error.message }, { status: 500 })
    }

    return NextResponse.json({
      totalBatches: batches?.length || 0,
      batches: batches?.map(b => ({
        id: b.id,
        trip_id: b.trip_id,
        template_name: b.template_name,
        campaign_name: b.campaign_name,
        total_count: b.total_count,
        created_at: b.created_at,
      }))
    })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
