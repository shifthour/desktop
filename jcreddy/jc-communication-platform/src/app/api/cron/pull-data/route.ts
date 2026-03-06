import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

// Route segment config for Vercel - allow 60 second timeout for cron
export const maxDuration = 60

// Secret key for cron job authentication
const CRON_SECRET = process.env.CRON_SECRET || 'mythri-cron-secret-2024'

/**
 * GET /api/cron/pull-data
 * Automated endpoint for cron-job.org to pull ALL passenger data
 *
 * Calls the main pull-data API without batching to process ALL routes
 * (same behavior as manual Pull Data page)
 *
 * Required Header: x-cron-secret: <your-secret>
 */
export async function GET(request: NextRequest) {
  const startTime = Date.now()

  try {
    // Validate cron secret
    const cronSecret = request.headers.get('x-cron-secret')
    if (cronSecret !== CRON_SECRET) {
      console.log('[Cron Pull] Unauthorized request - invalid secret')
      return NextResponse.json(
        { error: 'Unauthorized' },
        { status: 401 }
      )
    }

    // Get current date in IST (Indian Standard Time - UTC+5:30)
    const istDate = new Date().toLocaleDateString('en-CA', {
      timeZone: 'Asia/Kolkata'
    })

    console.log(`[Cron Pull] Starting FULL pull for IST date: ${istDate}`)

    const supabase = createServerClient()

    // Create a sync job record to track this cron trigger
    const { data: syncJob, error: syncError } = await supabase
      .from('jc_sync_jobs')
      .insert({
        sync_type: 'cron_pull',
        target_date: istDate,
        status: 'running',
        started_at: new Date().toISOString(),
        metadata: {
          trigger: 'cron-job.org',
          mode: 'full_sync',
        }
      })
      .select('id')
      .single()

    if (syncError) {
      console.error('[Cron Pull] Failed to create sync job:', syncError)
    }

    // Call pull-data WITHOUT batch parameter to process ALL routes
    // This is the same as what manual Pull Data page does
    const baseUrl = 'https://mythri-communication-poc.vercel.app'

    console.log(`[Cron Pull] Triggering full pull for all routes (no batching)...`)

    // Fire the request without awaiting (fire and forget)
    fetch(`${baseUrl}/api/pull-data`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        target_date: istDate,
        triggered_by: 'cron',
        sync_job_id: syncJob?.id,
      }),
    })
      .then(async (response) => {
        const data = await response.json()
        console.log(`[Cron Pull] Pull-data response: ${response.status}`, data?.summary)
      })
      .catch((err) => {
        console.error(`[Cron Pull] Pull-data error: ${err.message}`)
      })

    const latency = Date.now() - startTime

    console.log(`[Cron Pull] Full pull triggered in ${latency}ms`)

    // Update sync job - mark as "triggered" (running in background)
    if (syncJob?.id) {
      await supabase
        .from('jc_sync_jobs')
        .update({
          status: 'triggered',
          metadata: {
            trigger: 'cron-job.org',
            mode: 'full_sync',
            trigger_latency_ms: latency,
            note: 'Full pull running in background',
          }
        })
        .eq('id', syncJob.id)
    }

    return NextResponse.json({
      success: true,
      message: 'Triggered full pull for all routes',
      date: istDate,
      sync_job_id: syncJob?.id,
      latency_ms: latency,
      note: 'Full pull is running in background. Check jc_sync_jobs and trips table for results.',
      timestamp: new Date().toISOString(),
    })

  } catch (error: any) {
    console.error('[Cron Pull] Error:', error.message)

    return NextResponse.json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString(),
    }, { status: 500 })
  }
}

/**
 * POST /api/cron/pull-data
 * Alternative POST endpoint (some cron services prefer POST)
 */
export async function POST(request: NextRequest) {
  return GET(request)
}
