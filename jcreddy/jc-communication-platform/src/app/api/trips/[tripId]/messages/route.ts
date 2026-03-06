import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * GET /api/trips/[tripId]/messages
 * Fetch message batches and logs for a specific trip
 *
 * ROBUST APPROACH: Find messages by multiple strategies to handle trip_id mismatches
 * 1. Direct trip_id match on batches
 * 2. Via message_logs trip_id
 * 3. Via passenger phone numbers from the trip
 */
export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ tripId: string }> }
) {
  try {
    const { tripId } = await params
    const supabase = createServerClient()

    console.log('[Messages API] Looking for messages for trip:', tripId)

    let batches: any[] = []
    let matchStrategy = 'none'

    // Strategy 1: Direct trip_id match on batches
    // trip_id is the unique link - no date filter needed
    // A feedback message sent on 18th for 17th's trip will have 17th's trip_id
    const { data: directBatches, error: batchesError } = await supabase
      .from('jc_message_batches')
      .select('*')
      .eq('trip_id', tripId)
      .order('created_at', { ascending: false })

    if (directBatches && directBatches.length > 0) {
      batches = directBatches
      matchStrategy = 'direct_trip_id'
      console.log('[Messages API] Found', batches.length, 'batches via direct trip_id match')
    }

    // Strategy 2: Find via message_logs.trip_id
    if (batches.length === 0) {
      const { data: logsWithTrip } = await supabase
        .from('jc_message_logs')
        .select('batch_id')
        .eq('trip_id', tripId)
        .not('batch_id', 'is', null)

      if (logsWithTrip && logsWithTrip.length > 0) {
        const batchIds = Array.from(new Set(logsWithTrip.map((l: any) => l.batch_id)))

        const { data: batchesByLogs } = await supabase
          .from('jc_message_batches')
          .select('*')
          .in('id', batchIds)
          .order('created_at', { ascending: false })

        if (batchesByLogs && batchesByLogs.length > 0) {
          batches = batchesByLogs
          matchStrategy = 'message_logs_trip_id'
          console.log('[Messages API] Found', batches.length, 'batches via message_logs trip_id')
        }
      }
    }

    // Strategy 3: Find via passenger phone numbers from this trip
    // Only used if trip_id wasn't saved properly on batch/logs
    if (batches.length === 0) {
      console.log('[Messages API] Trying to find via passenger phone numbers...')

      // Get passengers for this trip
      const { data: passengers } = await supabase
        .from('jc_passengers')
        .select('mobile')
        .eq('trip_id', tripId)

      if (passengers && passengers.length > 0) {
        const phoneNumbers = passengers.map((p: any) => p.mobile).filter(Boolean)
        // Also add versions with country code
        const allPhones = new Set<string>()
        phoneNumbers.forEach((phone: string) => {
          allPhones.add(phone)
          if (phone.length === 10) {
            allPhones.add('91' + phone)
          } else if (phone.startsWith('91') && phone.length === 12) {
            allPhones.add(phone.substring(2))
          }
        })

        if (allPhones.size > 0) {
          // Find message logs for these phone numbers
          // Also filter by trip_id to avoid showing messages for same phone on different trips
          const { data: logsByPhone } = await supabase
            .from('jc_message_logs')
            .select('batch_id')
            .in('phone_number', Array.from(allPhones))
            .eq('trip_id', tripId)
            .not('batch_id', 'is', null)

          if (logsByPhone && logsByPhone.length > 0) {
            const batchIds = Array.from(new Set(logsByPhone.map((l: any) => l.batch_id)))

            const { data: batchesByPhone } = await supabase
              .from('jc_message_batches')
              .select('*')
              .in('id', batchIds)
              .order('created_at', { ascending: false })

            if (batchesByPhone && batchesByPhone.length > 0) {
              batches = batchesByPhone
              matchStrategy = 'passenger_phones'
              console.log('[Messages API] Found', batches.length, 'batches via passenger phones')
            }
          }
        }
      }
    }

    // No fallback - only show messages that belong to THIS trip
    // If no messages found, return empty array
    if (batches.length === 0) {
      console.log('[Messages API] No messages found for trip:', tripId)
    }

    if (batchesError) {
      console.error('Error fetching message batches:', batchesError)
      return NextResponse.json(
        { error: 'Failed to fetch message batches' },
        { status: 500 }
      )
    }

    // Fetch ALL message logs for all batches upfront
    const batchIds = batches.map(b => b.id)
    const { data: allLogs, error: allLogsError } = await supabase
      .from('jc_message_logs')
      .select('*')
      .in('batch_id', batchIds)
      .order('passenger_name', { ascending: true })

    if (allLogsError) {
      console.error('Error fetching all message logs:', allLogsError)
    }

    // Get all unique phone numbers from logs to fetch their REAL status from webhook table
    const allPhoneNumbers = new Set<string>()
    ;(allLogs || []).forEach((log: any) => {
      if (log.phone_number) {
        allPhoneNumbers.add(log.phone_number)
        // Also add with/without country code
        if (log.phone_number.length === 10) {
          allPhoneNumbers.add('91' + log.phone_number)
        } else if (log.phone_number.startsWith('91') && log.phone_number.length === 12) {
          allPhoneNumbers.add(log.phone_number.substring(2))
        }
      }
    })

    // Fetch LATEST status from jc_aisensy_webhooks for these phone numbers
    // Sort by created_at DESC so we get the MOST RECENT status first (not the highest priority)
    // This handles the case where old messages are READ but new messages are only DELIVERED
    const phoneStatusMap: Record<string, string> = {}
    console.log('[Messages API] Phone numbers to query:', Array.from(allPhoneNumbers))
    if (allPhoneNumbers.size > 0) {
      const { data: webhooks, error: webhookError } = await supabase
        .from('jc_aisensy_webhooks')
        .select('phone_number, status, created_at')
        .in('phone_number', Array.from(allPhoneNumbers))
        .order('created_at', { ascending: false })

      console.log('[Messages API] Webhooks found:', webhooks?.length, 'Error:', webhookError?.message)
      if (webhooks?.length) {
        console.log('[Messages API] First 5 webhooks:', webhooks.slice(0, 5).map(w => ({ phone: w.phone_number, status: w.status })))
      }

      // Build a map of phone -> MOST RECENT status (first one found since sorted by date DESC)
      ;(webhooks || []).forEach((w: any) => {
        const phone = w.phone_number
        const status = w.status?.toUpperCase()
        const normalizedStatus = status === 'READ' ? 'read' : status === 'DELIVERED' ? 'delivered' : status === 'SENT' ? 'sent' : null

        if (normalizedStatus) {
          // Also map to 10-digit version
          let phone10 = phone
          if (phone.startsWith('91') && phone.length === 12) {
            phone10 = phone.substring(2)
          }

          // Only set if not already set (first one is most recent due to DESC sort)
          if (!phoneStatusMap[phone10]) {
            phoneStatusMap[phone10] = normalizedStatus
            phoneStatusMap[phone] = normalizedStatus
          }
        }
      })
      console.log('[Messages API] Phone status map entries:', Object.keys(phoneStatusMap).length)
      console.log('[Messages API] Sample mappings:', Object.entries(phoneStatusMap).slice(0, 5))
    }

    // Group logs by batch_id and UPDATE status from webhook table
    const logsByBatch: Record<string, any[]> = {}
    ;(allLogs || []).forEach((log: any) => {
      if (!logsByBatch[log.batch_id]) {
        logsByBatch[log.batch_id] = []
      }

      // Override status with REAL status from webhook table if available
      const webhookStatus = phoneStatusMap[log.phone_number]
      if (webhookStatus) {
        log.status = webhookStatus
      }

      logsByBatch[log.batch_id].push(log)
    })

    // Build batches with logs and calculate live counts
    const batchesWithLogs = batches.map((batch) => {
      const logs = logsByBatch[batch.id] || []

      // Calculate counts from actual message statuses
      const liveCounts = {
        sent: 0,
        delivered: 0,
        read: 0,
        failed: 0,
      }
      logs.forEach((log: any) => {
        if (log.status === 'sent') liveCounts.sent++
        else if (log.status === 'delivered') liveCounts.delivered++
        else if (log.status === 'read') liveCounts.read++
        else if (log.status === 'failed') liveCounts.failed++
      })

      return {
        ...batch,
        sent_count: liveCounts.sent,
        delivered_count: liveCounts.delivered,
        read_count: liveCounts.read,
        failed_count: liveCounts.failed,
        sent_at: batch.sent_at || batch.created_at,
        messages: logs,
      }
    })

    // Calculate overall stats from live batch data
    const stats = {
      totalBatches: batchesWithLogs?.length || 0,
      totalMessages: batchesWithLogs?.reduce((sum, b) => sum + (b.messages?.length || 0), 0) || 0,
      totalSent: batchesWithLogs?.reduce((sum, b) => sum + (b.sent_count || 0), 0) || 0,
      totalDelivered: batchesWithLogs?.reduce((sum, b) => sum + (b.delivered_count || 0), 0) || 0,
      totalRead: batchesWithLogs?.reduce((sum, b) => sum + (b.read_count || 0), 0) || 0,
      totalFailed: batchesWithLogs?.reduce((sum, b) => sum + (b.failed_count || 0), 0) || 0,
    }

    console.log('[Messages API] Returning', batchesWithLogs.length, 'batches, strategy:', matchStrategy)

    const response = NextResponse.json({
      tripId,
      batches: batchesWithLogs,
      stats,
      matchStrategy,
    })

    // Disable caching to always get fresh data
    response.headers.set('Cache-Control', 'no-store, no-cache, must-revalidate')
    response.headers.set('Pragma', 'no-cache')

    return response
  } catch (error: any) {
    console.error('Error in trip messages API:', error)
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    )
  }
}
