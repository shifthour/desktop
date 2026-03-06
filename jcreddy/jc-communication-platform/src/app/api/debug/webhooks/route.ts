import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * GET /api/debug/webhooks
 * Debug endpoint to check AISensy webhook events received
 */
export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()

    // Get recent AISensy webhook events
    const { data: aisensyWebhooks, error: aisensyError } = await supabase
      .from('jc_aisensy_webhooks')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(20)

    // Get recent message batches
    const { data: messageBatches, error: batchesError } = await supabase
      .from('jc_message_batches')
      .select('id, trip_id, template_name, created_at, total_count, sent_count, delivered_count, read_count, failed_count')
      .order('created_at', { ascending: false })
      .limit(20)

    // Get ALL message logs (not just 10) to check for matches
    const { data: messageLogs, error: logsError } = await supabase
      .from('jc_message_logs')
      .select('id, aisensy_message_id, status, sent_at, delivered_at, read_at, passenger_name, phone_number, trip_id, batch_id, updated_at')
      .order('sent_at', { ascending: false })
      .limit(100)

    // Extract webhook phones and check which exist in message logs
    const webhookPhones = new Set<string>()
    aisensyWebhooks?.forEach((w: any) => {
      if (w.phone_number) {
        webhookPhones.add(w.phone_number)
        // Also add 10-digit version
        if (w.phone_number.startsWith('91') && w.phone_number.length === 12) {
          webhookPhones.add(w.phone_number.substring(2))
        }
      }
    })

    const logPhones = new Set<string>()
    messageLogs?.forEach((m: any) => {
      if (m.phone_number) {
        logPhones.add(m.phone_number)
        // Also add with country code
        if (m.phone_number.length === 10) {
          logPhones.add('91' + m.phone_number)
        }
      }
    })

    // Find matching phones
    const matchingPhones = Array.from(webhookPhones).filter(p => logPhones.has(p))

    return NextResponse.json({
      // Message batches
      messageBatchesCount: messageBatches?.length || 0,
      messageBatchesError: batchesError?.message || null,
      messageBatches: messageBatches?.map(b => ({
        id: b.id,
        trip_id: b.trip_id,
        template_name: b.template_name,
        created_at: b.created_at,
        total_count: b.total_count,
        sent_count: b.sent_count,
        delivered_count: b.delivered_count,
        read_count: b.read_count,
        failed_count: b.failed_count,
      })),
      // Phone matching analysis
      analysis: {
        webhookPhonesCount: webhookPhones.size,
        logPhonesCount: logPhones.size,
        matchingPhones: matchingPhones,
        matchingCount: matchingPhones.length,
        webhookPhones: Array.from(webhookPhones).sort(),
        logPhones: Array.from(logPhones).sort(),
      },
      // AISensy webhooks table
      aisensyWebhooksCount: aisensyWebhooks?.length || 0,
      aisensyWebhooksError: aisensyError?.message || null,
      aisensyWebhooks: aisensyWebhooks?.map(w => ({
        id: w.id,
        message_id: w.message_id,
        status: w.status,
        phone_number: w.phone_number,
        error_message: w.error_message,
        processed: w.processed,
        created_at: w.created_at,
        raw_payload_preview: JSON.stringify(w.raw_payload)?.substring(0, 300),
      })),
      // Message logs for comparison
      messageLogsCount: messageLogs?.length || 0,
      messageLogsError: logsError?.message || null,
      messageLogs: messageLogs?.slice(0, 15).map(m => ({
        id: m.id?.substring(0, 8),
        aisensy_message_id: m.aisensy_message_id,
        phone_number: m.phone_number,
        status: m.status,
        passenger_name: m.passenger_name,
        sent_at: m.sent_at,
        delivered_at: m.delivered_at,
        read_at: m.read_at,
        trip_id: (m as any).trip_id,
        batch_id: (m as any).batch_id,
      })),
    })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
