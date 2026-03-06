import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * POST /api/webhooks/aisensy
 * Webhook endpoint for AISensy to send message status updates
 *
 * AISensy sends status updates when messages are:
 * - delivered: Message delivered to customer's phone
 * - read: Customer opened/read the message
 * - failed: Message failed to deliver
 */
export async function POST(request: NextRequest) {
  try {
    const supabase = createServerClient()

    // Get raw body for logging
    const rawBody = await request.text()
    let payload: any = {}

    try {
      payload = JSON.parse(rawBody)
    } catch (e) {
      console.log('[AISensy Webhook] Non-JSON body received:', rawBody)
    }

    console.log('[AISensy Webhook] RAW BODY:', rawBody)
    console.log('[AISensy Webhook] PARSED:', JSON.stringify(payload))
    console.log('[AISensy Webhook] KEYS:', Object.keys(payload))

    // AISensy webhook can have different structures:
    // 1. Nested: { id, data: { message: { ... } }, topic, created_at }
    // 2. Flat: { submitted_message_id, status, phone_number, ... }
    // 3. Alternative: { messageId, status, ... }
    const messageData = payload.data?.message || payload.data || payload
    const topic = payload.topic || payload.event || ''

    // Extract AISensy specific fields - try multiple possible field names
    const messageId = messageData.submitted_message_id
      || messageData.messageId
      || messageData.message_id
      || messageData.id
      || payload.submitted_message_id
      || payload.messageId
      || payload.message_id
      || null

    const status = messageData.status
      || payload.status
      || topic  // Sometimes the topic IS the status (e.g., "delivered", "read")
      || null

    const phoneNumber = messageData.phone_number
      || messageData.phoneNumber
      || messageData.to
      || payload.phone_number
      || payload.to
      || null

    const errorMessage = messageData.failureResponse?.message
      || messageData.error
      || messageData.error_message
      || payload.error
      || null

    console.log('[AISensy Webhook] Extracted - messageId:', messageId, 'status:', status, 'phone:', phoneNumber)

    // Log to jc_aisensy_webhooks table (dedicated table for AISensy events)
    // ALWAYS log the raw payload, even if extraction fails
    const { error: insertError } = await supabase
      .from('jc_aisensy_webhooks')
      .insert({
        message_id: messageId,
        status: status,
        phone_number: phoneNumber,
        error_message: errorMessage,
        raw_payload: payload,
        processed: false,
      })

    if (insertError) {
      console.error('[AISensy Webhook] Failed to insert webhook event:', insertError)
    } else {
      console.log('[AISensy Webhook] Successfully logged to jc_aisensy_webhooks')
    }

    // Get timestamp from nested data or payload
    const timestamp = messageData.read_at || messageData.delivered_at || messageData.sent_at || payload.created_at

    const aisensyMessageId = messageId
    const messageStatus = status?.toLowerCase()
    const errorMsg = errorMessage

    // ALWAYS update ALL messages for this phone number
    // This handles the case where same phone is used for multiple message types (bus_delay, redbus_rating, etc.)
    if (phoneNumber) {
      // Normalize phone number - create all possible formats to match
      let cleanPhone = phoneNumber.replace(/[^0-9]/g, '')

      // Create variations: with and without country code
      let phoneWithCode = cleanPhone
      let phoneWithoutCode = cleanPhone

      if (cleanPhone.startsWith('91') && cleanPhone.length === 12) {
        phoneWithoutCode = cleanPhone.substring(2)
      } else if (cleanPhone.length === 10) {
        phoneWithCode = '91' + cleanPhone
      }

      const phoneVariations = [phoneWithCode, phoneWithoutCode]
      console.log('[AISensy Webhook] Finding ALL messages for phone:', phoneVariations)

      // Find ALL messages to this phone number (not just the most recent)
      const { data: allMessages, error: findAllError } = await supabase
        .from('jc_message_logs')
        .select('id, batch_id, status, phone_number, delivered_at')
        .in('phone_number', phoneVariations)

      if (findAllError) {
        console.error('[AISensy Webhook] Error finding messages:', findAllError)
      }

      // If NO messages found for this phone, CREATE a new message log entry
      // This handles messages sent directly from AISensy dashboard (not through our app)
      if (!allMessages || allMessages.length === 0) {
        console.log('[AISensy Webhook] No existing messages found for phone:', phoneWithCode, '- Creating new entry')

        const now = new Date().toISOString()
        const newLogData: any = {
          phone_number: phoneWithCode, // Store with country code
          passenger_name: 'External Message', // We don't have passenger info
          channel: 'whatsapp',
          aisensy_message_id: messageId || null,
          status: messageStatus || 'sent',
          sent_at: timestamp || now,
          created_at: now,
          updated_at: now,
        }

        // Set timestamps based on status
        if (messageStatus === 'delivered') {
          newLogData.delivered_at = timestamp || now
        } else if (messageStatus === 'read' || messageStatus === 'seen') {
          newLogData.status = 'read'
          newLogData.read_at = timestamp || now
          newLogData.delivered_at = timestamp || now
        } else if (messageStatus === 'failed' || messageStatus === 'undelivered') {
          newLogData.status = 'failed'
          newLogData.failed_at = timestamp || now
          newLogData.error_message = errorMsg || 'Delivery failed'
        }

        const { data: newLog, error: createLogError } = await supabase
          .from('jc_message_logs')
          .insert(newLogData)
          .select('id')

        if (createLogError) {
          console.error('[AISensy Webhook] Failed to create message log:', createLogError)
          return NextResponse.json({
            received: true,
            status: messageStatus,
            action: 'create_failed',
            error: createLogError.message,
            code: createLogError.code,
            details: createLogError.details,
            phone: phoneWithCode,
          })
        }

        console.log('[AISensy Webhook] Created new message log:', newLog)

        // Mark webhook as processed
        await supabase
          .from('jc_aisensy_webhooks')
          .update({ processed: true })
          .eq('phone_number', phoneNumber)
          .eq('status', messageStatus?.toUpperCase())
          .eq('processed', false)

        return NextResponse.json({
          received: true,
          status: messageStatus,
          action: 'created_new_log',
          phone: phoneWithCode,
          logId: newLog?.[0]?.id,
        })
      }

      if (allMessages && allMessages.length > 0) {
        console.log('[AISensy Webhook] Found', allMessages.length, 'messages for phone')
        console.log('[AISensy Webhook] All messages found:', JSON.stringify(allMessages.map(m => ({ id: m.id, phone: m.phone_number, status: m.status }))))

        // Status hierarchy: sent < delivered < read
        const statusRank: Record<string, number> = { sent: 1, delivered: 2, read: 3, failed: 0 }
        const newRank = statusRank[messageStatus] || 0
        console.log('[AISensy Webhook] New status:', messageStatus, 'newRank:', newRank)
        const now = new Date().toISOString()

        // Update ALL messages that need updating
        const batchIdsToUpdate = new Set<string>()
        let updatedCount = 0

        for (const msg of allMessages) {
          const currentRank = statusRank[msg.status] || 0
          console.log('[AISensy Webhook] Checking msg:', msg.id, 'status:', msg.status, 'currentRank:', currentRank, 'newRank > currentRank:', newRank > currentRank)

          // Only update if new status is higher
          if (newRank > currentRank) {
            const updateData: any = {
              updated_at: now,
            }

            if (messageStatus === 'delivered') {
              updateData.status = 'delivered'
              updateData.delivered_at = timestamp || now
            } else if (messageStatus === 'read' || messageStatus === 'seen') {
              updateData.status = 'read'
              updateData.read_at = timestamp || now
              if (!msg.delivered_at) {
                updateData.delivered_at = timestamp || now
              }
            } else if ((messageStatus === 'failed' || messageStatus === 'undelivered') && msg.status === 'sent') {
              updateData.status = 'failed'
              updateData.failed_at = timestamp || now
              updateData.error_message = errorMsg || 'Delivery failed'
            }

            if (updateData.status) {
              console.log('[AISensy Webhook] Updating msg.id:', msg.id, 'from', msg.status, 'to', updateData.status)
              const { error: updateError, data: updateResult } = await supabase
                .from('jc_message_logs')
                .update(updateData)
                .eq('id', msg.id)
                .select('id, status')

              if (updateError) {
                console.error('[AISensy Webhook] Update ERROR for', msg.id, ':', updateError)
              } else {
                console.log('[AISensy Webhook] Update SUCCESS for', msg.id, '- result:', JSON.stringify(updateResult))
                updatedCount++
                if (msg.batch_id) {
                  batchIdsToUpdate.add(msg.batch_id)
                }
              }
            }
          }
        }

        console.log('[AISensy Webhook] Updated', updatedCount, 'messages to status:', messageStatus)

        // Update batch counts for all affected batches
        for (const batchId of Array.from(batchIdsToUpdate)) {
          await updateBatchCounts(supabase, batchId)
        }

        // Mark webhook as processed
        if (phoneNumber) {
          await supabase
            .from('jc_aisensy_webhooks')
            .update({ processed: true })
            .eq('phone_number', phoneNumber)
            .eq('status', messageStatus?.toUpperCase())
            .eq('processed', false)
        }

        return NextResponse.json({
          received: true,
          status: messageStatus,
          updatedCount,
        })
      }
    }

    // No phone number provided
    console.log('[AISensy Webhook] No phone number in webhook')
    return NextResponse.json({ received: true, message: 'No phone number provided' })
  } catch (error: any) {
    console.error('[AISensy Webhook] Error:', error)
    // Return 200 to acknowledge receipt even on error (so AISensy doesn't retry)
    return NextResponse.json({
      received: true,
      error: error.message,
    })
  }
}

/**
 * Update batch counts based on current message statuses
 */
async function updateBatchCounts(supabase: any, batchId: string) {
  try {
    // Get counts of each status for this batch
    const { data: logs, error } = await supabase
      .from('jc_message_logs')
      .select('status')
      .eq('batch_id', batchId)

    if (error || !logs) return

    const counts = {
      sent: 0,
      delivered: 0,
      read: 0,
      failed: 0,
    }

    logs.forEach((log: any) => {
      if (log.status === 'sent') counts.sent++
      else if (log.status === 'delivered') counts.delivered++
      else if (log.status === 'read') counts.read++
      else if (log.status === 'failed') counts.failed++
    })

    // Update batch with new counts
    await supabase
      .from('jc_message_batches')
      .update({
        sent_count: counts.sent,
        delivered_count: counts.delivered,
        read_count: counts.read,
        failed_count: counts.failed,
        updated_at: new Date().toISOString(),
      })
      .eq('id', batchId)

  } catch (err) {
    console.error('[AISensy Webhook] Failed to update batch counts:', err)
  }
}

/**
 * GET endpoint - for webhook verification (if AISensy requires it)
 */
export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url)
  const challenge = searchParams.get('challenge') || searchParams.get('hub.challenge')

  // Return challenge for webhook verification
  if (challenge) {
    return new NextResponse(challenge, { status: 200 })
  }

  return NextResponse.json({
    status: 'active',
    endpoint: '/api/webhooks/aisensy',
    description: 'AISensy webhook endpoint for message status updates',
  })
}
