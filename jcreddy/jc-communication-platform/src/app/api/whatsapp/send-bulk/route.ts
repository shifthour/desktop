import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'
import { sendWhatsAppMessageWithMedia } from '@/lib/aisensy'

// ============================================
// PRODUCTION MODE - Messages go to actual passengers
// ============================================

/**
 * POST /api/whatsapp/send-bulk
 * Send WhatsApp messages to multiple passengers using a template
 * Supports media attachments and custom template variables
 * Now also stores message logs for tracking delivery status
 */
export async function POST(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const body = await request.json()

    const {
      passengerIds,
      templateId,
      tripId, // Optional - for linking messages to a trip
      campaignName, // AISensy campaign name (can be passed directly or fetched from template)
      mediaUrl, // Optional video/image URL
      customParams, // Optional custom variables { var2: string, var3: string }
    } = body

    console.log('[Send Bulk] Received request - tripId:', tripId, 'type:', typeof tripId, 'passengerIds:', passengerIds?.length, 'campaignName:', campaignName)
    console.log('[Send Bulk] Full request body tripId value:', JSON.stringify(tripId))

    if (!passengerIds || !Array.isArray(passengerIds) || passengerIds.length === 0) {
      return NextResponse.json(
        { error: 'passengerIds array is required' },
        { status: 400 }
      )
    }

    if (!templateId && !campaignName) {
      return NextResponse.json(
        { error: 'Either templateId or campaignName is required' },
        { status: 400 }
      )
    }

    // Get the campaign name and template details
    let aiSensyCampaignName = campaignName
    let templateName = campaignName || 'Unknown'
    let resolvedTemplateId = templateId

    if (templateId) {
      const { data: template, error: templateError } = await supabase
        .from('jc_message_templates')
        .select('id, template_name, whatsapp_template_name, variables')
        .eq('id', templateId)
        .single()

      if (templateError || !template) {
        return NextResponse.json(
          { error: 'Template not found' },
          { status: 404 }
        )
      }

      aiSensyCampaignName = template.whatsapp_template_name
      templateName = template.template_name

      if (!aiSensyCampaignName) {
        return NextResponse.json(
          { error: 'Template does not have an AISensy campaign name configured' },
          { status: 400 }
        )
      }
    }

    // Fetch all passengers with their booking details
    const { data: passengers, error: passengersError } = await supabase
      .from('jc_passengers')
      .select(`
        id,
        full_name,
        mobile,
        seat_number,
        pnr_number,
        fare,
        booking_id,
        trip_id,
        jc_bookings (
          id,
          trip_id,
          travel_operator_pnr,
          origin,
          destination,
          travel_date,
          boarding_point_name,
          boarding_time,
          service_number
        )
      `)
      .in('id', passengerIds)

    if (passengersError || !passengers) {
      return NextResponse.json(
        { error: 'Failed to fetch passengers' },
        { status: 500 }
      )
    }

    // IMPORTANT: Always use tripId from the request (the trip being viewed in UI)
    // Do NOT fall back to passenger's trip_id as it may be different due to data sync issues
    let resolvedTripId = (tripId && tripId.trim() !== '') ? tripId : null

    console.log('[Send Bulk] Trip ID:', resolvedTripId, '(received:', tripId, ')')

    // CRITICAL: Verify trip_id exists in database before using it
    // This prevents foreign key constraint failures that cause batch inserts to fail silently
    let tripIdValid = false
    if (resolvedTripId) {
      const { data: tripExists, error: tripCheckError } = await supabase
        .from('jc_trips')
        .select('id')
        .eq('id', resolvedTripId)
        .single()

      tripIdValid = !!tripExists && !tripCheckError
      console.log('[Send Bulk] Trip ID validation:', tripIdValid ? 'EXISTS' : 'NOT FOUND',
                  'error:', tripCheckError?.message || 'none')

      if (!tripIdValid) {
        console.warn('[Send Bulk] WARNING: Trip ID', resolvedTripId, 'does not exist in database!')
        console.warn('[Send Bulk] This is likely due to stale cache in frontend. Setting trip_id to null for tracking.')
        // Set to null to avoid foreign key constraint failure
        // Batch will still be created for tracking purposes
        resolvedTripId = null
      }
    }

    // Group passengers by phone number to avoid duplicate messages
    // Multiple passengers with same phone = only 1 message sent
    const passengersByPhone = new Map<string, typeof passengers>()
    for (const passenger of passengers) {
      const phone = passenger.mobile
      if (phone) {
        if (!passengersByPhone.has(phone)) {
          passengersByPhone.set(phone, [])
        }
        passengersByPhone.get(phone)!.push(passenger)
      }
    }

    const uniquePhoneCount = passengersByPhone.size
    console.log('[Send Bulk] Grouped', passengers.length, 'passengers into', uniquePhoneCount, 'unique phone numbers')

    // Create a message batch record
    const batchInsertData = {
      trip_id: resolvedTripId,
      template_id: resolvedTemplateId || null,
      template_name: templateName,
      campaign_name: aiSensyCampaignName,
      total_count: uniquePhoneCount,
      sent_count: 0,
      delivered_count: 0,
      read_count: 0,
      failed_count: 0,
      media_url: mediaUrl || null,
      custom_params: customParams || null,
    }
    console.log('[Send Bulk] INSERT PAYLOAD:', JSON.stringify(batchInsertData))

    const { data: batch, error: batchError } = await supabase
      .from('jc_message_batches')
      .insert(batchInsertData)
      .select('id')
      .single()

    let batchErrorMessage: string | null = null
    let storedTripId: string | null = null
    let tripIdMismatch = false

    if (batchError) {
      console.error('[Send Bulk] Failed to create message batch:', batchError)
      batchErrorMessage = batchError.message || 'Unknown batch error'
      // Continue without batch tracking if it fails
    } else {
      console.log('[Send Bulk] Created batch with ID:', batch?.id, 'for trip_id:', resolvedTripId)

      // VERIFY: Immediately read back what was inserted
      if (batch?.id) {
        const { data: verifyInsert } = await supabase
          .from('jc_message_batches')
          .select('id, trip_id')
          .eq('id', batch.id)
          .single()
        storedTripId = verifyInsert?.trip_id || null
        tripIdMismatch = storedTripId !== resolvedTripId
        console.log('[Send Bulk] VERIFY INSERT - stored trip_id:', storedTripId, 'expected:', resolvedTripId, 'match:', !tripIdMismatch)
      }
    }

    const batchId = batch?.id || null

    // VERIFY: Immediately check if batch was actually inserted
    let verifiedBatchExists = false
    let totalBatchesForTrip = 0

    if (batchId) {
      const { data: verifyBatch, error: verifyError } = await supabase
        .from('jc_message_batches')
        .select('id, trip_id')
        .eq('id', batchId)
        .single()

      verifiedBatchExists = !!verifyBatch
      console.log('[Send Bulk] VERIFY batch exists:', verifyBatch ? 'YES' : 'NO',
                  'id:', verifyBatch?.id, 'trip_id:', verifyBatch?.trip_id,
                  'error:', verifyError?.message)

      // Also count total batches for this trip
      const { count } = await supabase
        .from('jc_message_batches')
        .select('*', { count: 'exact', head: true })
        .eq('trip_id', resolvedTripId)

      totalBatchesForTrip = count || 0
      console.log('[Send Bulk] Total batches for trip', resolvedTripId, ':', count)
    }

    // Send messages - one per unique phone number
    const results = {
      total: uniquePhoneCount,
      totalPassengers: passengers.length,
      success: 0,
      failed: 0,
      batchId: batchId,
      details: [] as Array<{
        passengerId: string
        passengerName: string
        mobile: string
        success: boolean
        error?: string
        messageId?: string
        passengerCount?: number
      }>,
    }

    const messageLogsToInsert: any[] = []

    for (const [phoneNumber, phonePassengers] of Array.from(passengersByPhone.entries())) {
      // Get the first passenger's booking for common data
      const primaryPassenger = phonePassengers[0]
      const booking = primaryPassenger.jc_bookings as any

      // Combine names and seats for all passengers with this phone number
      const allNames = phonePassengers.map(p => p.full_name).join(', ')
      const allSeats = phonePassengers.map(p => p.seat_number).join(', ')
      const totalFare = phonePassengers.reduce((sum, p) => sum + (p.fare || 0), 0)

      // Build template params based on passenger/booking data
      let templateParams: string[]

      // Check if this is a simple template (rating, feedback) that only needs passenger name
      const isRatingTemplate = aiSensyCampaignName.toLowerCase().includes('rating') ||
                               templateName.toLowerCase().includes('rating')
      const isFeedbackTemplate = aiSensyCampaignName.toLowerCase().includes('feedback') ||
                                 templateName.toLowerCase().includes('feedback')
      const isSimpleTemplate = isRatingTemplate || isFeedbackTemplate

      // Use provided media URL (image for rating templates, video for delay templates)
      const effectiveMediaUrl = mediaUrl

      if (isSimpleTemplate) {
        // Simple template - only needs passenger name
        templateParams = [
          allNames || 'Valued Customer',
        ]
      } else if (customParams?.var2 || customParams?.var3) {
        // Custom template (like delay notification) - only needs name + custom vars
        templateParams = [
          allNames || 'Valued Customer',
          customParams.var2 || '',
          customParams.var3 || '',
        ]
      } else {
        // Standard booking template - needs all booking data
        templateParams = [
          allNames || 'Valued Customer',
          primaryPassenger.pnr_number || booking?.travel_operator_pnr || 'N/A',
          booking?.origin || 'N/A',
          booking?.destination || 'N/A',
          booking?.travel_date ? formatDateForTemplate(booking.travel_date) : 'N/A',
          booking?.boarding_time ? formatTimeForTemplate(booking.boarding_time) : 'N/A',
          allSeats || 'N/A',
          booking?.boarding_point_name || 'N/A',
          totalFare ? `₹${totalFare.toLocaleString('en-IN')}` : 'N/A',
        ]
      }

      // PRODUCTION: Messages go to actual passenger phone numbers
      const targetPhoneNumber = phoneNumber

      // Send message (with optional media) - ONE message per phone number
      const result = await sendWhatsAppMessageWithMedia(
        aiSensyCampaignName,
        targetPhoneNumber,  // Use test number if in test mode
        templateParams,
        allNames,
        effectiveMediaUrl  // Use effective URL (includes default for rating templates)
      )

      const sentAt = new Date().toISOString()
      const messageId = result.data?.submitted_message_id || null

      // Create ONE message log entry per phone (not per passenger)
      // Combine all passenger names for this phone
      const messageLog: any = {
        batch_id: batchId,
        trip_id: resolvedTripId,
        passenger_id: primaryPassenger.id,
        booking_id: primaryPassenger.booking_id || booking?.id || null,
        template_id: resolvedTemplateId || null,
        phone_number: phoneNumber,
        passenger_name: allNames, // Combined names: "Miss Nishad afza, Miss Rumana, Miss Sameera"
        channel: 'whatsapp',
        template_params: templateParams,
        sent_at: sentAt,
      }

      if (result.success) {
        messageLog.aisensy_message_id = messageId
        messageLog.status = 'sent'
      } else {
        messageLog.status = 'failed'
        messageLog.failed_at = sentAt
        messageLog.error_message = result.error || 'Unknown error'
      }

      messageLogsToInsert.push(messageLog)

      if (result.success) {
        results.success++
        results.details.push({
          passengerId: primaryPassenger.id,
          passengerName: allNames,
          mobile: phoneNumber,
          success: true,
          messageId: messageId,
          passengerCount: phonePassengers.length,
        })
      } else {
        results.failed++
        results.details.push({
          passengerId: primaryPassenger.id,
          passengerName: allNames,
          mobile: phoneNumber,
          success: false,
          error: result.error,
          passengerCount: phonePassengers.length,
        })
      }

      // Add small delay between messages to avoid rate limiting
      await new Promise((resolve) => setTimeout(resolve, 100))
    }

    // Insert all message logs
    console.log('[Send Bulk] Inserting', messageLogsToInsert.length, 'message logs')
    let logsErrorMessage: string | null = null
    if (messageLogsToInsert.length > 0) {
      console.log('[Send Bulk] First log sample:', JSON.stringify(messageLogsToInsert[0], null, 2))
      const { error: logsError } = await supabase
        .from('jc_message_logs')
        .insert(messageLogsToInsert)

      if (logsError) {
        console.error('[Send Bulk] Failed to insert message logs:', logsError)
        logsErrorMessage = logsError.message || 'Unknown logs error'
      } else {
        console.log('[Send Bulk] Successfully inserted message logs')
      }
    }

    // Update batch with final counts
    if (batchId) {
      await supabase
        .from('jc_message_batches')
        .update({
          sent_count: results.success,
          failed_count: results.failed,
          updated_at: new Date().toISOString(),
        })
        .eq('id', batchId)
    }

    return NextResponse.json({
      message: `Sent ${results.success}/${results.total} messages successfully (${results.totalPassengers} passengers, ${results.total} unique phone numbers)`,
      campaignName: aiSensyCampaignName,
      tripId: resolvedTripId,
      batchId: batchId,
      // Verification info
      batchVerified: verifiedBatchExists,
      totalBatchesForTrip: totalBatchesForTrip,
      // Debug trip ID info
      tripIdDebug: {
        received: tripId,
        used: resolvedTripId,
        receivedTripValid: tripIdValid,
        stored: storedTripId,
        mismatch: tripIdMismatch,
      },
      // Error info for debugging
      dbErrors: {
        batchError: batchErrorMessage,
        logsError: logsErrorMessage,
      },
      results,
    })
  } catch (error: any) {
    console.error('Bulk WhatsApp send error:', error)
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    )
  }
}

// Helper to format date for template
function formatDateForTemplate(dateStr: string): string {
  try {
    const date = new Date(dateStr)
    if (isNaN(date.getTime())) return dateStr

    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    const day = date.getDate()
    const month = months[date.getMonth()]
    const year = date.getFullYear()

    return `${day} ${month} ${year}`
  } catch {
    return dateStr
  }
}

// Helper to format time for template
function formatTimeForTemplate(timeStr: string): string {
  if (!timeStr) return 'N/A'

  try {
    const [hours, minutes] = timeStr.split(':').map(Number)
    const period = hours >= 12 ? 'PM' : 'AM'
    const displayHours = hours % 12 || 12

    return `${displayHours}:${minutes.toString().padStart(2, '0')} ${period}`
  } catch {
    return timeStr
  }
}
