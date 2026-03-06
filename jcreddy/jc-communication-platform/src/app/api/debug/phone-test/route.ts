import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * GET /api/debug/phone-test?phone=9742596739
 * Test phone number matching - checks both message_logs and webhooks
 */
export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url)
  const phone = searchParams.get('phone') || '9742596739'

  const supabase = createServerClient()

  // Normalize phone - same logic as webhook handler
  let cleanPhone = phone.replace(/[^0-9]/g, '')
  let phoneWithCode = cleanPhone
  let phoneWithoutCode = cleanPhone

  if (cleanPhone.startsWith('91') && cleanPhone.length === 12) {
    phoneWithoutCode = cleanPhone.substring(2)
  } else if (cleanPhone.length === 10) {
    phoneWithCode = '91' + cleanPhone
  }

  const phoneVariations = [phoneWithCode, phoneWithoutCode]

  // Query messages
  const { data: messages, error } = await supabase
    .from('jc_message_logs')
    .select('id, phone_number, status, passenger_name, created_at')
    .in('phone_number', phoneVariations)
    .order('created_at', { ascending: false })
    .limit(20)

  // Query webhooks for this phone - IMPORTANT: check both variations
  const { data: webhooks, error: webhookError } = await supabase
    .from('jc_aisensy_webhooks')
    .select('phone_number, status, message_id, created_at')
    .in('phone_number', phoneVariations)
    .order('created_at', { ascending: false })
    .limit(20)

  // What status would be used (most recent webhook)
  let resolvedStatus = null
  if (webhooks && webhooks.length > 0) {
    const firstWebhook = webhooks[0]
    const status = firstWebhook.status?.toUpperCase()
    resolvedStatus = status === 'READ' ? 'read' : status === 'DELIVERED' ? 'delivered' : status === 'SENT' ? 'sent' : null
  }

  return NextResponse.json({
    input: phone,
    cleanPhone,
    phoneVariations,
    messagesFound: messages?.length || 0,
    messages: messages?.map(m => ({
      id: m.id?.substring(0, 8),
      phone: m.phone_number,
      status: m.status,
      name: m.passenger_name,
    })),
    messageError: error?.message,
    webhooksFound: webhooks?.length || 0,
    webhooks: webhooks?.map(w => ({
      phone: w.phone_number,
      status: w.status,
      message_id: w.message_id?.substring(0, 20),
      created_at: w.created_at,
    })),
    webhookError: webhookError?.message,
    resolvedStatus: resolvedStatus,
  })
}
