import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

const VERIFY_TOKEN = process.env.WHATSAPP_WEBHOOK_VERIFY_TOKEN || '';

// GET - Meta webhook verification (challenge-response)
export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const mode = searchParams.get('hub.mode');
  const token = searchParams.get('hub.verify_token');
  const challenge = searchParams.get('hub.challenge');

  if (mode === 'subscribe' && token === VERIFY_TOKEN) {
    return new NextResponse(challenge, { status: 200 });
  }

  return NextResponse.json({ error: 'Forbidden' }, { status: 403 });
}

// POST - Receive delivery status updates from Meta
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();

    const entries = body.entry || [];
    for (const entry of entries) {
      const changes = entry.changes || [];
      for (const change of changes) {
        const statuses = change.value?.statuses || [];
        for (const status of statuses) {
          const waMessageId = status.id;
          const statusValue = status.status;
          const timestamp = status.timestamp;

          const updateData: Record<string, any> = {
            updated_at: new Date().toISOString(),
          };

          if (statusValue === 'sent') {
            updateData.status = 'sent';
            updateData.sent_at = new Date(parseInt(timestamp) * 1000).toISOString();
          } else if (statusValue === 'delivered') {
            updateData.status = 'delivered';
            updateData.delivered_at = new Date(parseInt(timestamp) * 1000).toISOString();
          } else if (statusValue === 'read') {
            updateData.status = 'read';
            updateData.read_at = new Date(parseInt(timestamp) * 1000).toISOString();
          } else if (statusValue === 'failed') {
            updateData.status = 'failed';
            updateData.error_message = status.errors?.[0]?.message || 'Delivery failed';
            updateData.error_code = status.errors?.[0]?.code?.toString();
          }

          if (waMessageId) {
            await supabase
              .from('flatrix_whatsapp_logs')
              .update(updateData)
              .eq('wa_message_id', waMessageId);
          }
        }
      }
    }

    // Always return 200 to Meta to acknowledge receipt
    return NextResponse.json({ success: true }, { status: 200 });
  } catch (error: any) {
    console.error('[WHATSAPP WEBHOOK] Error:', error);
    return NextResponse.json({ success: true }, { status: 200 });
  }
}
