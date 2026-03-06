import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

const CONFIG_ID = '00000000-0000-0000-0000-000000000002';

// GET - Get current WhatsApp config
export async function GET() {
  try {
    const { data, error } = await supabase
      .from('flatrix_whatsapp_config')
      .select('*')
      .eq('id', CONFIG_ID)
      .single();

    if (error) throw error;

    // Check if env vars are configured
    const envConfigured = !!(
      process.env.WHATSAPP_PHONE_NUMBER_ID &&
      process.env.WHATSAPP_ACCESS_TOKEN
    );

    return NextResponse.json({
      success: true,
      config: {
        ...data,
        env_configured: envConfigured,
        phone_number_id_set: !!process.env.WHATSAPP_PHONE_NUMBER_ID,
        access_token_set: !!process.env.WHATSAPP_ACCESS_TOKEN,
      },
    });
  } catch (error: any) {
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}

// PUT - Update WhatsApp config (toggle on/off, etc.)
export async function PUT(request: NextRequest) {
  try {
    const body = await request.json();

    const { is_enabled, phone_number_id, business_account_id, webhook_verify_token, api_version } = body;

    const updateData: Record<string, any> = {
      updated_at: new Date().toISOString(),
    };

    if (typeof is_enabled === 'boolean') updateData.is_enabled = is_enabled;
    if (phone_number_id !== undefined) updateData.phone_number_id = phone_number_id;
    if (business_account_id !== undefined) updateData.business_account_id = business_account_id;
    if (webhook_verify_token !== undefined) updateData.webhook_verify_token = webhook_verify_token;
    if (api_version !== undefined) updateData.api_version = api_version;

    const { data, error } = await supabase
      .from('flatrix_whatsapp_config')
      .update(updateData)
      .eq('id', CONFIG_ID)
      .select()
      .single();

    if (error) throw error;

    return NextResponse.json({ success: true, config: data });
  } catch (error: any) {
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}
