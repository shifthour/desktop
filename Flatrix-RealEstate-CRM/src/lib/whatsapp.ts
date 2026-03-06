import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

const CONFIG_ID = '00000000-0000-0000-0000-000000000002';

interface LeadData {
  id: string;
  name: string;
  phone: string;
  project_name: string | null;
}

interface SendResult {
  success: boolean;
  messageId?: string;
  error?: string;
  errorCode?: string;
}

interface WhatsAppTemplate {
  id: string;
  greeting_template_name: string;
  greeting_body: string | null;
  brochure_template_name: string | null;
  brochure_url: string | null;
  video_template_name: string | null;
  video_url: string | null;
  template_language: string;
  project_name_pattern: string;
}

function getConfig() {
  return {
    phoneNumberId: process.env.WHATSAPP_PHONE_NUMBER_ID || '',
    accessToken: process.env.WHATSAPP_ACCESS_TOKEN || '',
    apiVersion: process.env.WHATSAPP_API_VERSION || 'v18.0',
  };
}

function formatPhone(phone: string): string {
  let cleaned = phone.replace(/[\s\-\(\)\+]/g, '');
  // If 10-digit Indian number, prepend 91
  if (cleaned.length === 10 && /^[6-9]/.test(cleaned)) {
    cleaned = '91' + cleaned;
  }
  return cleaned;
}

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function findTemplate(projectName: string | null): Promise<WhatsAppTemplate | null> {
  // Fetch all active templates and match in code
  // (SQL ILIKE can't check if lead's project_name contains pattern)
  const { data: templates } = await supabase
    .from('flatrix_whatsapp_templates')
    .select('*')
    .eq('is_active', true);

  if (!templates || templates.length === 0) return null;

  // Try to find a matching non-default template by checking if
  // the lead's project name contains the template's pattern
  if (projectName && projectName !== 'Not Specified') {
    const lowerProject = projectName.toLowerCase();
    const match = templates.find(
      t => !t.is_default && lowerProject.includes(t.project_name_pattern.toLowerCase())
    );
    if (match) return match;
  }

  // Fallback to default template
  return templates.find(t => t.is_default) || null;
}

async function sendTemplateMessage(
  phone: string,
  templateName: string,
  language: string,
  components: any[]
): Promise<SendResult> {
  const config = getConfig();

  if (!config.phoneNumberId || !config.accessToken) {
    return { success: false, error: 'WhatsApp API not configured' };
  }

  const payload = {
    messaging_product: 'whatsapp',
    to: phone,
    type: 'template',
    template: {
      name: templateName,
      language: { code: language },
      components,
    },
  };

  const apiUrl = `https://graph.facebook.com/${config.apiVersion}/${config.phoneNumberId}/messages`;

  try {
    const response = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${config.accessToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    });

    const result = await response.json();

    if (response.ok && result.messages?.[0]?.id) {
      return { success: true, messageId: result.messages[0].id };
    }

    return {
      success: false,
      error: result.error?.message || 'Unknown API error',
      errorCode: result.error?.code?.toString(),
    };
  } catch (err: any) {
    return { success: false, error: err.message || 'Network error' };
  }
}

async function logMessage(
  lead: LeadData,
  messageType: 'greeting' | 'brochure' | 'video',
  templateId: string | null,
  templateName: string | null,
  result: SendResult
) {
  await supabase.from('flatrix_whatsapp_logs').insert({
    lead_id: lead.id,
    lead_phone: lead.phone,
    lead_name: lead.name,
    project_name: lead.project_name,
    template_id: templateId,
    message_type: messageType,
    wa_template_name: templateName,
    wa_message_id: result.messageId || null,
    status: result.success ? 'sent' : 'failed',
    error_message: result.error || null,
    error_code: result.errorCode || null,
    sent_at: result.success ? new Date().toISOString() : null,
    created_at: new Date().toISOString(),
  });
}

/**
 * Main entry point - sends 3 WhatsApp messages to a new lead:
 * 1. Greeting text message
 * 2. Brochure PDF attachment
 * 3. Video attachment
 *
 * This function is fire-and-forget. Callers should NOT await it.
 */
export async function sendWhatsAppToNewLead(lead: LeadData): Promise<void> {
  // Check if enabled
  const { data: waConfig } = await supabase
    .from('flatrix_whatsapp_config')
    .select('is_enabled')
    .eq('id', CONFIG_ID)
    .single();

  if (!waConfig?.is_enabled) {
    console.log('[WHATSAPP] Automation is disabled, skipping');
    return;
  }

  // Find matching template
  const template = await findTemplate(lead.project_name);
  if (!template) {
    console.log('[WHATSAPP] No template found for project:', lead.project_name);
    return;
  }

  const phone = formatPhone(lead.phone);
  const lang = template.template_language || 'en';

  // 1. Send greeting message
  const greetingComponents = [
    {
      type: 'body',
      parameters: [{ type: 'text', text: lead.name || 'there' }],
    },
  ];

  const greetingResult = await sendTemplateMessage(
    phone,
    template.greeting_template_name,
    lang,
    greetingComponents
  );

  await logMessage(lead, 'greeting', template.id, template.greeting_template_name, greetingResult);
  console.log(`[WHATSAPP] Greeting ${greetingResult.success ? 'sent' : 'failed'} for lead ${lead.id}`);

  // 2. Send brochure PDF (if configured)
  if (template.brochure_template_name && template.brochure_url) {
    await delay(2000);

    const brochureComponents = [
      {
        type: 'header',
        parameters: [
          {
            type: 'document',
            document: {
              link: template.brochure_url,
              filename: `${template.project_name_pattern}_Brochure.pdf`,
            },
          },
        ],
      },
    ];

    const brochureResult = await sendTemplateMessage(
      phone,
      template.brochure_template_name,
      lang,
      brochureComponents
    );

    await logMessage(lead, 'brochure', template.id, template.brochure_template_name, brochureResult);
    console.log(`[WHATSAPP] Brochure ${brochureResult.success ? 'sent' : 'failed'} for lead ${lead.id}`);
  }

  // 3. Send video (if configured)
  if (template.video_template_name && template.video_url) {
    await delay(2000);

    const videoComponents = [
      {
        type: 'header',
        parameters: [
          {
            type: 'video',
            video: { link: template.video_url },
          },
        ],
      },
    ];

    const videoResult = await sendTemplateMessage(
      phone,
      template.video_template_name,
      lang,
      videoComponents
    );

    await logMessage(lead, 'video', template.id, template.video_template_name, videoResult);
    console.log(`[WHATSAPP] Video ${videoResult.success ? 'sent' : 'failed'} for lead ${lead.id}`);
  }
}
