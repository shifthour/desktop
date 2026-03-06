-- WhatsApp Cloud API Integration Tables
-- Run this migration in Supabase SQL Editor

-- ============================================
-- Table 1: WhatsApp Templates (per-project)
-- ============================================
CREATE TABLE IF NOT EXISTS flatrix_whatsapp_templates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID REFERENCES flatrix_projects(id) ON DELETE SET NULL,
    project_name_pattern TEXT NOT NULL,
    greeting_template_name TEXT NOT NULL,
    greeting_body TEXT,
    brochure_template_name TEXT,
    brochure_url TEXT,
    video_template_name TEXT,
    video_url TEXT,
    template_language TEXT NOT NULL DEFAULT 'en',
    is_active BOOLEAN DEFAULT true,
    is_default BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wa_templates_project ON flatrix_whatsapp_templates(project_name_pattern);
CREATE INDEX IF NOT EXISTS idx_wa_templates_active ON flatrix_whatsapp_templates(is_active);
CREATE INDEX IF NOT EXISTS idx_wa_templates_default ON flatrix_whatsapp_templates(is_default) WHERE is_default = true;

-- ============================================
-- Table 2: WhatsApp Message Logs
-- ============================================
CREATE TABLE IF NOT EXISTS flatrix_whatsapp_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    lead_id UUID,
    lead_phone TEXT NOT NULL,
    lead_name TEXT,
    project_name TEXT,
    template_id UUID REFERENCES flatrix_whatsapp_templates(id) ON DELETE SET NULL,
    message_type TEXT NOT NULL DEFAULT 'greeting' CHECK (message_type IN ('greeting', 'brochure', 'video')),
    wa_template_name TEXT,
    wa_message_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'delivered', 'read', 'failed')),
    error_message TEXT,
    error_code TEXT,
    retry_count INTEGER DEFAULT 0,
    sent_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    read_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wa_logs_lead ON flatrix_whatsapp_logs(lead_id);
CREATE INDEX IF NOT EXISTS idx_wa_logs_status ON flatrix_whatsapp_logs(status);
CREATE INDEX IF NOT EXISTS idx_wa_logs_phone ON flatrix_whatsapp_logs(lead_phone);
CREATE INDEX IF NOT EXISTS idx_wa_logs_created ON flatrix_whatsapp_logs(created_at DESC);

-- ============================================
-- Table 3: WhatsApp Config (singleton)
-- ============================================
CREATE TABLE IF NOT EXISTS flatrix_whatsapp_config (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    phone_number_id TEXT,
    business_account_id TEXT,
    webhook_verify_token TEXT,
    is_enabled BOOLEAN DEFAULT false,
    api_version TEXT DEFAULT 'v18.0',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert default config row
INSERT INTO flatrix_whatsapp_config (id, is_enabled)
VALUES ('00000000-0000-0000-0000-000000000002', false)
ON CONFLICT (id) DO NOTHING;

-- ============================================
-- Pre-populate WhatsApp Templates
-- ============================================

-- Template 1: Anahata Project (matches any project containing "anahata")
INSERT INTO flatrix_whatsapp_templates (
    project_name_pattern,
    greeting_template_name,
    greeting_body,
    brochure_template_name,
    brochure_url,
    video_template_name,
    video_url,
    template_language,
    is_active,
    is_default
) VALUES (
    'Anahata',
    'anahata_greeting',
    E'Dear {{1}},\n\nThank you for your interest in Ishtika Anahata, located on Soukya road in Whitefield, a peaceful, well-connected community offering spacious homes with higher ceilings and quality finishes.\n\nHere''s a quick Video Tour to help you experience the project from wherever you are.\n\nRegards,\nAnahata Team',
    'anahata_brochure',
    'REPLACE_WITH_BROCHURE_URL',
    'anahata_video',
    'REPLACE_WITH_VIDEO_URL',
    'en',
    true,
    false
);

-- Template 2: Default (for all non-Anahata leads)
INSERT INTO flatrix_whatsapp_templates (
    project_name_pattern,
    greeting_template_name,
    greeting_body,
    brochure_template_name,
    brochure_url,
    video_template_name,
    video_url,
    template_language,
    is_active,
    is_default
) VALUES (
    'Default',
    'default_greeting',
    E'Dear {{1}},\n\nWe got to know that you''re exploring options for a new flat in Whitefield, Bangalore.\nWe''d love to introduce you to Ishtika Anahata, our premium residential project located on Soukya Road, Whitefield — a peaceful, well-connected community offering spacious homes with higher ceilings and quality finishes.\n\nHere''s a quick Video Tour to help you experience the project from wherever you are.\n\nRegards,\nAnahata Team',
    'default_brochure',
    'REPLACE_WITH_BROCHURE_URL',
    'default_video',
    'REPLACE_WITH_VIDEO_URL',
    'en',
    true,
    true
);
