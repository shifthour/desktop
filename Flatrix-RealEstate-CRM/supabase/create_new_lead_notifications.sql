-- Create table to track dismissed new lead notifications
CREATE TABLE IF NOT EXISTS flatrix_dismissed_new_lead_notifications (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES flatrix_users(id) ON DELETE CASCADE,
    lead_id UUID NOT NULL REFERENCES flatrix_leads(id) ON DELETE CASCADE,
    dismissed_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    -- Unique constraint to prevent duplicate dismissals
    UNIQUE(user_id, lead_id)
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_dismissed_new_lead_notifications_user_lead
ON flatrix_dismissed_new_lead_notifications(user_id, lead_id);

-- Add comment
COMMENT ON TABLE flatrix_dismissed_new_lead_notifications IS 'Tracks which new lead notifications have been dismissed by which users';
