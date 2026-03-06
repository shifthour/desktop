-- Create table to track dismissed followup notifications
CREATE TABLE IF NOT EXISTS flatrix_dismissed_followup_notifications (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES flatrix_users(id) ON DELETE CASCADE,
    lead_id UUID NOT NULL REFERENCES flatrix_leads(id) ON DELETE CASCADE,
    followup_datetime TIMESTAMPTZ NOT NULL,
    dismissed_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    -- Unique constraint to prevent duplicate dismissals
    UNIQUE(user_id, lead_id, followup_datetime)
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_dismissed_notifications_user_lead
ON flatrix_dismissed_followup_notifications(user_id, lead_id, followup_datetime);

-- Add comment
COMMENT ON TABLE flatrix_dismissed_followup_notifications IS 'Tracks which followup notifications have been dismissed by which users';
