-- Alter next_followup_date column from DATE to TIMESTAMPTZ to support time
-- This is needed because we want to store specific times (e.g., 2:30 PM) not just dates

ALTER TABLE flatrix_leads
ALTER COLUMN next_followup_date TYPE TIMESTAMPTZ
USING next_followup_date::TIMESTAMPTZ;

-- Update the comment
COMMENT ON COLUMN flatrix_leads.next_followup_date IS 'Next followup date and time for this lead';

-- The existing indexes will continue to work with TIMESTAMPTZ
