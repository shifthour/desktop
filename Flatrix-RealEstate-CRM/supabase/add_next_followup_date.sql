-- Add next_followup_date column to flatrix_leads table

ALTER TABLE flatrix_leads 
ADD COLUMN IF NOT EXISTS next_followup_date DATE;

-- Add index for better performance on followup date queries
CREATE INDEX IF NOT EXISTS idx_flatrix_leads_next_followup ON flatrix_leads(next_followup_date);

-- Add index for overdue followups (common query)
CREATE INDEX IF NOT EXISTS idx_flatrix_leads_overdue_followup ON flatrix_leads(next_followup_date) 
WHERE next_followup_date < CURRENT_DATE;

-- Optional: Add a constraint to ensure followup date is not in the past (uncomment if needed)
-- ALTER TABLE flatrix_leads ADD CONSTRAINT check_future_followup 
-- CHECK (next_followup_date IS NULL OR next_followup_date >= CURRENT_DATE);