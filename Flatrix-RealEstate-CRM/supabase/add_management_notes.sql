-- Add management_notes column to flatrix_leads table
ALTER TABLE flatrix_leads 
ADD COLUMN IF NOT EXISTS management_notes TEXT;

-- Add management_notes column to flatrix_deals table
ALTER TABLE flatrix_deals 
ADD COLUMN IF NOT EXISTS management_notes TEXT;

-- Add comment to explain the purpose
COMMENT ON COLUMN flatrix_leads.management_notes IS 'Management notes for guiding CRM users on next steps. Format: [timestamp]: note content';
COMMENT ON COLUMN flatrix_deals.management_notes IS 'Management notes for guiding CRM users on next steps. Format: [timestamp]: note content';