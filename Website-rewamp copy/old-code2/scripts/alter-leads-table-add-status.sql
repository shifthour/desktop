-- Add status_of_save column to flatrix_leads table
ALTER TABLE flatrix_leads ADD COLUMN IF NOT EXISTS status_of_save VARCHAR(50) DEFAULT 'before generating OTP';

-- Create index for better query performance on status
CREATE INDEX IF NOT EXISTS idx_flatrix_leads_status_of_save ON flatrix_leads(status_of_save);

-- Update existing records to have default status
UPDATE flatrix_leads SET status_of_save = 'after generating OTP' WHERE status_of_save IS NULL;
