-- SQL script to add 'registered' column to flatrix_leads table
-- Run this in Supabase SQL Editor

-- Add the registered column as boolean with default value false
ALTER TABLE flatrix_leads 
ADD COLUMN IF NOT EXISTS registered BOOLEAN DEFAULT false;

-- Add a comment to document the column
COMMENT ON COLUMN flatrix_leads.registered IS 'Indicates if the lead is registered (Yes/No)';

-- Optional: Update existing records to have a default value
-- UPDATE flatrix_leads SET registered = false WHERE registered IS NULL;

-- Check the result
SELECT 
  column_name, 
  data_type, 
  is_nullable, 
  column_default 
FROM information_schema.columns 
WHERE table_name = 'flatrix_leads' 
  AND column_name = 'registered';