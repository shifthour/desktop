-- SQL script to add 'registered_date' column to flatrix_leads table
-- Run this in Supabase SQL Editor (FIXED VERSION)

-- Add the registered_date column as timestamp with timezone
ALTER TABLE flatrix_leads 
ADD COLUMN IF NOT EXISTS registered_date TIMESTAMP WITH TIME ZONE;

-- Add a comment to document the column
COMMENT ON COLUMN flatrix_leads.registered_date IS 'Date when the lead was marked as registered (Yes)';

-- Update existing records where registered = true to set registered_date to current timestamp
-- (This is a one-time migration for existing data)
UPDATE flatrix_leads 
SET registered_date = updated_at 
WHERE registered = true AND registered_date IS NULL;

-- Check the result
SELECT 
  column_name, 
  data_type, 
  is_nullable, 
  column_default 
FROM information_schema.columns 
WHERE table_name = 'flatrix_leads' 
  AND column_name = 'registered_date';

-- First, let's check what columns actually exist in the table
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = 'flatrix_leads' 
ORDER BY ordinal_position;

-- Sample query to verify the data (using only columns that definitely exist)
SELECT id, name, registered, registered_date 
FROM flatrix_leads 
WHERE registered = true 
LIMIT 5;