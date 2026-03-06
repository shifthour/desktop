-- SQL script to update 'registered' column from boolean to text type
-- to support three values: 'Yes', 'No', 'Existing Lead'
-- Run this in Supabase SQL Editor

-- Step 1: Add a new temporary text column
ALTER TABLE flatrix_leads 
ADD COLUMN IF NOT EXISTS registered_status TEXT;

-- Step 2: Migrate existing boolean data to text values
UPDATE flatrix_leads 
SET registered_status = CASE 
  WHEN registered = true THEN 'Yes'
  WHEN registered = false THEN 'No'
  ELSE 'No'
END;

-- Step 3: Drop the old boolean column
ALTER TABLE flatrix_leads 
DROP COLUMN IF EXISTS registered;

-- Step 4: Rename the new column to 'registered'
ALTER TABLE flatrix_leads 
RENAME COLUMN registered_status TO registered;

-- Step 5: Add a CHECK constraint to ensure only valid values
ALTER TABLE flatrix_leads 
ADD CONSTRAINT registered_valid_values 
CHECK (registered IN ('Yes', 'No', 'Existing Lead'));

-- Step 6: Set default value
ALTER TABLE flatrix_leads 
ALTER COLUMN registered SET DEFAULT 'No';

-- Step 7: Update the comment
COMMENT ON COLUMN flatrix_leads.registered IS 'Registration status: Yes, No, or Existing Lead';

-- Step 8: Verify the changes
SELECT 
  column_name, 
  data_type, 
  is_nullable, 
  column_default 
FROM information_schema.columns 
WHERE table_name = 'flatrix_leads' 
  AND column_name = 'registered';

-- Step 9: Check some sample data
SELECT id, name, registered, registered_date 
FROM flatrix_leads 
LIMIT 10;

-- Step 10: Show the distribution of values
SELECT registered, COUNT(*) as count 
FROM flatrix_leads 
GROUP BY registered;