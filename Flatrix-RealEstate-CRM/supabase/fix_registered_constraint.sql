-- Fix CHECK constraint for registered column to include "Accompany client"
-- This resolves the error: new row violates check constraint "registered_valid_values"

-- Step 1: Check current constraint definition
SELECT
    con.conname AS constraint_name,
    pg_get_constraintdef(con.oid) AS constraint_definition
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
WHERE rel.relname = 'flatrix_leads'
  AND con.conname = 'registered_valid_values';

-- Step 2: Drop the old constraint
ALTER TABLE flatrix_leads
DROP CONSTRAINT IF EXISTS registered_valid_values;

-- Step 3: Add new constraint with "Accompany client" included
ALTER TABLE flatrix_leads
ADD CONSTRAINT registered_valid_values
CHECK (registered IN ('No', 'Yes', 'Existing Lead', 'Accompany client'));

-- Step 4: Verify the new constraint
SELECT
    con.conname AS constraint_name,
    pg_get_constraintdef(con.oid) AS constraint_definition
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
WHERE rel.relname = 'flatrix_leads'
  AND con.conname = 'registered_valid_values';

-- Step 5: Test that it works by checking current values
SELECT registered, COUNT(*) as count
FROM flatrix_leads
GROUP BY registered
ORDER BY count DESC;

-- Success! "Accompany client" is now a valid value for the registered column
