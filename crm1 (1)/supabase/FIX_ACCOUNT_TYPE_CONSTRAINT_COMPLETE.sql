-- =====================================================
-- Fix Account Type Constraint - COMPLETE VERSION
-- This drops ALL possible constraints and recreates it
-- =====================================================

-- Step 1: Drop all possible constraint names
ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_account_type_check;
ALTER TABLE accounts DROP CONSTRAINT IF EXISTS chk_account_type;
ALTER TABLE accounts DROP CONSTRAINT IF EXISTS check_account_type;

-- Step 2: Add the new constraint
ALTER TABLE accounts ADD CONSTRAINT accounts_account_type_check
CHECK (account_type IN (
    'Customer',
    'Manufacturer',
    'Distributor',
    'End_Customer',
    'Research_Institution',
    'Hospital',
    'University',
    'Government_Department'
));

-- Step 3: Update the field configuration
UPDATE account_field_configurations
SET field_options = '["Customer", "Manufacturer", "Distributor", "End_Customer", "Research_Institution", "Hospital", "University", "Government_Department"]'::jsonb,
    help_text = 'Primary business type'
WHERE field_name = 'account_type';

-- Step 4: Verify the constraint exists
SELECT
    conname as constraint_name,
    pg_get_constraintdef(oid) as constraint_definition
FROM pg_constraint
WHERE conname LIKE '%account_type%';

-- =====================================================
-- DONE! Run this and verify you see the new constraint
-- =====================================================
