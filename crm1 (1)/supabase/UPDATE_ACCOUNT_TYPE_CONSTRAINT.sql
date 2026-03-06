-- =====================================================
-- Update Account Type Constraint
-- This script updates the account_type field constraint
-- =====================================================

-- Step 1: Drop the old constraint
ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_account_type_check;

-- Step 2: Add the new constraint with updated values
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

-- Step 3: Update the field configuration options for account_type
UPDATE account_field_configurations
SET field_options = '["Customer", "Manufacturer", "Distributor", "End_Customer", "Research_Institution", "Hospital", "University", "Government_Department"]'::jsonb,
    help_text = 'Primary business type'
WHERE field_name = 'account_type';

-- =====================================================
-- DONE! Account type options updated successfully
-- =====================================================
SELECT 'Success! Account type dropdown options have been updated.' as message;
