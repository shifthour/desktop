-- Make primary_contact_name a mandatory field in account creation
-- This will update the field configuration to make it required

-- Step 1: Update the field configuration to make primary_contact_name mandatory
UPDATE account_field_configurations
SET
  is_mandatory = true,
  is_enabled = true
WHERE field_name = 'primary_contact_name';

-- Step 2: Verify the update
SELECT
  field_name,
  field_label,
  is_mandatory,
  is_enabled,
  field_section,
  display_order
FROM account_field_configurations
WHERE field_name = 'primary_contact_name';

-- Step 3: Check if any existing accounts have NULL primary_contact_name
SELECT
  id,
  account_name,
  primary_contact_name
FROM accounts
WHERE primary_contact_name IS NULL OR primary_contact_name = ''
ORDER BY created_at DESC
LIMIT 10;

-- Optional: If you want to set a default value for existing NULL records
-- Uncomment the lines below if needed:
-- UPDATE accounts
-- SET primary_contact_name = 'Not Provided'
-- WHERE primary_contact_name IS NULL OR primary_contact_name = '';
