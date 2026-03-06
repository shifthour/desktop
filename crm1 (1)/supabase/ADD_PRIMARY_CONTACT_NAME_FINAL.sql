-- Step 1: Check existing columns in accounts table
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'accounts'
ORDER BY ordinal_position;

-- Step 2: Add primary_contact_name column to accounts table
ALTER TABLE accounts
ADD COLUMN IF NOT EXISTS primary_contact_name VARCHAR(255);

-- Step 3: Verify the column was added
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'accounts' AND column_name = 'primary_contact_name';

-- Step 4: Check if field configuration exists for primary_contact_name
SELECT id, field_name, field_label, is_mandatory, is_enabled, field_section
FROM account_field_configurations
WHERE field_name = 'primary_contact_name';

-- Step 5: Insert field configuration for all companies
-- This will add the field configuration if it doesn't exist
INSERT INTO account_field_configurations (
  field_name,
  field_label,
  field_type,
  is_mandatory,
  is_enabled,
  field_section,
  display_order,
  placeholder,
  help_text,
  company_id
)
SELECT
  'primary_contact_name',
  'Primary Contact Name',
  'text',
  true,
  true,
  'contact',
  5,
  'Enter primary contact name',
  'Main contact person for this account',
  c.id
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM account_field_configurations
  WHERE field_name = 'primary_contact_name' AND company_id = c.id
);

-- Step 6: Update existing field configurations to make it mandatory
UPDATE account_field_configurations
SET is_mandatory = true, is_enabled = true
WHERE field_name = 'primary_contact_name';

-- Step 7: Verify the final configuration
SELECT
  field_name,
  field_label,
  is_mandatory,
  is_enabled,
  field_section,
  display_order,
  company_id
FROM account_field_configurations
WHERE field_name = 'primary_contact_name';

-- Step 8: Check current accounts data
SELECT
  id,
  account_name,
  primary_contact_name
FROM accounts
ORDER BY created_at DESC
LIMIT 10;
