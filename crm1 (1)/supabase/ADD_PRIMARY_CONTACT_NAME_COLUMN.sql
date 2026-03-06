-- Step 1: Add primary_contact_name column to accounts table
ALTER TABLE accounts
ADD COLUMN IF NOT EXISTS primary_contact_name VARCHAR(255);

-- Step 2: Populate existing records with contact_name if available
UPDATE accounts
SET primary_contact_name = contact_name
WHERE primary_contact_name IS NULL AND contact_name IS NOT NULL;

-- Step 3: Verify the column was added
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'accounts' AND column_name = 'primary_contact_name';

-- Step 4: Check if field configuration exists for primary_contact_name
SELECT id, field_name, field_label, is_mandatory, is_enabled, field_section
FROM account_field_configurations
WHERE field_name = 'primary_contact_name';

-- Step 5: If the field configuration doesn't exist, insert it
-- (If it already exists, this will be skipped due to unique constraint)
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
  company_id
FROM companies
LIMIT 1
ON CONFLICT (field_name, company_id)
DO UPDATE SET
  is_mandatory = true,
  is_enabled = true;

-- Step 6: If you have multiple companies, run this for all companies
-- Update for all companies if needed
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

-- Step 7: Update existing field configurations to make it mandatory
UPDATE account_field_configurations
SET is_mandatory = true, is_enabled = true
WHERE field_name = 'primary_contact_name';

-- Step 8: Verify the final state
SELECT
  a.id,
  a.account_name,
  a.primary_contact_name,
  a.contact_name
FROM accounts a
ORDER BY a.created_at DESC
LIMIT 10;

-- Step 9: Check field configuration
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
