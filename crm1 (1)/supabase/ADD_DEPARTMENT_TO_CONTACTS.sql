-- =====================================================
-- ADD DEPARTMENT AS MANDATORY FIELD IN CONTACTS
-- =====================================================
-- This script adds department field to contacts table
-- and makes it mandatory in the field configuration
-- =====================================================

-- Step 1: Add department column to contacts table if it doesn't exist
ALTER TABLE contacts
ADD COLUMN IF NOT EXISTS department VARCHAR(150);

-- Step 2: Add department field configuration for all companies
-- Position it right after company_name (display_order = 2)
INSERT INTO contact_field_configurations (
  company_id,
  field_name,
  field_label,
  field_type,
  is_mandatory,
  is_enabled,
  field_section,
  display_order,
  placeholder,
  help_text
)
SELECT
  c.id,
  'department',
  'Department',
  'text',
  true,
  true,
  'basic_info',
  2,
  'Enter department',
  'Department of the contact'
FROM companies c
ON CONFLICT (company_id, field_name) DO UPDATE
SET
  is_mandatory = true,
  is_enabled = true,
  display_order = 2,
  updated_at = NOW();

-- Step 3: Update display_order for other fields to make room
-- Move first_name from 2 to 3
UPDATE contact_field_configurations
SET display_order = 3, updated_at = NOW()
WHERE field_name = 'first_name';

-- Move last_name from 3 to 4
UPDATE contact_field_configurations
SET display_order = 4, updated_at = NOW()
WHERE field_name = 'last_name';

-- Move email_primary from 4 to 5
UPDATE contact_field_configurations
SET display_order = 5, updated_at = NOW()
WHERE field_name = 'email_primary';

-- Move phone_mobile from 5 to 6
UPDATE contact_field_configurations
SET display_order = 6, updated_at = NOW()
WHERE field_name = 'phone_mobile';

-- Adjust all other fields that come after
UPDATE contact_field_configurations
SET display_order = display_order + 1, updated_at = NOW()
WHERE display_order >= 6 AND field_name NOT IN ('department', 'first_name', 'last_name', 'email_primary', 'phone_mobile');

-- Step 4: Create index for department
CREATE INDEX IF NOT EXISTS idx_contacts_department ON contacts(department);

-- Verification
SELECT '✅ Checking department field configuration:' AS info;

SELECT
  field_name,
  field_label,
  is_mandatory,
  is_enabled,
  display_order
FROM contact_field_configurations
WHERE field_name IN ('company_name', 'department', 'first_name', 'last_name', 'email_primary', 'phone_mobile')
ORDER BY display_order;

SELECT '✅ Department field added successfully!' AS status;
