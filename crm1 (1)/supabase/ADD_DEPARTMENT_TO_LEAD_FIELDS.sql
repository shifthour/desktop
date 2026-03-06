-- =====================================================
-- ADD DEPARTMENT FIELD TO LEAD FIELD CONFIGURATIONS
-- =====================================================
-- This script adds department field configuration for leads
-- and positions it right after the contact field
-- =====================================================

-- Add department field configuration for all companies
-- Position it after contact field
INSERT INTO lead_field_configurations (
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
  false,  -- Not mandatory as it auto-populates from contact
  true,
  'basic_info',
  4,  -- Right after contact (which should be at 3)
  'Auto-populated from contact',
  'Department of the contact (auto-populated)'
FROM companies c
ON CONFLICT (company_id, field_name) DO UPDATE
SET
  is_mandatory = false,
  is_enabled = true,
  field_section = 'basic_info',
  display_order = 4,
  placeholder = 'Auto-populated from contact',
  help_text = 'Department of the contact (auto-populated)',
  updated_at = NOW();

-- Adjust display_order for fields that come after department
-- Move first_name from 4 to 5
UPDATE lead_field_configurations
SET display_order = 5, updated_at = NOW()
WHERE field_name = 'first_name' AND display_order = 4;

-- Move last_name from 5 to 6
UPDATE lead_field_configurations
SET display_order = 6, updated_at = NOW()
WHERE field_name = 'last_name' AND display_order = 5;

-- Move other fields accordingly
UPDATE lead_field_configurations
SET display_order = display_order + 1, updated_at = NOW()
WHERE display_order >= 4
  AND field_name NOT IN ('department', 'account', 'contact', 'lead_title');

-- Verification
SELECT '✅ Checking department field in lead configurations:' AS info;

SELECT
  field_name,
  field_label,
  is_mandatory,
  is_enabled,
  field_section,
  display_order
FROM lead_field_configurations
WHERE field_name IN ('lead_title', 'account', 'contact', 'department', 'first_name', 'last_name')
ORDER BY display_order;

SELECT '✅ Department field added to lead configurations!' AS status;
