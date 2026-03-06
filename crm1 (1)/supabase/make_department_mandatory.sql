-- =====================================================
-- MAKE DEPARTMENT FIELD MANDATORY IN LEADS
-- =====================================================
-- This script updates the department field configuration
-- to make it a mandatory field in the lead form
-- =====================================================

-- Update department field to be mandatory
UPDATE lead_field_configurations
SET
  is_mandatory = true,
  help_text = 'Department of the contact (required)',
  updated_at = NOW()
WHERE field_name = 'department';

-- Verification
SELECT '✅ Checking department field configuration:' AS info;

SELECT
  field_name,
  field_label,
  is_mandatory,
  is_enabled,
  field_section,
  display_order,
  placeholder,
  help_text
FROM lead_field_configurations
WHERE field_name = 'department';

SELECT '✅ Department field is now mandatory!' AS status;
