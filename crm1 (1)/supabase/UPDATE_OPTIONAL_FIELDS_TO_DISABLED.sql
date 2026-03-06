-- =====================================================
-- UPDATE OPTIONAL FIELDS TO DISABLED BY DEFAULT
-- =====================================================
-- This script updates existing field configurations to set
-- all optional fields to disabled by default
-- =====================================================

-- Update Account Field Configurations
-- Set all optional (non-mandatory) fields to disabled
UPDATE account_field_configurations
SET is_enabled = false,
    updated_at = NOW()
WHERE is_mandatory = false;

-- Update Contact Field Configurations
-- Set all optional (non-mandatory) fields to disabled
UPDATE contact_field_configurations
SET is_enabled = false,
    updated_at = NOW()
WHERE is_mandatory = false;

-- Verification queries
SELECT '✅ Account Fields Updated' AS status;
SELECT
  'Total optional account fields: ' || COUNT(*)::TEXT || ' | Disabled: ' || SUM(CASE WHEN is_enabled = false THEN 1 ELSE 0 END)::TEXT AS summary
FROM account_field_configurations
WHERE is_mandatory = false;

SELECT '✅ Contact Fields Updated' AS status;
SELECT
  'Total optional contact fields: ' || COUNT(*)::TEXT || ' | Disabled: ' || SUM(CASE WHEN is_enabled = false THEN 1 ELSE 0 END)::TEXT AS summary
FROM contact_field_configurations
WHERE is_mandatory = false;

SELECT '✅ UPDATE COMPLETE - All optional fields are now disabled by default!' AS status;
