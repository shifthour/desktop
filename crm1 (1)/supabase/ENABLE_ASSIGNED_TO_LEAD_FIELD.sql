-- =====================================================
-- ENABLE ASSIGNED TO FIELD FOR LEADS
-- =====================================================
-- This script updates the lead_field_configurations table
-- to enable the 'assigned_to' field and change it to a
-- select_dependent type so it fetches users dynamically
-- =====================================================

-- Update assigned_to field for all companies
UPDATE lead_field_configurations
SET
    is_enabled = true,
    field_type = 'select_dependent',
    field_label = 'Assigned To',
    placeholder = 'Select user to assign',
    help_text = 'Select a user from your company to assign this lead to',
    display_order = 14
WHERE field_name = 'assigned_to';

-- Verify the update
SELECT
    field_name,
    field_label,
    field_type,
    is_enabled,
    is_mandatory,
    display_order,
    COUNT(*) OVER (PARTITION BY field_name) as companies_updated
FROM lead_field_configurations
WHERE field_name = 'assigned_to'
LIMIT 1;

SELECT '✅ ASSIGNED TO FIELD ENABLED SUCCESSFULLY!' AS status;
