-- =====================================================
-- MAKE ASSIGNED_TO MANDATORY FOR LEADS
-- =====================================================
-- This script makes the assigned_to field mandatory in:
-- 1. lead_field_configurations table (for admin UI)
-- 2. leads table (database constraint)
-- =====================================================

-- Step 1: Update lead_field_configurations to make assigned_to mandatory and enabled
UPDATE lead_field_configurations
SET
    is_mandatory = true,
    is_enabled = true,
    field_type = 'select_dependent',
    field_label = 'Assigned To',
    placeholder = 'Select account to assign',
    help_text = 'Select an account to assign this lead to (Required)',
    display_order = 14
WHERE field_name = 'assigned_to';

-- Step 2: Check current state of assigned_to column in leads table
SELECT
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'leads'
  AND column_name = 'assigned_to';

-- Step 3: Update any NULL values in existing records to a default value
-- This prevents the NOT NULL constraint from failing
UPDATE leads
SET assigned_to = 'Unassigned'
WHERE assigned_to IS NULL OR assigned_to = '';

-- Step 4: Make assigned_to NOT NULL in the leads table
ALTER TABLE leads
ALTER COLUMN assigned_to SET NOT NULL;

-- Step 5: Verify the changes
SELECT
    field_name,
    field_label,
    field_type,
    is_enabled,
    is_mandatory,
    display_order,
    help_text
FROM lead_field_configurations
WHERE field_name = 'assigned_to';

-- Verify column constraint
SELECT
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'leads'
  AND column_name = 'assigned_to';

SELECT '✅ ASSIGNED_TO FIELD IS NOW MANDATORY!' AS status;
