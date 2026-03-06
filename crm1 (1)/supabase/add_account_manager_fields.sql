-- =====================================================
-- ADD ACCOUNT MANAGER AND CONTACT FIELDS TO ACCOUNTS
-- =====================================================
-- This script adds new optional fields for account managers and key contacts
-- =====================================================

-- Step 1: Add columns to accounts table
ALTER TABLE accounts
ADD COLUMN IF NOT EXISTS account_manager_name VARCHAR(150),
ADD COLUMN IF NOT EXISTS sales_owner_name VARCHAR(150),
ADD COLUMN IF NOT EXISTS technical_contact_name VARCHAR(150),
ADD COLUMN IF NOT EXISTS billing_contact_name VARCHAR(150),
ADD COLUMN IF NOT EXISTS acc_budget_authority_name VARCHAR(150);

-- Step 2: Add field configurations for all companies
-- Loop through all companies and add the new field configurations

DO $$
DECLARE
    company_record RECORD;
    max_order INTEGER;
BEGIN
    FOR company_record IN SELECT id FROM companies LOOP
        -- Get the maximum display_order for business_details section
        SELECT COALESCE(MAX(display_order), 54) INTO max_order
        FROM account_field_configurations
        WHERE company_id = company_record.id AND field_section = 'business_details';

        -- Add Account Manager Name
        INSERT INTO account_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES (
            company_record.id,
            'account_manager_name',
            'Account Manager Name',
            'text',
            false,
            false,
            'business_details',
            max_order + 1,
            'Enter account manager name',
            'Primary account manager for this account'
        ) ON CONFLICT (company_id, field_name) DO NOTHING;

        -- Add Sales Owner Name
        INSERT INTO account_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES (
            company_record.id,
            'sales_owner_name',
            'Sales Owner Name',
            'text',
            false,
            false,
            'business_details',
            max_order + 2,
            'Enter sales owner name',
            'Primary sales owner for this account'
        ) ON CONFLICT (company_id, field_name) DO NOTHING;

        -- Add Technical Contact Name
        INSERT INTO account_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES (
            company_record.id,
            'technical_contact_name',
            'Technical Contact Name',
            'text',
            false,
            false,
            'business_details',
            max_order + 3,
            'Enter technical contact name',
            'Primary technical contact for this account'
        ) ON CONFLICT (company_id, field_name) DO NOTHING;

        -- Add Billing Contact Name
        INSERT INTO account_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES (
            company_record.id,
            'billing_contact_name',
            'Billing Contact Name',
            'text',
            false,
            false,
            'business_details',
            max_order + 4,
            'Enter billing contact name',
            'Primary billing contact for this account'
        ) ON CONFLICT (company_id, field_name) DO NOTHING;

        -- Add Budget Authority Name
        INSERT INTO account_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES (
            company_record.id,
            'acc_budget_authority_name',
            'Budget Authority Name',
            'text',
            false,
            false,
            'business_details',
            max_order + 5,
            'Enter budget authority name',
            'Person with budget authority for this account'
        ) ON CONFLICT (company_id, field_name) DO NOTHING;

    END LOOP;
END $$;

-- Step 3: Verification
SELECT '✅ Columns added to accounts table' AS status;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'accounts'
AND column_name IN (
    'account_manager_name',
    'sales_owner_name',
    'technical_contact_name',
    'billing_contact_name',
    'acc_budget_authority_name'
)
ORDER BY column_name;

SELECT '✅ Field configurations added' AS status;

SELECT
    field_name,
    field_label,
    field_section,
    is_mandatory,
    is_enabled,
    COUNT(*) as company_count
FROM account_field_configurations
WHERE field_name IN (
    'account_manager_name',
    'sales_owner_name',
    'technical_contact_name',
    'billing_contact_name',
    'acc_budget_authority_name'
)
GROUP BY field_name, field_label, field_section, is_mandatory, is_enabled
ORDER BY field_name;

SELECT '✅ MIGRATION COMPLETE - 5 new account manager/contact fields added!' AS status;
