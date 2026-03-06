-- =====================================================
-- UPDATE LEADS FIELD CONFIGURATIONS
-- =====================================================
-- This script updates lead field configurations to:
-- 1. Change 'account' and 'contact' to select_dependent type
-- 2. Split 'name' into 'first_name' and 'last_name'
-- 3. Add 'phone_number' as mandatory field
-- =====================================================

DO $$
DECLARE
    company_record RECORD;
BEGIN
    FOR company_record IN SELECT id FROM companies LOOP

        -- Update 'account' field to be a dropdown (select_dependent)
        UPDATE lead_field_configurations
        SET
            field_type = 'select_dependent',
            placeholder = 'Select account',
            help_text = 'Select an existing account from the dropdown',
            updated_at = NOW()
        WHERE company_id = company_record.id AND field_name = 'account';

        -- Update 'contact' field to be a dropdown (select_dependent)
        UPDATE lead_field_configurations
        SET
            field_type = 'select_dependent',
            placeholder = 'Select contact',
            help_text = 'Select an existing contact (filtered by account)',
            updated_at = NOW()
        WHERE company_id = company_record.id AND field_name = 'contact';

        -- Delete the old 'name' field if it exists
        DELETE FROM lead_field_configurations
        WHERE company_id = company_record.id AND field_name = 'name';

        -- Add 'first_name' field (mandatory)
        INSERT INTO lead_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES (
            company_record.id,
            'first_name',
            'First Name',
            'text',
            true,
            true,
            'basic_info',
            4,
            'Enter first name',
            'First name of the contact person'
        )
        ON CONFLICT (company_id, field_name) DO UPDATE
        SET
            field_label = 'First Name',
            field_type = 'text',
            is_mandatory = true,
            is_enabled = true,
            field_section = 'basic_info',
            display_order = 4,
            placeholder = 'Enter first name',
            help_text = 'First name of the contact person',
            updated_at = NOW();

        -- Add 'last_name' field (mandatory)
        INSERT INTO lead_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES (
            company_record.id,
            'last_name',
            'Last Name',
            'text',
            true,
            true,
            'basic_info',
            5,
            'Enter last name',
            'Last name of the contact person'
        )
        ON CONFLICT (company_id, field_name) DO UPDATE
        SET
            field_label = 'Last Name',
            field_type = 'text',
            is_mandatory = true,
            is_enabled = true,
            field_section = 'basic_info',
            display_order = 5,
            placeholder = 'Enter last name',
            help_text = 'Last name of the contact person',
            updated_at = NOW();

        -- Add 'phone_number' field (mandatory) in contact_info section
        INSERT INTO lead_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES (
            company_record.id,
            'phone_number',
            'Phone Number',
            'text',
            true,
            true,
            'contact_info',
            12,
            'Enter phone number',
            'Primary phone number of the contact'
        )
        ON CONFLICT (company_id, field_name) DO UPDATE
        SET
            field_label = 'Phone Number',
            field_type = 'text',
            is_mandatory = true,
            is_enabled = true,
            field_section = 'contact_info',
            display_order = 12,
            placeholder = 'Enter phone number',
            help_text = 'Primary phone number of the contact',
            updated_at = NOW();

        -- Update display_order for other mandatory fields to maintain sequence
        UPDATE lead_field_configurations
        SET display_order = 6, updated_at = NOW()
        WHERE company_id = company_record.id AND field_name = 'lead_source';

        UPDATE lead_field_configurations
        SET display_order = 7, updated_at = NOW()
        WHERE company_id = company_record.id AND field_name = 'lead_type';

        UPDATE lead_field_configurations
        SET display_order = 8, updated_at = NOW()
        WHERE company_id = company_record.id AND field_name = 'lead_status';

        UPDATE lead_field_configurations
        SET display_order = 9, updated_at = NOW()
        WHERE company_id = company_record.id AND field_name = 'lead_stage';

        UPDATE lead_field_configurations
        SET display_order = 10, updated_at = NOW()
        WHERE company_id = company_record.id AND field_name = 'nurture_level';

        UPDATE lead_field_configurations
        SET display_order = 11, updated_at = NOW()
        WHERE company_id = company_record.id AND field_name = 'priority_level';

        UPDATE lead_field_configurations
        SET display_order = 13, updated_at = NOW()
        WHERE company_id = company_record.id AND field_name = 'email_address';

        UPDATE lead_field_configurations
        SET display_order = 14, updated_at = NOW()
        WHERE company_id = company_record.id AND field_name = 'consent_status';

    END LOOP;
END $$;

-- Verification
SELECT '✅ Lead field configurations updated' AS status;

SELECT '📊 Updated mandatory fields:' AS info;
SELECT
  field_name,
  field_label,
  field_type,
  is_mandatory,
  field_section,
  display_order
FROM lead_field_configurations
WHERE is_mandatory = true
ORDER BY display_order;

SELECT '✅ UPDATE COMPLETE!' AS status;
