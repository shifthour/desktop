-- =====================================================
-- UPDATE PRODUCT FIELD NAMES AND TYPES
-- =====================================================
-- This script updates existing product field configurations
-- to match the new naming and types
-- =====================================================

-- Update product_category to product_category_id (text field, not dropdown)
UPDATE product_field_configurations
SET
  field_label = 'Product Category ID',
  field_type = 'text',
  placeholder = 'Enter product category ID',
  help_text = 'Primary product category classification',
  field_options = NULL,
  updated_at = NOW()
WHERE field_name = 'product_category_id';

-- Rename data_sensitive_information to data_classification with dropdown
-- First, insert the new field configuration for all companies
DO $$
DECLARE
    company_record RECORD;
BEGIN
    FOR company_record IN SELECT id FROM companies LOOP
        -- Check if data_classification already exists
        IF NOT EXISTS (
            SELECT 1 FROM product_field_configurations
            WHERE company_id = company_record.id AND field_name = 'data_classification'
        ) THEN
            -- Insert new data_classification field
            INSERT INTO product_field_configurations (
                company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
                field_section, display_order, placeholder, help_text, field_options
            ) VALUES (
                company_record.id,
                'data_classification',
                'Data Classification',
                'select',
                true,
                true,
                'technical',
                10,
                'Select data classification',
                'Data classification level',
                ARRAY['Public', 'Internal', 'Confidential', 'Restricted']
            );
        ELSE
            -- Update existing data_classification field
            UPDATE product_field_configurations
            SET
                field_label = 'Data Classification',
                field_type = 'select',
                placeholder = 'Select data classification',
                help_text = 'Data classification level',
                field_options = ARRAY['Public', 'Internal', 'Confidential', 'Restricted'],
                updated_at = NOW()
            WHERE company_id = company_record.id AND field_name = 'data_classification';
        END IF;
    END LOOP;
END $$;

-- Delete old data_sensitive_information field if it exists
DELETE FROM product_field_configurations
WHERE field_name = 'data_sensitive_information';

-- Verification
SELECT '✅ Product field configurations updated' AS status;

SELECT
    field_name,
    field_label,
    field_type,
    field_options,
    is_mandatory,
    COUNT(*) as company_count
FROM product_field_configurations
WHERE field_name IN ('product_category_id', 'data_classification')
GROUP BY field_name, field_label, field_type, field_options, is_mandatory
ORDER BY field_name;

SELECT '✅ UPDATE COMPLETE!' AS status;
