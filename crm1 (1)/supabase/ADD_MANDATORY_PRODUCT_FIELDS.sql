-- =====================================================
-- ADD MANDATORY PRODUCT FIELDS
-- =====================================================
-- This script adds the three mandatory columns to the products table
-- and updates product_field_configurations for all companies
--
-- Fields to add:
-- 1. principals (already exists as 'principal')
-- 2. hsn_code (NEW)
-- 3. unit_of_measurement (NEW)
-- =====================================================

-- Step 1: Add missing columns to products table
-- Note: 'principal' column already exists, so we only add hsn_code and unit_of_measurement

ALTER TABLE products
ADD COLUMN IF NOT EXISTS hsn_code VARCHAR(20),
ADD COLUMN IF NOT EXISTS unit_of_measurement VARCHAR(50);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_products_hsn_code ON products(hsn_code);
CREATE INDEX IF NOT EXISTS idx_products_uom ON products(unit_of_measurement);

-- Step 2: Update product_field_configurations to add these as mandatory fields
DO $$
DECLARE
    company_record RECORD;
BEGIN
    FOR company_record IN SELECT id FROM companies LOOP

        -- Add Principals field (using existing 'principal' column)
        INSERT INTO product_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES
        (company_record.id, 'principal', 'Principals', 'text', true, true, 'basic_info', 11,
            'Enter principal/manufacturer name', 'Principal or manufacturer of the product')
        ON CONFLICT (company_id, field_name)
        DO UPDATE SET
            is_mandatory = true,
            is_enabled = true,
            field_label = 'Principals',
            display_order = 11;

        -- Add HSN Code field (NEW mandatory field)
        INSERT INTO product_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES
        (company_record.id, 'hsn_code', 'HSN Code', 'text', true, true, 'basic_info', 12,
            'Enter HSN/SAC code', 'Harmonized System of Nomenclature code for tax classification')
        ON CONFLICT (company_id, field_name)
        DO UPDATE SET
            is_mandatory = true,
            is_enabled = true,
            display_order = 12;

        -- Add Unit of Measurement field (NEW mandatory field)
        INSERT INTO product_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, field_options, placeholder, help_text
        ) VALUES
        (company_record.id, 'unit_of_measurement', 'Unit of Measurement (UOM)', 'select', true, true, 'inventory', 13,
            ARRAY['Piece', 'Kilogram (kg)', 'Gram (g)', 'Liter (L)', 'Milliliter (mL)', 'Meter (m)', 'Centimeter (cm)', 'Square Meter (sqm)', 'Cubic Meter (cbm)', 'Box', 'Carton', 'Dozen', 'Pack', 'Set', 'Unit', 'Hour', 'Day', 'Month', 'Year'],
            'Select unit of measurement', 'Unit of measurement for quantity calculations')
        ON CONFLICT (company_id, field_name)
        DO UPDATE SET
            is_mandatory = true,
            is_enabled = true,
            display_order = 13,
            field_options = ARRAY['Piece', 'Kilogram (kg)', 'Gram (g)', 'Liter (L)', 'Milliliter (mL)', 'Meter (m)', 'Centimeter (cm)', 'Square Meter (sqm)', 'Cubic Meter (cbm)', 'Box', 'Carton', 'Dozen', 'Pack', 'Set', 'Unit', 'Hour', 'Day', 'Month', 'Year'];

    END LOOP;
END $$;

-- Step 3: Verification
SELECT '✅ Product table columns added/verified' AS status;

SELECT 'Column exists check:' AS info;
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_name = 'products'
AND column_name IN ('principal', 'hsn_code', 'unit_of_measurement')
ORDER BY column_name;

SELECT '📊 Updated mandatory field configurations:' AS info;
SELECT
    fc.field_name,
    fc.field_label,
    fc.field_type,
    fc.is_mandatory,
    fc.is_enabled,
    fc.field_section,
    fc.display_order,
    COUNT(*) OVER (PARTITION BY fc.field_name) as company_count
FROM product_field_configurations fc
WHERE fc.field_name IN ('principal', 'hsn_code', 'unit_of_measurement')
ORDER BY fc.display_order
LIMIT 3;

SELECT '✅ MANDATORY PRODUCT FIELDS ADDED SUCCESSFULLY!' AS status;
