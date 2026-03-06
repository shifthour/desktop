-- =====================================================
-- FIX ALL NOT NULL CONSTRAINTS IN AMC_CONTRACTS
-- =====================================================
-- Make all optional fields nullable so the form can work
-- =====================================================

-- Make product_name nullable (uses equipment_details instead)
ALTER TABLE amc_contracts
ALTER COLUMN product_name DROP NOT NULL;

-- Make installation_id nullable (not all AMCs come from installations)
ALTER TABLE amc_contracts
ALTER COLUMN installation_id DROP NOT NULL;

-- Check if amc_number exists and make it nullable if it does
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'amc_contracts'
        AND column_name = 'amc_number'
    ) THEN
        EXECUTE 'ALTER TABLE amc_contracts ALTER COLUMN amc_number DROP NOT NULL';
    END IF;
END $$;

-- Check which columns still have NOT NULL constraints
SELECT
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'amc_contracts'
  AND is_nullable = 'NO'
  AND column_name NOT IN ('id', 'created_at', 'updated_at', 'company_id')
ORDER BY column_name;

SELECT '✅ Fixed NOT NULL constraints in amc_contracts table' AS status;
