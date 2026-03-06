-- =====================================================
-- FIX AMC PRODUCT_NAME COLUMN
-- =====================================================
-- Make product_name nullable since the form uses
-- equipment_details (JSONB) to store equipment info
-- =====================================================

-- Make product_name nullable
ALTER TABLE amc_contracts
ALTER COLUMN product_name DROP NOT NULL;

-- Verification
SELECT
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'amc_contracts'
  AND column_name = 'product_name';

SELECT '✅ product_name is now nullable in amc_contracts table' AS status;
