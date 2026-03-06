-- =====================================================
-- MAKE AMC INSTALLATION_ID NULLABLE
-- =====================================================
-- This allows creating AMC contracts directly without
-- requiring them to be linked to an installation
-- =====================================================

-- Make installation_id nullable in amc_contracts table
ALTER TABLE amc_contracts
ALTER COLUMN installation_id DROP NOT NULL;

-- Verification
SELECT
    column_name,
    is_nullable,
    data_type
FROM information_schema.columns
WHERE table_name = 'amc_contracts'
  AND column_name = 'installation_id';

-- Show result
SELECT '✅ installation_id is now nullable in amc_contracts table' AS status;
