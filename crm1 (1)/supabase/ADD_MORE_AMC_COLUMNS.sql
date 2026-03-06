-- =====================================================
-- ADD MORE MISSING COLUMNS TO AMC_CONTRACTS TABLE
-- =====================================================

-- Add auto_renewal if it doesn't exist
ALTER TABLE amc_contracts
ADD COLUMN IF NOT EXISTS auto_renewal BOOLEAN DEFAULT false;

-- Add renewal_notice_days if it doesn't exist
ALTER TABLE amc_contracts
ADD COLUMN IF NOT EXISTS renewal_notice_days INTEGER DEFAULT 30;

-- Verification
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_name = 'amc_contracts'
  AND column_name IN ('auto_renewal', 'renewal_notice_days')
ORDER BY column_name;

SELECT '✅ Renewal columns added to amc_contracts table' AS status;
