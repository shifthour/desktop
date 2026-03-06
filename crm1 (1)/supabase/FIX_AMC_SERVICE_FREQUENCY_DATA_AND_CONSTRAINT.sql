-- =====================================================
-- FIX AMC SERVICE_FREQUENCY - UPDATE DATA AND CONSTRAINT
-- =====================================================

-- First, let's see what values currently exist in the table
SELECT DISTINCT service_frequency, COUNT(*) as count
FROM amc_contracts
GROUP BY service_frequency;

-- Drop the old constraint
ALTER TABLE amc_contracts
DROP CONSTRAINT IF EXISTS amc_service_frequency_check;

-- Update any existing data that might have old values
-- Map common variations to the new format
UPDATE amc_contracts
SET service_frequency = CASE
    WHEN LOWER(service_frequency) IN ('month', 'monthly') THEN 'monthly'
    WHEN LOWER(service_frequency) IN ('quarter', 'quarterly', '3 months') THEN 'quarterly'
    WHEN LOWER(service_frequency) IN ('half-yearly', 'half yearly', 'semi-annual', '6 months') THEN 'half_yearly'
    WHEN LOWER(service_frequency) IN ('year', 'yearly', 'annual', 'annually', '12 months') THEN 'yearly'
    ELSE 'quarterly'  -- Default to quarterly if unknown
END
WHERE service_frequency IS NOT NULL;

-- Now add the new constraint
ALTER TABLE amc_contracts
ADD CONSTRAINT amc_service_frequency_check
CHECK (service_frequency IN ('monthly', 'quarterly', 'half_yearly', 'yearly'));

-- Verification - check updated values
SELECT DISTINCT service_frequency, COUNT(*) as count
FROM amc_contracts
GROUP BY service_frequency;

-- Show constraint
SELECT
    conname AS constraint_name,
    pg_get_constraintdef(oid) AS constraint_definition
FROM pg_constraint
WHERE conname = 'amc_service_frequency_check'
  AND conrelid = 'amc_contracts'::regclass;

SELECT '✅ service_frequency data updated and constraint fixed!' AS status;
