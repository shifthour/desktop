-- =====================================================
-- FIX AMC SERVICE_FREQUENCY CHECK CONSTRAINT
-- =====================================================
-- First, let's see what the current constraint is
-- =====================================================

-- Check the current constraint definition
SELECT
    conname AS constraint_name,
    pg_get_constraintdef(oid) AS constraint_definition
FROM pg_constraint
WHERE conname = 'amc_service_frequency_check'
  AND conrelid = 'amc_contracts'::regclass;

-- Drop the old constraint
ALTER TABLE amc_contracts
DROP CONSTRAINT IF EXISTS amc_service_frequency_check;

-- Add new constraint that matches what the form sends
ALTER TABLE amc_contracts
ADD CONSTRAINT amc_service_frequency_check
CHECK (service_frequency IN ('monthly', 'quarterly', 'half_yearly', 'yearly'));

-- Verification
SELECT
    conname AS constraint_name,
    pg_get_constraintdef(oid) AS constraint_definition
FROM pg_constraint
WHERE conname = 'amc_service_frequency_check'
  AND conrelid = 'amc_contracts'::regclass;

SELECT '✅ service_frequency check constraint updated!' AS status;
