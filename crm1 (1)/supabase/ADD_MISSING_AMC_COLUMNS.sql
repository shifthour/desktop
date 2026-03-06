-- =====================================================
-- ADD MISSING COLUMNS TO AMC_CONTRACTS TABLE
-- =====================================================
-- This adds any missing columns that the form sends
-- but the database doesn't have
-- =====================================================

-- Add assigned_technician if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'amc_contracts'
        AND column_name = 'assigned_technician'
    ) THEN
        ALTER TABLE amc_contracts
        ADD COLUMN assigned_technician VARCHAR(255);
    END IF;
END $$;

-- Add service_manager if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'amc_contracts'
        AND column_name = 'service_manager'
    ) THEN
        ALTER TABLE amc_contracts
        ADD COLUMN service_manager VARCHAR(255);
    END IF;
END $$;

-- Add terms_and_conditions if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'amc_contracts'
        AND column_name = 'terms_and_conditions'
    ) THEN
        ALTER TABLE amc_contracts
        ADD COLUMN terms_and_conditions TEXT;
    END IF;
END $$;

-- Add exclusions if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'amc_contracts'
        AND column_name = 'exclusions'
    ) THEN
        ALTER TABLE amc_contracts
        ADD COLUMN exclusions TEXT;
    END IF;
END $$;

-- Add special_instructions if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'amc_contracts'
        AND column_name = 'special_instructions'
    ) THEN
        ALTER TABLE amc_contracts
        ADD COLUMN special_instructions TEXT;
    END IF;
END $$;

-- Verification
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'amc_contracts'
  AND column_name IN (
    'assigned_technician',
    'service_manager',
    'terms_and_conditions',
    'exclusions',
    'special_instructions'
  )
ORDER BY column_name;

SELECT '✅ Missing columns added to amc_contracts table' AS status;
