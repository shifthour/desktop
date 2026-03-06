-- First, let's check what columns exist in the AMC table
-- Run this to see the current table structure:
-- SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'amc_contracts';

-- Add missing columns if they don't exist
-- This script will safely add columns without affecting existing data

-- Add customer_name if it doesn't exist (might be named account_name)
DO $$ 
BEGIN
    -- Check if customer_name column exists, if not add it
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'customer_name'
    ) THEN
        -- Check if account_name exists and rename it
        IF EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'amc_contracts' AND column_name = 'account_name'
        ) THEN
            ALTER TABLE amc_contracts RENAME COLUMN account_name TO customer_name;
        ELSE
            ALTER TABLE amc_contracts ADD COLUMN customer_name VARCHAR(255);
        END IF;
    END IF;

    -- Add contact_person if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'contact_person'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN contact_person VARCHAR(255);
    END IF;

    -- Add service_address if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'service_address'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN service_address TEXT;
    END IF;

    -- Add equipment_details if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'equipment_details'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN equipment_details JSONB DEFAULT '[]'::jsonb;
    END IF;

    -- Add contract_value if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'contract_value'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN contract_value DECIMAL(15,2);
    END IF;

    -- Add start_date if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'start_date'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN start_date DATE;
    END IF;

    -- Add end_date if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'end_date'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN end_date DATE;
    END IF;

    -- Add installation_id if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'installation_id'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN installation_id UUID;
    END IF;

    -- Add installation_number if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'installation_number'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN installation_number VARCHAR(50);
    END IF;

    -- Add source_type if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'source_type'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN source_type VARCHAR(20);
    END IF;

    -- Add source_reference if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'source_reference'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN source_reference VARCHAR(50);
    END IF;

    -- Add service_frequency if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'service_frequency'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN service_frequency VARCHAR(20) DEFAULT 'quarterly';
    END IF;

    -- Add number_of_services if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'number_of_services'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN number_of_services INTEGER DEFAULT 4;
    END IF;

    -- Add services_completed if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'services_completed'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN services_completed INTEGER DEFAULT 0;
    END IF;

    -- Add labour_charges_included if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'labour_charges_included'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN labour_charges_included BOOLEAN DEFAULT true;
    END IF;

    -- Add spare_parts_included if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'spare_parts_included'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN spare_parts_included BOOLEAN DEFAULT false;
    END IF;

    -- Add emergency_support if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'emergency_support'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN emergency_support BOOLEAN DEFAULT false;
    END IF;

    -- Add response_time_hours if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'response_time_hours'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN response_time_hours INTEGER DEFAULT 24;
    END IF;

    -- Add special_instructions if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'special_instructions'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN special_instructions TEXT;
    END IF;

    -- Add duration_months if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'duration_months'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN duration_months INTEGER DEFAULT 12;
    END IF;

    -- Add contract_type if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'contract_type'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN contract_type VARCHAR(50) DEFAULT 'comprehensive';
    END IF;

END $$;

-- Create or update the auto-number function for AMC numbers
CREATE OR REPLACE FUNCTION generate_amc_number()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.amc_number IS NULL OR NEW.amc_number = '' THEN
        NEW.amc_number := 'AMC-' || TO_CHAR(NOW(), 'YYYY') || '-' || 
                         LPAD((
                             SELECT COALESCE(MAX(CAST(SUBSTRING(amc_number FROM 'AMC-\d{4}-(\d+)') AS INTEGER)), 0) + 1
                             FROM amc_contracts 
                             WHERE amc_number ~ '^AMC-\d{4}-\d+$'
                             AND EXTRACT(YEAR FROM created_at) = EXTRACT(YEAR FROM NOW())
                         )::TEXT, 4, '0');
    END IF;
    
    IF TG_OP = 'UPDATE' THEN
        NEW.updated_at := NOW();
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger if it doesn't exist
DROP TRIGGER IF EXISTS amc_contracts_auto_number ON amc_contracts;
CREATE TRIGGER amc_contracts_auto_number
    BEFORE INSERT OR UPDATE ON amc_contracts
    FOR EACH ROW
    EXECUTE FUNCTION generate_amc_number();

-- Show the final table structure
SELECT column_name, data_type, is_nullable, column_default 
FROM information_schema.columns 
WHERE table_name = 'amc_contracts' 
ORDER BY ordinal_position;