-- Add missing columns to AMC contracts table
DO $$ 
BEGIN
    -- Add city if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'city'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN city VARCHAR(100);
    END IF;

    -- Add state if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'state'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN state VARCHAR(100);
    END IF;

    -- Add pincode if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'pincode'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN pincode VARCHAR(10);
    END IF;

    -- Add customer_phone if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'customer_phone'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN customer_phone VARCHAR(20);
    END IF;

    -- Add customer_email if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'amc_contracts' AND column_name = 'customer_email'
    ) THEN
        ALTER TABLE amc_contracts ADD COLUMN customer_email VARCHAR(255);
    END IF;

END $$;

-- Show updated table structure
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'amc_contracts' 
ORDER BY ordinal_position;