-- Add missing columns to complaints table
DO $$ 
BEGIN
    -- Add category if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'category'
    ) THEN
        ALTER TABLE complaints ADD COLUMN category VARCHAR(50);
    END IF;

    -- Add city if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'city'
    ) THEN
        ALTER TABLE complaints ADD COLUMN city VARCHAR(100);
    END IF;

    -- Add state if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'state'
    ) THEN
        ALTER TABLE complaints ADD COLUMN state VARCHAR(100);
    END IF;

    -- Add pincode if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'pincode'
    ) THEN
        ALTER TABLE complaints ADD COLUMN pincode VARCHAR(10);
    END IF;

    -- Add customer_phone if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'customer_phone'
    ) THEN
        ALTER TABLE complaints ADD COLUMN customer_phone VARCHAR(20);
    END IF;

    -- Add customer_email if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'customer_email'
    ) THEN
        ALTER TABLE complaints ADD COLUMN customer_email VARCHAR(255);
    END IF;

    -- Add customer_address if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'customer_address'
    ) THEN
        ALTER TABLE complaints ADD COLUMN customer_address TEXT;
    END IF;

    -- Add product_name if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'product_name'
    ) THEN
        ALTER TABLE complaints ADD COLUMN product_name VARCHAR(255);
    END IF;

    -- Add product_model if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'product_model'
    ) THEN
        ALTER TABLE complaints ADD COLUMN product_model VARCHAR(255);
    END IF;

    -- Add serial_number if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'serial_number'
    ) THEN
        ALTER TABLE complaints ADD COLUMN serial_number VARCHAR(255);
    END IF;

    -- Add purchase_date if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'purchase_date'
    ) THEN
        ALTER TABLE complaints ADD COLUMN purchase_date DATE;
    END IF;

    -- Add warranty_status if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'warranty_status'
    ) THEN
        ALTER TABLE complaints ADD COLUMN warranty_status VARCHAR(50);
    END IF;

    -- Add communication_history if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'communication_history'
    ) THEN
        ALTER TABLE complaints ADD COLUMN communication_history JSONB DEFAULT '[]'::jsonb;
    END IF;

END $$;

-- Show updated table structure
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'complaints' 
ORDER BY ordinal_position;