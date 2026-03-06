-- Add address column to accounts table
-- This script safely adds the address column if it doesn't already exist

DO $$ 
BEGIN
    -- Check if address column already exists
    IF NOT EXISTS (
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name='accounts' 
        AND column_name='address'
    ) THEN
        -- Add the address column
        ALTER TABLE accounts ADD COLUMN address TEXT;
        
        -- Add comment to document the change
        COMMENT ON COLUMN accounts.address IS 'Complete address field for account location';
        
        RAISE NOTICE 'Address column added to accounts table successfully';
    ELSE
        RAISE NOTICE 'Address column already exists in accounts table';
    END IF;
END $$;