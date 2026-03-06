-- Add address column to contacts table
-- This script safely adds the address column if it doesn't already exist

DO $$ 
BEGIN
    -- Check if address column already exists
    IF NOT EXISTS (
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name='contacts' 
        AND column_name='address'
    ) THEN
        -- Add the address column
        ALTER TABLE contacts ADD COLUMN address TEXT;
        
        -- Add comment to document the change
        COMMENT ON COLUMN contacts.address IS 'Address field for contact location';
        
        RAISE NOTICE 'Address column added to contacts table successfully';
    ELSE
        RAISE NOTICE 'Address column already exists in contacts table';
    END IF;
END $$;