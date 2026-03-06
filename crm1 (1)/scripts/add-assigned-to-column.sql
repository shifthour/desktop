-- Add missing columns to accounts table for import functionality
-- This script safely adds the assigned_to and contact_name columns if they don't exist

-- Step 1: Add the assigned_to column to accounts table
DO $$ 
BEGIN
    -- Check if the assigned_to column doesn't exist before adding it
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'accounts' 
        AND column_name = 'assigned_to' 
        AND table_schema = 'public'
    ) THEN
        ALTER TABLE accounts ADD COLUMN assigned_to TEXT;
        RAISE NOTICE 'Column assigned_to added to accounts table';
    ELSE
        RAISE NOTICE 'Column assigned_to already exists in accounts table';
    END IF;
    
    -- Check if the contact_name column doesn't exist before adding it
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'accounts' 
        AND column_name = 'contact_name' 
        AND table_schema = 'public'
    ) THEN
        ALTER TABLE accounts ADD COLUMN contact_name TEXT;
        RAISE NOTICE 'Column contact_name added to accounts table';
    ELSE
        RAISE NOTICE 'Column contact_name already exists in accounts table';
    END IF;
END $$;

-- Step 2: Create index for better performance on assigned_to field
CREATE INDEX IF NOT EXISTS idx_accounts_assigned_to ON accounts(assigned_to);

-- Step 3: Update the account_summary view to include assigned_to and contact_name
DROP VIEW IF EXISTS account_summary;
CREATE VIEW account_summary AS
SELECT 
    a.id,
    a.account_name,
    a.industry,
    a.status,
    a.billing_city,
    a.billing_state,
    a.billing_country,
    a.country,
    a.phone,
    a.website,
    a.contact_name,
    a.assigned_to,
    a.company_id,
    a.owner_id,
    a.created_at,
    u.full_name as owner_name,
    u.email as owner_email
FROM accounts a
LEFT JOIN users u ON a.owner_id = u.id;

-- Step 4: Grant necessary permissions
GRANT ALL ON account_summary TO authenticated;

-- Step 5: Show the updated table structure
SELECT 
    column_name, 
    data_type, 
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_name = 'accounts' 
    AND table_schema = 'public'
ORDER BY ordinal_position;

-- Step 6: Show sample of accounts with new column
SELECT 
    account_name,
    assigned_to,
    status,
    created_at
FROM accounts 
LIMIT 5;