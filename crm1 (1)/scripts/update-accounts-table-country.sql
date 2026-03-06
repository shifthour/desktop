-- Update accounts table to add country column and remove unnecessary columns
-- Run this script in your Supabase SQL Editor

-- Step 1: Add country column if it doesn't exist
ALTER TABLE accounts 
ADD COLUMN IF NOT EXISTS country TEXT DEFAULT 'India';

-- Step 2: Add billing_country column if it doesn't exist (for consistency with billing address)
ALTER TABLE accounts 
ADD COLUMN IF NOT EXISTS billing_country TEXT DEFAULT 'India';

-- Step 3: Drop the columns we no longer need (if they exist)
-- Note: Be careful with these drops as they will permanently delete the data in these columns
-- Comment out any DROP statements for columns you want to keep

-- Drop annual_turnover column if it exists
ALTER TABLE accounts 
DROP COLUMN IF EXISTS annual_turnover;

-- Drop turnover column if it exists  
ALTER TABLE accounts 
DROP COLUMN IF EXISTS turnover;

-- Drop employees column if it exists
ALTER TABLE accounts 
DROP COLUMN IF EXISTS employees;

-- Drop employee_count column if it exists
ALTER TABLE accounts 
DROP COLUMN IF EXISTS employee_count;

-- Drop region column if it exists (we're using country instead)
ALTER TABLE accounts 
DROP COLUMN IF EXISTS region;

-- Step 4: Update any existing records that have NULL country to default to 'India'
UPDATE accounts 
SET billing_country = 'India' 
WHERE billing_country IS NULL;

UPDATE accounts 
SET country = 'India' 
WHERE country IS NULL;

-- Step 5: Verify the changes
SELECT 
    column_name, 
    data_type, 
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_name = 'accounts' 
    AND table_schema = 'public'
ORDER BY ordinal_position;

-- Step 6: Show sample data to verify
SELECT 
    id,
    account_name,
    billing_city,
    billing_state,
    billing_country,
    country,
    status,
    created_at
FROM accounts
LIMIT 10;