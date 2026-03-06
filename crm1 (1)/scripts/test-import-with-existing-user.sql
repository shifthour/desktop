-- Test import using an existing user ID
-- First, let's find an existing user to use as owner

-- Find any existing admin user
WITH existing_admin AS (
    SELECT u.id, u.full_name, c.name as company_name
    FROM users u
    JOIN companies c ON u.company_id = c.id
    WHERE u.is_admin = true
    LIMIT 1
)
SELECT 'Using existing admin:' as info, * FROM existing_admin;

-- Temporarily disable RLS
ALTER TABLE accounts DISABLE ROW LEVEL SECURITY;

-- Insert test accounts using the first available admin user
INSERT INTO accounts (
    company_id, 
    account_name, 
    industry, 
    website, 
    billing_city, 
    billing_state, 
    billing_country,
    owner_id, 
    status, 
    account_type
) 
SELECT 
    '22adbd06-8ce1-49ea-9a03-d0b46720c624' as company_id,
    'Test Account 1' as account_name,
    'Technology' as industry,
    'www.test1.com' as website,
    'Mumbai' as billing_city,
    'Maharashtra' as billing_state,
    'India' as billing_country,
    u.id as owner_id,
    'Active' as status,
    'Customer' as account_type
FROM users u
WHERE u.is_admin = true
LIMIT 1;

-- Re-enable RLS
ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;

-- Check if test account was created
SELECT COUNT(*) as test_count FROM accounts 
WHERE company_id = '22adbd06-8ce1-49ea-9a03-d0b46720c624';