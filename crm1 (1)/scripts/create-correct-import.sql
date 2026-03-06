-- Step 1: Let's see what we have in the database
SELECT 'EXISTING COMPANIES:' as section;
SELECT id, name, domain, created_at FROM companies ORDER BY created_at;

SELECT 'EXISTING USERS:' as section;
SELECT u.id, u.full_name, u.email, u.is_admin, c.name as company_name
FROM users u
LEFT JOIN companies c ON u.company_id = c.id
ORDER BY c.name, u.is_admin DESC;

-- Step 2: Create demo user if doesn't exist, or find existing one
-- Let's use Labgigs company (the first one) for our demo accounts
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
    (SELECT id FROM companies ORDER BY created_at LIMIT 1) as company_id,
    'Demo Account Test' as account_name,
    'Technology' as industry,
    'www.demo.com' as website,
    'Bangalore' as billing_city,
    'Karnataka' as billing_state,
    'India' as billing_country,
    (SELECT id FROM users WHERE is_admin = true ORDER BY created_at LIMIT 1) as owner_id,
    'Active' as status,
    'Customer' as account_type
WHERE NOT EXISTS (SELECT 1 FROM accounts WHERE account_name = 'Demo Account Test');

-- Step 3: Check what we created
SELECT 'TEST RESULT:' as section;
SELECT 
    a.account_name,
    a.billing_city,
    u.full_name as owner_name,
    c.name as company_name
FROM accounts a
JOIN users u ON a.owner_id = u.id
JOIN companies c ON a.company_id = c.id
WHERE a.account_name = 'Demo Account Test';