-- Step 1: Find all companies and their users
SELECT 'COMPANIES:' as info;
SELECT id, name, domain FROM companies ORDER BY created_at;

SELECT 'USERS:' as info;
SELECT u.id, u.full_name, u.email, u.is_admin, c.name as company_name
FROM users u
LEFT JOIN companies c ON u.company_id = c.id
ORDER BY c.name, u.is_admin DESC;

-- Step 2: Check if demo company exists
SELECT 'DEMO COMPANY CHECK:' as info;
SELECT * FROM companies WHERE name = 'LabGig Demo Company';

-- Step 3: Check if demo admin exists
SELECT 'DEMO ADMIN CHECK:' as info;
SELECT u.* FROM users u
JOIN companies c ON u.company_id = c.id
WHERE c.name = 'LabGig Demo Company' AND u.is_admin = true;