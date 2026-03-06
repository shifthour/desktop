-- Update existing accounts to set owner_id to a specific user
-- First, let's see who the users are
SELECT id, full_name, email, company_id FROM users;

-- Replace 'YOUR_USER_ID_HERE' with your actual user ID from the query above
-- UPDATE accounts
-- SET owner_id = 'YOUR_USER_ID_HERE'
-- WHERE owner_id IS NULL;

-- To set it to the user f41e509a-4c92-4baa-bca2-ab1d3410e465 (from the logs):
UPDATE accounts
SET owner_id = 'f41e509a-4c92-4baa-bca2-ab1d3410e465',
    created_by = 'f41e509a-4c92-4baa-bca2-ab1d3410e465'
WHERE owner_id IS NULL;

-- Verify the update
SELECT
    a.id,
    a.account_name,
    a.owner_id,
    u.full_name,
    u.email
FROM accounts a
LEFT JOIN users u ON a.owner_id = u.id
ORDER BY a.created_at DESC
LIMIT 10;
