-- Check accounts and their owner_id values
SELECT
    id,
    account_name,
    owner_id,
    created_by
FROM accounts
ORDER BY created_at DESC
LIMIT 10;

-- Check if owner_id exists in users table
SELECT
    a.id,
    a.account_name,
    a.owner_id,
    u.id as user_id,
    u.full_name,
    u.email
FROM accounts a
LEFT JOIN users u ON a.owner_id = u.id
ORDER BY a.created_at DESC
LIMIT 10;
