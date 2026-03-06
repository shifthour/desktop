-- Find users in the demo company
SELECT 
    u.id,
    u.full_name,
    u.email,
    u.is_admin,
    c.name as company_name,
    c.id as company_id
FROM users u
JOIN companies c ON u.company_id = c.id
WHERE c.name = 'LabGig Demo Company'
ORDER BY u.is_admin DESC, u.created_at;