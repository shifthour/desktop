-- Debug why accounts are not showing in the UI
-- Check if accounts exist in the database
SELECT 'TOTAL ACCOUNTS IN DATABASE:' as info;
SELECT COUNT(*) as total_accounts FROM accounts;

SELECT 'ACCOUNTS IN DEMO COMPANY:' as info;
SELECT COUNT(*) as demo_company_accounts FROM accounts 
WHERE company_id = '22adbd06-8ce1-49ea-9a03-d0b46720c624';

SELECT 'SAMPLE ACCOUNTS:' as info;
SELECT id, account_name, company_id, owner_id FROM accounts 
WHERE company_id = '22adbd06-8ce1-49ea-9a03-d0b46720c624' 
LIMIT 5;

SELECT 'DEMO COMPANY INFO:' as info;
SELECT id, name FROM companies WHERE id = '22adbd06-8ce1-49ea-9a03-d0b46720c624';

SELECT 'DEMO USER INFO:' as info;
SELECT id, full_name, email, company_id FROM users 
WHERE id = '45b84401-ca13-4627-ac8f-42a11374633c';

-- Check RLS policies
SELECT 'RLS POLICIES:' as info;
SELECT schemaname, tablename, policyname, permissive, roles, cmd, qual 
FROM pg_policies 
WHERE tablename = 'accounts';