-- Check RLS status on all tables
SELECT 
    schemaname,
    tablename,
    rowsecurity
FROM 
    pg_tables
WHERE 
    schemaname = 'public'
    AND tablename IN ('accounts', 'leads', 'products', 'deals', 'contacts')
ORDER BY 
    tablename;

-- Check if there are any RLS policies
SELECT 
    schemaname,
    tablename,
    policyname,
    permissive,
    roles,
    cmd,
    qual,
    with_check
FROM 
    pg_policies
WHERE 
    schemaname = 'public'
    AND tablename IN ('accounts', 'leads', 'products', 'deals', 'contacts');

-- Count records in each table
SELECT 'accounts' as table_name, COUNT(*) as count FROM accounts
UNION ALL
SELECT 'leads' as table_name, COUNT(*) as count FROM leads
UNION ALL
SELECT 'products' as table_name, COUNT(*) as count FROM products
UNION ALL
SELECT 'deals' as table_name, COUNT(*) as count FROM deals
UNION ALL
SELECT 'contacts' as table_name, COUNT(*) as count FROM contacts
ORDER BY table_name;

-- IMPORTANT: Temporarily disable RLS to test if this is the issue
-- WARNING: Only run this for debugging, re-enable RLS after testing!
ALTER TABLE accounts DISABLE ROW LEVEL SECURITY;
ALTER TABLE leads DISABLE ROW LEVEL SECURITY;
ALTER TABLE products DISABLE ROW LEVEL SECURITY;
ALTER TABLE deals DISABLE ROW LEVEL SECURITY;
ALTER TABLE contacts DISABLE ROW LEVEL SECURITY;

-- To re-enable RLS later (DO NOT FORGET!):
-- ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE leads ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE products ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE deals ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE contacts ENABLE ROW LEVEL SECURITY;