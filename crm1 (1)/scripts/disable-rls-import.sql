-- Temporarily disable RLS for import
ALTER TABLE accounts DISABLE ROW LEVEL SECURITY;

-- You can run the import now

-- After import, re-enable RLS with this command:
-- ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;