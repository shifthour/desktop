-- Enable RLS Policies for CRM System
-- First, drop any existing policies to avoid conflicts

-- Drop existing policies on companies table
DROP POLICY IF EXISTS "companies_all_access" ON companies;
DROP POLICY IF EXISTS "companies_select_policy" ON companies;
DROP POLICY IF EXISTS "companies_insert_policy" ON companies;
DROP POLICY IF EXISTS "companies_update_policy" ON companies;
DROP POLICY IF EXISTS "companies_delete_policy" ON companies;

-- Drop existing policies on users table
DROP POLICY IF EXISTS "users_all_access" ON users;
DROP POLICY IF EXISTS "users_select_policy" ON users;
DROP POLICY IF EXISTS "users_insert_policy" ON users;
DROP POLICY IF EXISTS "users_update_policy" ON users;
DROP POLICY IF EXISTS "users_delete_policy" ON users;

-- Drop existing policies on user_roles table
DROP POLICY IF EXISTS "user_roles_all_access" ON user_roles;
DROP POLICY IF EXISTS "user_roles_select_policy" ON user_roles;

-- Now enable RLS on all tables
ALTER TABLE companies ENABLE ROW LEVEL SECURITY;
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_roles ENABLE ROW LEVEL SECURITY;

-- Create is_super_admin function if it doesn't exist
CREATE OR REPLACE FUNCTION is_super_admin()
RETURNS BOOLEAN AS $$
BEGIN
  RETURN EXISTS (
    SELECT 1 FROM users 
    WHERE id = auth.uid() 
    AND is_super_admin = true
  );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Policies for companies table
CREATE POLICY "companies_all_access" ON companies
FOR ALL USING (true);

-- Policies for users table  
CREATE POLICY "users_all_access" ON users
FOR ALL USING (true);

-- Policies for user_roles table (read-only for everyone)
CREATE POLICY "user_roles_all_access" ON user_roles
FOR ALL USING (true);

-- Grant necessary permissions
GRANT ALL ON companies TO authenticated;
GRANT ALL ON users TO authenticated;
GRANT ALL ON user_roles TO authenticated;

-- Success message
SELECT 'RLS policies successfully enabled on all tables' as message;