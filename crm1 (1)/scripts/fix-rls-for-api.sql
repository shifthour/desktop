-- Fix RLS policies to allow service role access for API calls
-- This will allow the accounts API to work properly

-- First, let's see current policies
SELECT 'CURRENT RLS POLICIES:' as info;
SELECT schemaname, tablename, policyname, permissive, roles, cmd, qual 
FROM pg_policies 
WHERE tablename = 'accounts';

-- Drop existing restrictive policies
DROP POLICY IF EXISTS "Super admins can access all accounts" ON accounts;
DROP POLICY IF EXISTS "Company admins can access company accounts" ON accounts;
DROP POLICY IF EXISTS "Users can view assigned accounts" ON accounts;

-- Create new policies that work with both authenticated users and service role

-- Policy 1: Allow service role (for API calls)
CREATE POLICY "Allow service role access" ON accounts
    FOR ALL
    TO service_role
    USING (true);

-- Policy 2: Allow authenticated users to see accounts from their company
CREATE POLICY "Users can access company accounts" ON accounts
    FOR SELECT
    TO authenticated
    USING (
        company_id IN (
            SELECT company_id FROM users 
            WHERE users.id = auth.uid()
        )
    );

-- Policy 3: Allow company admins to manage accounts in their company  
CREATE POLICY "Company admins can manage company accounts" ON accounts
    FOR ALL
    TO authenticated
    USING (
        company_id IN (
            SELECT company_id FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_admin = true
        )
    );

-- Policy 4: Allow super admins full access
CREATE POLICY "Super admins can access all accounts" ON accounts
    FOR ALL
    TO authenticated
    USING (
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_super_admin = true
        )
    );

-- Test the fix
SELECT 'TESTING ACCESS:' as info;
SELECT COUNT(*) as total_accounts_via_service_role FROM accounts;