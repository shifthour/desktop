-- Temporarily disable RLS for products table to fix the API
-- You can run this in Supabase SQL Editor

-- Drop the existing policy that's causing issues
DROP POLICY IF EXISTS products_company_policy ON products;

-- Disable RLS for products table temporarily  
ALTER TABLE products DISABLE ROW LEVEL SECURITY;

-- Or if you want to keep RLS but fix the policy, use this instead:
-- CREATE POLICY products_company_policy ON products
--     FOR ALL
--     USING (company_id = current_setting('request.jwt.claims', true)::json->>'company_id');