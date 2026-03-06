-- Fix RLS policy to allow login access
-- Copy and paste this EXACTLY into Supabase SQL Editor

-- First, check if users exist at all (this will show regardless of RLS)
SELECT 'Checking users table from admin perspective:' as info;

-- Disable RLS temporarily to see what's really in the table
ALTER TABLE public.edc_users DISABLE ROW LEVEL SECURITY;

-- Show all users
SELECT username, email, phase, status FROM public.edc_users;

-- If no users, insert them now
INSERT INTO public.edc_users (
  username, email, password_hash, first_name, last_name, role, phase, permissions, status
) VALUES 
('startup_admin', 'startup@edc.com', '$2b$10$hashedpassword1', 'Sarah', 'Johnson', 'Study Designer', 'START_UP', '{"createStudies": true, "designForms": true, "setupSites": true}', 'ACTIVE'),
('conduct_crc', 'conduct@edc.com', '$2b$10$hashedpassword2', 'Michael', 'Chen', 'Clinical Research Coordinator', 'CONDUCT', '{"enrollSubjects": true, "enterData": true}', 'ACTIVE'),  
('closeout_manager', 'closeout@edc.com', '$2b$10$hashedpassword3', 'Emily', 'Rodriguez', 'Data Manager', 'CLOSE_OUT', '{"closeStudy": true, "exportData": true}', 'ACTIVE')
ON CONFLICT (username) DO NOTHING;

-- Show users after insert
SELECT 'Users after insert:' as info;
SELECT username, email, phase, status FROM public.edc_users;

-- Now create RLS policy that allows anon users to SELECT for login
-- First drop any existing policies
DROP POLICY IF EXISTS "Allow anon login access" ON public.edc_users;

-- Create a policy that allows anon role to SELECT users for login purposes
CREATE POLICY "Allow anon login access" ON public.edc_users
    FOR SELECT USING (true);

-- Re-enable RLS with the new policy
ALTER TABLE public.edc_users ENABLE ROW LEVEL SECURITY;

-- Test the policy works
SELECT 'Testing RLS policy - this should show users:' as info;
SET ROLE anon;
SELECT username, phase, status FROM public.edc_users;
RESET ROLE;

SELECT 'RLS fix complete!' as info;