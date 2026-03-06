-- Manual user insertion - copy and paste this EXACTLY into Supabase SQL Editor

-- First, check if RLS is enabled
SELECT schemaname, tablename, rowsecurity FROM pg_tables WHERE tablename = 'edc_users';

-- Disable RLS temporarily  
ALTER TABLE public.edc_users DISABLE ROW LEVEL SECURITY;

-- Clear any existing users first (in case there are conflicts)
DELETE FROM public.edc_users;

-- Insert users one by one
INSERT INTO public.edc_users (
  username, 
  email, 
  password_hash, 
  first_name, 
  last_name, 
  role, 
  phase, 
  permissions,
  status,
  created_at,
  updated_at
) VALUES (
  'startup_admin',
  'startup@edc.com', 
  '$2b$10$hashedpassword1',
  'Sarah',
  'Johnson',
  'Study Designer',
  'START_UP',
  '{"createStudies": true, "designForms": true, "setupSites": true, "configureWorkflows": true, "viewReports": true, "manageUsers": false}',
  'ACTIVE',
  NOW(),
  NOW()
);

INSERT INTO public.edc_users (
  username, 
  email, 
  password_hash, 
  first_name, 
  last_name, 
  role, 
  phase, 
  permissions,
  status,
  created_at,
  updated_at
) VALUES (
  'conduct_crc',
  'conduct@edc.com',
  '$2b$10$hashedpassword2', 
  'Michael',
  'Chen',
  'Clinical Research Coordinator',
  'CONDUCT',
  '{"enrollSubjects": true, "enterData": true, "manageQueries": true, "viewReports": true, "exportData": false, "lockDatabase": false}',
  'ACTIVE',
  NOW(),
  NOW()
);

INSERT INTO public.edc_users (
  username, 
  email, 
  password_hash, 
  first_name, 
  last_name, 
  role, 
  phase, 
  permissions,
  status,
  created_at,
  updated_at
) VALUES (
  'closeout_manager',
  'closeout@edc.com',
  '$2b$10$hashedpassword3',
  'Emily',
  'Rodriguez', 
  'Data Manager',
  'CLOSE_OUT',
  '{"closeStudy": true, "exportData": true, "generateReports": true, "verifyData": true, "lockDatabase": true, "archiveData": true}',
  'ACTIVE',
  NOW(),
  NOW()
);

-- Re-enable RLS
ALTER TABLE public.edc_users ENABLE ROW LEVEL SECURITY;

-- Verify users were created
SELECT username, email, phase, role, status, created_at FROM public.edc_users;