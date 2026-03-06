-- Safe user insertion - handles foreign key constraints
-- Copy and paste this EXACTLY into Supabase SQL Editor

-- First, check current users and their references
SELECT 'Current users:' as info;
SELECT username, id FROM public.edc_users;

SELECT 'Studies referencing users:' as info;
SELECT protocol_number, created_by FROM public.edc_studies WHERE created_by IS NOT NULL;

-- Disable RLS temporarily  
ALTER TABLE public.edc_users DISABLE ROW LEVEL SECURITY;

-- Instead of deleting, let's INSERT with ON CONFLICT DO NOTHING
-- This will add users if they don't exist, or skip if they do

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
) ON CONFLICT (username) DO UPDATE SET
  password_hash = EXCLUDED.password_hash,
  first_name = EXCLUDED.first_name,
  last_name = EXCLUDED.last_name,
  role = EXCLUDED.role,
  phase = EXCLUDED.phase,
  permissions = EXCLUDED.permissions,
  status = EXCLUDED.status,
  updated_at = NOW();

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
) ON CONFLICT (username) DO UPDATE SET
  password_hash = EXCLUDED.password_hash,
  first_name = EXCLUDED.first_name,
  last_name = EXCLUDED.last_name,
  role = EXCLUDED.role,
  phase = EXCLUDED.phase,
  permissions = EXCLUDED.permissions,
  status = EXCLUDED.status,
  updated_at = NOW();

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
) ON CONFLICT (username) DO UPDATE SET
  password_hash = EXCLUDED.password_hash,
  first_name = EXCLUDED.first_name,
  last_name = EXCLUDED.last_name,
  role = EXCLUDED.role,
  phase = EXCLUDED.phase,
  permissions = EXCLUDED.permissions,
  status = EXCLUDED.status,
  updated_at = NOW();

-- Re-enable RLS
ALTER TABLE public.edc_users ENABLE ROW LEVEL SECURITY;

-- Verify users were created/updated
SELECT 'Final user list:' as info;
SELECT username, email, phase, role, status, created_at FROM public.edc_users ORDER BY username;