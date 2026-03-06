-- Insert EDC users directly
-- Run this in Supabase SQL Editor to add the missing users

-- Temporarily disable RLS for this operation
ALTER TABLE edc_users DISABLE ROW LEVEL SECURITY;

-- Insert the users
INSERT INTO edc_users (username, email, password_hash, first_name, last_name, role, phase, permissions, status) VALUES
('startup_admin', 'startup@edc.com', '$2b$10$hashedpassword1', 'Sarah', 'Johnson', 'Study Designer', 'START_UP', 
 '{"createStudies": true, "designForms": true, "setupSites": true, "configureWorkflows": true, "viewReports": true, "manageUsers": false}', 'ACTIVE'),
('conduct_crc', 'conduct@edc.com', '$2b$10$hashedpassword2', 'Michael', 'Chen', 'Clinical Research Coordinator', 'CONDUCT', 
 '{"enrollSubjects": true, "enterData": true, "manageQueries": true, "viewReports": true, "exportData": false, "lockDatabase": false}', 'ACTIVE'),
('closeout_manager', 'closeout@edc.com', '$2b$10$hashedpassword3', 'Emily', 'Rodriguez', 'Data Manager', 'CLOSE_OUT', 
 '{"closeStudy": true, "exportData": true, "generateReports": true, "verifyData": true, "lockDatabase": true, "archiveData": true}', 'ACTIVE')
ON CONFLICT (username) DO NOTHING;

-- Re-enable RLS
ALTER TABLE edc_users ENABLE ROW LEVEL SECURITY;

-- Verify the users were inserted
SELECT username, email, phase, role, status FROM edc_users;