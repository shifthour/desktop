-- Insert only the EDC users if they don't exist
-- This handles the case where table exists but is empty

INSERT INTO edc_users (username, email, password_hash, first_name, last_name, role, phase, permissions) 
VALUES
('startup_admin', 'startup@edc.com', '$2b$10$hashedpassword1', 'Sarah', 'Johnson', 'Study Designer', 'START_UP', 
 '{"createStudies": true, "designForms": true, "setupSites": true, "configureWorkflows": true, "viewReports": true, "manageUsers": false}'),
('conduct_crc', 'conduct@edc.com', '$2b$10$hashedpassword2', 'Michael', 'Chen', 'Clinical Research Coordinator', 'CONDUCT', 
 '{"enrollSubjects": true, "enterData": true, "manageQueries": true, "viewReports": true, "exportData": false, "lockDatabase": false}'),
('closeout_manager', 'closeout@edc.com', '$2b$10$hashedpassword3', 'Emily', 'Rodriguez', 'Data Manager', 'CLOSE_OUT', 
 '{"closeStudy": true, "exportData": true, "generateReports": true, "verifyData": true, "lockDatabase": true, "archiveData": true}')
ON CONFLICT (username) DO NOTHING;

-- Insert the missing study data for the users table to work
INSERT INTO edc_studies (protocol_number, title, description, phase, status, sponsor, indication, projected_enrollment, current_enrollment, primary_investigator, created_by) 
VALUES
('PROTO-2024-001', 'Phase III Hypertension Study', 'A randomized, double-blind, placebo-controlled study evaluating the efficacy and safety of investigational drug in patients with hypertension', 'Phase III', 'ACTIVE', 'Pharma Corp Inc', 'Hypertension', 300, 75, 'Dr. James Wilson', (SELECT id FROM edc_users WHERE username = 'startup_admin')),
('PROTO-2024-002', 'Phase II Diabetes Study', 'A multicenter study to assess the effectiveness of new diabetes treatment', 'Phase II', 'ACTIVE', 'MedTech Solutions', 'Type 2 Diabetes', 150, 45, 'Dr. Lisa Park', (SELECT id FROM edc_users WHERE username = 'startup_admin'))
ON CONFLICT (protocol_number) DO NOTHING;