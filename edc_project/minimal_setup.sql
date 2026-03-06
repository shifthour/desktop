-- Minimal EDC setup for immediate login testing
-- Run this in Supabase SQL Editor if the full script has issues

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create just the users table first
CREATE TABLE IF NOT EXISTS EDC_users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    role VARCHAR(50) NOT NULL,
    phase VARCHAR(20) NOT NULL CHECK (phase IN ('START_UP', 'CONDUCT', 'CLOSE_OUT')),
    status VARCHAR(20) DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'INACTIVE', 'SUSPENDED')),
    permissions JSONB DEFAULT '{}',
    last_login TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Insert the 3 test users
INSERT INTO EDC_users (username, email, password_hash, first_name, last_name, role, phase, permissions) VALUES
('startup_admin', 'startup@edc.com', '$2b$10$hashedpassword1', 'Sarah', 'Johnson', 'Study Designer', 'START_UP', 
 '{"createStudies": true, "designForms": true, "setupSites": true, "configureWorkflows": true, "viewReports": true, "manageUsers": false}'),
('conduct_crc', 'conduct@edc.com', '$2b$10$hashedpassword2', 'Michael', 'Chen', 'Clinical Research Coordinator', 'CONDUCT', 
 '{"enrollSubjects": true, "enterData": true, "manageQueries": true, "viewReports": true, "exportData": false, "lockDatabase": false}'),
('closeout_manager', 'closeout@edc.com', '$2b$10$hashedpassword3', 'Emily', 'Rodriguez', 'Data Manager', 'CLOSE_OUT', 
 '{"closeStudy": true, "exportData": true, "generateReports": true, "verifyData": true, "lockDatabase": true, "archiveData": true}');

-- Verify the users were created
SELECT username, email, phase, role FROM EDC_users;