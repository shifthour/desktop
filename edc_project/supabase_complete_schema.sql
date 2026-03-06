-- Complete EDC Clinical Trials System Database Schema
-- All tables prefixed with 'edc_' for organization
-- Run this script in Supabase SQL editor

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Drop existing tables if they exist (in dependency order)
DROP TABLE IF EXISTS edc_reports CASCADE;
DROP TABLE IF EXISTS edc_closure_activities CASCADE;
DROP TABLE IF EXISTS edc_data_verification CASCADE;
DROP TABLE IF EXISTS edc_study_metrics CASCADE;
DROP TABLE IF EXISTS edc_audit_trail CASCADE;
DROP TABLE IF EXISTS edc_queries CASCADE;
DROP TABLE IF EXISTS edc_form_data CASCADE;
DROP TABLE IF EXISTS edc_visits CASCADE;
DROP TABLE IF EXISTS edc_form_fields CASCADE;
DROP TABLE IF EXISTS edc_form_templates CASCADE;
DROP TABLE IF EXISTS edc_subjects CASCADE;
DROP TABLE IF EXISTS edc_sites CASCADE;
DROP TABLE IF EXISTS edc_studies CASCADE;
DROP TABLE IF EXISTS edc_users CASCADE;

-- Users table (already exists, but ensure structure)
CREATE TABLE IF NOT EXISTS edc_users (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    role VARCHAR(100) NOT NULL,
    phase VARCHAR(20) NOT NULL CHECK (phase IN ('START_UP', 'CONDUCT', 'CLOSE_OUT')),
    status VARCHAR(20) DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'INACTIVE', 'SUSPENDED')),
    permissions JSONB DEFAULT '{}',
    last_login TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Studies table
CREATE TABLE IF NOT EXISTS edc_studies (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    protocol_number VARCHAR(100) UNIQUE NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    phase VARCHAR(20) NOT NULL,
    status VARCHAR(20) DEFAULT 'ACTIVE' CHECK (status IN ('PLANNING', 'ACTIVE', 'ON_HOLD', 'COMPLETED', 'CANCELLED')),
    sponsor VARCHAR(255),
    indication TEXT,
    primary_endpoint TEXT,
    secondary_endpoints TEXT[],
    inclusion_criteria TEXT,
    exclusion_criteria TEXT,
    projected_enrollment INTEGER DEFAULT 0,
    current_enrollment INTEGER DEFAULT 0,
    start_date DATE,
    estimated_completion_date DATE,
    actual_completion_date DATE,
    created_by UUID REFERENCES edc_users(id),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Sites table
CREATE TABLE IF NOT EXISTS edc_sites (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    study_id UUID REFERENCES edc_studies(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    site_number VARCHAR(50) NOT NULL,
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    postal_code VARCHAR(20),
    principal_investigator VARCHAR(255),
    coordinator_name VARCHAR(255),
    coordinator_email VARCHAR(255),
    coordinator_phone VARCHAR(50),
    status VARCHAR(20) DEFAULT 'ACTIVE' CHECK (status IN ('PENDING', 'ACTIVE', 'ON_HOLD', 'CLOSED')),
    activation_date DATE,
    subjects_enrolled INTEGER DEFAULT 0,
    subjects_completed INTEGER DEFAULT 0,
    data_complete_percentage DECIMAL(5,2) DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(study_id, site_number)
);

-- Subjects table
CREATE TABLE IF NOT EXISTS edc_subjects (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    study_id UUID REFERENCES edc_studies(id) ON DELETE CASCADE,
    site_id UUID REFERENCES edc_sites(id) ON DELETE CASCADE,
    subject_number VARCHAR(50) NOT NULL,
    screening_number VARCHAR(50),
    initials VARCHAR(10),
    date_of_birth DATE,
    gender VARCHAR(20) CHECK (gender IN ('MALE', 'FEMALE', 'OTHER', 'NOT_DISCLOSED')),
    enrollment_date DATE,
    randomization_date DATE,
    treatment_arm VARCHAR(100),
    status VARCHAR(20) DEFAULT 'ACTIVE' CHECK (status IN ('SCREENING', 'ENROLLED', 'ACTIVE', 'COMPLETED', 'WITHDRAWN', 'LOST_TO_FOLLOWUP')),
    withdrawal_reason TEXT,
    withdrawal_date DATE,
    completion_date DATE,
    visits_completed INTEGER DEFAULT 0,
    visits_pending INTEGER DEFAULT 0,
    last_visit_date DATE,
    next_visit_date DATE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(study_id, subject_number)
);

-- Form templates table (already exists but ensure structure)
CREATE TABLE IF NOT EXISTS edc_form_templates (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(100) DEFAULT 'Clinical',
    version VARCHAR(20) DEFAULT '1.0.0',
    status VARCHAR(20) DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'ACTIVE', 'RETIRED')),
    form_structure JSONB DEFAULT '{}',
    created_by UUID REFERENCES edc_users(id),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Form fields table
CREATE TABLE IF NOT EXISTS edc_form_fields (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    form_id UUID REFERENCES edc_form_templates(id) ON DELETE CASCADE,
    field_name VARCHAR(255) NOT NULL,
    field_label VARCHAR(255) NOT NULL,
    field_type VARCHAR(50) NOT NULL,
    field_order INTEGER DEFAULT 0,
    is_required BOOLEAN DEFAULT false,
    validation_rules JSONB DEFAULT '{}',
    options JSONB DEFAULT '{}',
    default_value TEXT,
    help_text TEXT,
    section VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Visits table
CREATE TABLE IF NOT EXISTS edc_visits (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    study_id UUID REFERENCES edc_studies(id) ON DELETE CASCADE,
    subject_id UUID REFERENCES edc_subjects(id) ON DELETE CASCADE,
    visit_name VARCHAR(255) NOT NULL,
    visit_number INTEGER,
    visit_type VARCHAR(50) DEFAULT 'SCHEDULED',
    scheduled_date DATE,
    actual_date DATE,
    visit_window_start DATE,
    visit_window_end DATE,
    status VARCHAR(20) DEFAULT 'SCHEDULED' CHECK (status IN ('SCHEDULED', 'IN_PROGRESS', 'COMPLETED', 'MISSED', 'CANCELLED')),
    completion_percentage DECIMAL(5,2) DEFAULT 0,
    forms_required INTEGER DEFAULT 0,
    forms_completed INTEGER DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Form data table
CREATE TABLE IF NOT EXISTS edc_form_data (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    study_id UUID REFERENCES edc_studies(id) ON DELETE CASCADE,
    site_id UUID REFERENCES edc_sites(id) ON DELETE CASCADE,
    subject_id UUID REFERENCES edc_subjects(id) ON DELETE CASCADE,
    visit_id UUID REFERENCES edc_visits(id) ON DELETE CASCADE,
    form_id UUID REFERENCES edc_form_templates(id) ON DELETE CASCADE,
    form_data JSONB DEFAULT '{}',
    status VARCHAR(20) DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'COMPLETED', 'LOCKED', 'SIGNED')),
    data_entry_start TIMESTAMPTZ,
    data_entry_complete TIMESTAMPTZ,
    entered_by UUID REFERENCES edc_users(id),
    reviewed_by UUID REFERENCES edc_users(id),
    review_date TIMESTAMPTZ,
    locked_by UUID REFERENCES edc_users(id),
    lock_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Queries table
CREATE TABLE IF NOT EXISTS edc_queries (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    study_id UUID REFERENCES edc_studies(id) ON DELETE CASCADE,
    site_id UUID REFERENCES edc_sites(id) ON DELETE CASCADE,
    subject_id UUID REFERENCES edc_subjects(id) ON DELETE CASCADE,
    visit_id UUID REFERENCES edc_visits(id) ON DELETE CASCADE,
    form_id UUID REFERENCES edc_form_templates(id) ON DELETE CASCADE,
    form_data_id UUID REFERENCES edc_form_data(id) ON DELETE CASCADE,
    field_name VARCHAR(255),
    query_type VARCHAR(50) DEFAULT 'MANUAL' CHECK (query_type IN ('MANUAL', 'AUTO', 'SYSTEM')),
    severity VARCHAR(20) DEFAULT 'MEDIUM' CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    priority VARCHAR(20) DEFAULT 'MEDIUM' CHECK (priority IN ('LOW', 'MEDIUM', 'HIGH', 'URGENT')),
    status VARCHAR(20) DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'ANSWERED', 'CLOSED', 'CANCELLED')),
    query_text TEXT NOT NULL,
    response_text TEXT,
    original_value TEXT,
    suggested_value TEXT,
    resolution_notes TEXT,
    raised_by UUID REFERENCES edc_users(id),
    raised_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    assigned_to UUID REFERENCES edc_users(id),
    assigned_date TIMESTAMPTZ,
    responded_by UUID REFERENCES edc_users(id),
    response_date TIMESTAMPTZ,
    closed_by UUID REFERENCES edc_users(id),
    closed_date TIMESTAMPTZ,
    due_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Audit trail table
CREATE TABLE IF NOT EXISTS edc_audit_trail (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    study_id UUID REFERENCES edc_studies(id),
    site_id UUID REFERENCES edc_sites(id),
    subject_id UUID REFERENCES edc_subjects(id),
    visit_id UUID REFERENCES edc_visits(id),
    form_id UUID REFERENCES edc_form_templates(id),
    form_data_id UUID REFERENCES edc_form_data(id),
    user_id UUID REFERENCES edc_users(id) NOT NULL,
    action_type VARCHAR(50) NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    entity_id UUID,
    field_name VARCHAR(255),
    old_value TEXT,
    new_value TEXT,
    reason_for_change TEXT,
    ip_address INET,
    user_agent TEXT,
    session_id VARCHAR(255),
    signature BOOLEAN DEFAULT false,
    signature_data JSONB,
    compliance_flags TEXT[],
    severity VARCHAR(20) DEFAULT 'LOW' CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    data_integrity_hash VARCHAR(64),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Study metrics table
CREATE TABLE IF NOT EXISTS edc_study_metrics (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    study_id UUID REFERENCES edc_studies(id) ON DELETE CASCADE,
    metric_name VARCHAR(255) NOT NULL,
    metric_value DECIMAL(10,2),
    metric_text TEXT,
    metric_date DATE DEFAULT CURRENT_DATE,
    calculation_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Data verification table
CREATE TABLE IF NOT EXISTS edc_data_verification (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    study_id UUID REFERENCES edc_studies(id),
    phase VARCHAR(20) NOT NULL,
    check_name VARCHAR(255) NOT NULL,
    check_description TEXT,
    status VARCHAR(20) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'IN_PROGRESS', 'PASSED', 'FAILED')),
    details TEXT,
    verified_by UUID REFERENCES edc_users(id),
    verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Closure activities table
CREATE TABLE IF NOT EXISTS edc_closure_activities (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    study_id UUID REFERENCES edc_studies(id) ON DELETE CASCADE,
    activity_name VARCHAR(255) NOT NULL,
    activity_description TEXT,
    status VARCHAR(20) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'IN_PROGRESS', 'COMPLETED')),
    completion_percentage DECIMAL(5,2) DEFAULT 0,
    assigned_to UUID REFERENCES edc_users(id),
    due_date DATE,
    completed_date DATE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Reports table
CREATE TABLE IF NOT EXISTS edc_reports (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    study_id UUID REFERENCES edc_studies(id) ON DELETE CASCADE,
    report_type VARCHAR(50) NOT NULL CHECK (report_type IN ('ENROLLMENT', 'QUERIES', 'DATA_COMPLETION', 'AUDIT_TRAIL', 'SITE_PERFORMANCE', 'STUDY_METRICS')),
    report_name VARCHAR(255) NOT NULL,
    description TEXT,
    parameters JSONB DEFAULT '{}',
    report_data JSONB DEFAULT '{}',
    status VARCHAR(20) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'GENERATING', 'COMPLETED', 'FAILED')),
    generated_by UUID REFERENCES edc_users(id) NOT NULL,
    generated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    file_path TEXT,
    file_size INTEGER,
    download_count INTEGER DEFAULT 0,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Insert demo data
-- Demo users first (needed for foreign key references)
INSERT INTO edc_users (username, email, password_hash, first_name, last_name, role, phase, status, permissions)
VALUES 
    ('startup_admin', 'admin@startup.edc.com', '$2b$10$hashedpassword1', 'Sarah', 'Wilson', 'Study Director', 'START_UP', 'ACTIVE', '{"setupSites": true, "designForms": true, "manageUsers": false, "viewReports": true, "createStudies": true, "configureWorkflows": true}'::jsonb),
    ('conduct_crc', 'crc@conduct.edc.com', '$2b$10$hashedpassword2', 'Michael', 'Chen', 'Clinical Research Coordinator', 'CONDUCT', 'ACTIVE', '{"enterData": true, "exportData": false, "viewReports": true, "lockDatabase": false, "manageQueries": true, "enrollSubjects": true}'::jsonb),
    ('closeout_manager', 'manager@closeout.edc.com', '$2b$10$hashedpassword3', 'Jennifer', 'Lopez', 'Data Manager', 'CLOSE_OUT', 'ACTIVE', '{"closeStudy": true, "exportData": true, "verifyData": true, "archiveData": true, "lockDatabase": true, "generateReports": true}'::jsonb),
    ('monitor_user', 'monitor@edc.com', '$2b$10$hashedpassword4', 'David', 'Martinez', 'Clinical Monitor', 'CONDUCT', 'ACTIVE', '{"viewReports": true, "viewData": true, "raiseQueries": true}'::jsonb)
ON CONFLICT (username) DO NOTHING;

-- Demo studies
INSERT INTO edc_studies (protocol_number, title, description, phase, status, sponsor, indication, projected_enrollment, current_enrollment, start_date, created_by) 
VALUES 
    ('PROTO-2024-001', 'Cardiology Phase III Clinical Trial', 'A randomized, double-blind, placebo-controlled study evaluating cardiovascular outcomes', 'PHASE_III', 'ACTIVE', 'CardioMed Pharmaceuticals', 'Cardiovascular Disease', 500, 362, '2024-01-15', (SELECT id FROM edc_users WHERE username = 'startup_admin')),
    ('PROTO-2024-002', 'Oncology Biomarker Study', 'Investigating novel biomarkers in cancer treatment response', 'PHASE_II', 'ACTIVE', 'OncoResearch Inc', 'Cancer', 200, 145, '2024-03-01', (SELECT id FROM edc_users WHERE username = 'startup_admin'))
ON CONFLICT (protocol_number) DO NOTHING;

-- Demo sites
INSERT INTO edc_sites (study_id, name, site_number, city, state, country, principal_investigator, status, subjects_enrolled, data_complete_percentage)
VALUES 
    ((SELECT id FROM edc_studies WHERE protocol_number = 'PROTO-2024-001'), 'Mayo Clinic', 'SITE-001', 'Rochester', 'Minnesota', 'USA', 'Dr. Sarah Johnson', 'ACTIVE', 92, 89.5),
    ((SELECT id FROM edc_studies WHERE protocol_number = 'PROTO-2024-001'), 'Johns Hopkins', 'SITE-002', 'Baltimore', 'Maryland', 'USA', 'Dr. Michael Chen', 'ACTIVE', 87, 91.2),
    ((SELECT id FROM edc_studies WHERE protocol_number = 'PROTO-2024-001'), 'Cleveland Clinic', 'SITE-003', 'Cleveland', 'Ohio', 'USA', 'Dr. Emily Rodriguez', 'ACTIVE', 94, 88.7),
    ((SELECT id FROM edc_studies WHERE protocol_number = 'PROTO-2024-002'), 'MD Anderson', 'SITE-004', 'Houston', 'Texas', 'USA', 'Dr. James Wilson', 'ACTIVE', 76, 93.1),
    ((SELECT id FROM edc_studies WHERE protocol_number = 'PROTO-2024-002'), 'Memorial Sloan Kettering', 'SITE-005', 'New York', 'New York', 'USA', 'Dr. Lisa Thompson', 'ACTIVE', 69, 87.6)
ON CONFLICT (study_id, site_number) DO NOTHING;

-- Demo subjects
INSERT INTO edc_subjects (study_id, site_id, subject_number, initials, enrollment_date, status, visits_completed, visits_pending, last_visit_date)
SELECT 
    s.study_id,
    s.id as site_id,
    'SUBJ-' || LPAD((ROW_NUMBER() OVER())::text, 3, '0'),
    CHR(65 + (random() * 25)::int) || CHR(65 + (random() * 25)::int),
    '2024-01-15'::date + (random() * 200)::int,
    CASE WHEN random() < 0.1 THEN 'COMPLETED'
         WHEN random() < 0.05 THEN 'WITHDRAWN'
         ELSE 'ACTIVE' END,
    (random() * 12)::int,
    (random() * 3)::int,
    CURRENT_DATE - (random() * 30)::int
FROM edc_sites s
CROSS JOIN generate_series(1, CASE WHEN s.site_number IN ('SITE-001', 'SITE-002', 'SITE-003') THEN 15 ELSE 12 END)
ON CONFLICT (study_id, subject_number) DO NOTHING;

-- Demo visits
INSERT INTO edc_visits (study_id, subject_id, visit_name, visit_number, scheduled_date, actual_date, status, completion_percentage)
SELECT 
    subj.study_id,
    subj.id,
    'Visit ' || v.visit_num,
    v.visit_num,
    subj.enrollment_date + (v.visit_num * 30),
    CASE WHEN v.visit_num <= subj.visits_completed THEN subj.enrollment_date + (v.visit_num * 30) + (random() * 7)::int ELSE NULL END,
    CASE WHEN v.visit_num <= subj.visits_completed THEN 'COMPLETED'
         WHEN v.visit_num = subj.visits_completed + 1 THEN 'IN_PROGRESS'
         ELSE 'SCHEDULED' END,
    CASE WHEN v.visit_num <= subj.visits_completed THEN 100.0 
         WHEN v.visit_num = subj.visits_completed + 1 THEN (random() * 50)::decimal(5,2)
         ELSE 0 END
FROM edc_subjects subj
CROSS JOIN generate_series(1, 12) v(visit_num);

-- Demo queries
INSERT INTO edc_queries (study_id, site_id, subject_id, form_data_id, field_name, severity, priority, status, query_text, raised_by, due_date)
SELECT 
    subj.study_id,
    subj.site_id,
    subj.id,
    NULL,
    CASE (random() * 5)::int
        WHEN 0 THEN 'date_of_birth'
        WHEN 1 THEN 'blood_pressure_systolic'
        WHEN 2 THEN 'medication_dose'
        WHEN 3 THEN 'adverse_event_date'
        ELSE 'vital_signs_temperature'
    END,
    CASE (random() * 4)::int
        WHEN 0 THEN 'LOW'
        WHEN 1 THEN 'MEDIUM'
        WHEN 2 THEN 'HIGH'
        ELSE 'CRITICAL'
    END,
    CASE (random() * 4)::int
        WHEN 0 THEN 'LOW'
        WHEN 1 THEN 'MEDIUM' 
        WHEN 2 THEN 'HIGH'
        ELSE 'URGENT'
    END,
    CASE (random() * 3)::int
        WHEN 0 THEN 'OPEN'
        WHEN 1 THEN 'ANSWERED'
        ELSE 'CLOSED'
    END,
    'Please clarify the reported value and provide source documentation.',
    (SELECT id FROM edc_users WHERE username = 'conduct_crc'),
    CURRENT_DATE + (random() * 30)::int
FROM edc_subjects subj
WHERE random() < 0.3
LIMIT 50;

-- Demo audit trail entries
INSERT INTO edc_audit_trail (study_id, user_id, action_type, entity_type, entity_id, field_name, old_value, new_value, ip_address, severity)
SELECT 
    s.study_id,
    (SELECT id FROM edc_users WHERE username = 'conduct_crc'),
    CASE (random() * 4)::int
        WHEN 0 THEN 'CREATE'
        WHEN 1 THEN 'UPDATE'
        WHEN 2 THEN 'DELETE'
        ELSE 'VIEW'
    END,
    'SUBJECT',
    s.id,
    'status',
    'ACTIVE',
    'COMPLETED',
    '192.168.1.100'::inet,
    'LOW'
FROM edc_subjects s
WHERE random() < 0.2
LIMIT 100;

-- Demo study metrics
INSERT INTO edc_study_metrics (study_id, metric_name, metric_value, metric_date)
SELECT 
    st.id,
    metric_name,
    CASE metric_name
        WHEN 'Enrollment Rate' THEN (st.current_enrollment::decimal / st.projected_enrollment * 100)
        WHEN 'Data Completion Rate' THEN (85 + random() * 15)
        WHEN 'Query Rate' THEN (5 + random() * 10)
        WHEN 'Site Performance' THEN (80 + random() * 20)
        ELSE random() * 100
    END,
    CURRENT_DATE - (generate_series * 7)
FROM edc_studies st
CROSS JOIN unnest(ARRAY['Enrollment Rate', 'Data Completion Rate', 'Query Rate', 'Site Performance']) metric_name
CROSS JOIN generate_series(0, 12);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_edc_subjects_study_site ON edc_subjects(study_id, site_id);
CREATE INDEX IF NOT EXISTS idx_edc_visits_subject ON edc_visits(subject_id);
CREATE INDEX IF NOT EXISTS idx_edc_queries_status ON edc_queries(status);
CREATE INDEX IF NOT EXISTS idx_edc_queries_severity ON edc_queries(severity);
CREATE INDEX IF NOT EXISTS idx_edc_audit_trail_user_date ON edc_audit_trail(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_edc_form_data_subject_form ON edc_form_data(subject_id, form_id);
CREATE INDEX IF NOT EXISTS idx_edc_reports_study_type ON edc_reports(study_id, report_type);
CREATE INDEX IF NOT EXISTS idx_edc_reports_generated_at ON edc_reports(generated_at);

-- Create views for common queries
CREATE OR REPLACE VIEW edc_study_summary AS
SELECT 
    s.id,
    s.protocol_number,
    s.title,
    s.status,
    s.projected_enrollment,
    s.current_enrollment,
    COUNT(DISTINCT sites.id) as total_sites,
    COUNT(DISTINCT CASE WHEN sites.status = 'ACTIVE' THEN sites.id END) as active_sites,
    COUNT(DISTINCT subjects.id) as total_subjects,
    COUNT(DISTINCT CASE WHEN subjects.status = 'ACTIVE' THEN subjects.id END) as active_subjects,
    COUNT(DISTINCT CASE WHEN subjects.status = 'COMPLETED' THEN subjects.id END) as completed_subjects,
    COUNT(DISTINCT queries.id) as total_queries,
    COUNT(DISTINCT CASE WHEN queries.status = 'OPEN' THEN queries.id END) as open_queries
FROM edc_studies s
LEFT JOIN edc_sites sites ON s.id = sites.study_id
LEFT JOIN edc_subjects subjects ON s.id = subjects.study_id
LEFT JOIN edc_queries queries ON s.id = queries.study_id
GROUP BY s.id, s.protocol_number, s.title, s.status, s.projected_enrollment, s.current_enrollment;

CREATE OR REPLACE VIEW edc_site_performance AS
SELECT 
    st.id as site_id,
    st.name as site_name,
    st.site_number,
    st.status,
    st.subjects_enrolled,
    st.data_complete_percentage,
    COUNT(DISTINCT subj.id) as actual_subjects,
    COUNT(DISTINCT CASE WHEN subj.status = 'COMPLETED' THEN subj.id END) as completed_subjects,
    COUNT(DISTINCT q.id) as total_queries,
    COUNT(DISTINCT CASE WHEN q.status = 'OPEN' THEN q.id END) as open_queries,
    AVG(CURRENT_DATE - q.raised_date::DATE) as avg_query_age
FROM edc_sites st
LEFT JOIN edc_subjects subj ON st.id = subj.site_id
LEFT JOIN edc_queries q ON st.id = q.site_id
GROUP BY st.id, st.name, st.site_number, st.status, st.subjects_enrolled, st.data_complete_percentage;

-- Create view for queries with calculated age
CREATE OR REPLACE VIEW edc_queries_with_age AS
SELECT 
    *,
    (CURRENT_DATE - raised_date::DATE) as age_in_days
FROM edc_queries;


-- Complete schema setup
COMMENT ON DATABASE postgres IS 'EDC Clinical Trials System Database - All tables prefixed with edc_';