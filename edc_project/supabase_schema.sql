-- EDC Clinical Trials System - Supabase Database Schema
-- All tables prefixed with EDC_
-- Run this script in Supabase SQL Editor

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Users and Authentication
CREATE TABLE EDC_users (
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

-- 2. Studies
CREATE TABLE EDC_studies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    protocol_number VARCHAR(100) UNIQUE NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    phase VARCHAR(20) NOT NULL,
    status VARCHAR(20) DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'ACTIVE', 'PAUSED', 'COMPLETED', 'CANCELLED')),
    sponsor VARCHAR(100),
    indication TEXT,
    projected_enrollment INTEGER,
    current_enrollment INTEGER DEFAULT 0,
    start_date DATE,
    end_date DATE,
    primary_investigator VARCHAR(100),
    created_by UUID REFERENCES EDC_users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 3. Sites
CREATE TABLE EDC_sites (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    study_id UUID NOT NULL REFERENCES EDC_studies(id),
    site_number VARCHAR(10) NOT NULL,
    name VARCHAR(200) NOT NULL,
    address TEXT,
    city VARCHAR(100),
    country VARCHAR(100),
    principal_investigator VARCHAR(100),
    contact_email VARCHAR(100),
    contact_phone VARCHAR(20),
    status VARCHAR(20) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'ACTIVE', 'INACTIVE', 'CLOSED')),
    subjects_enrolled INTEGER DEFAULT 0,
    data_complete_percentage INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(study_id, site_number)
);

-- 4. Form Templates and Sections
CREATE TABLE EDC_form_templates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(200) NOT NULL,
    description TEXT,
    category VARCHAR(50),
    version VARCHAR(20) DEFAULT '1.0.0',
    status VARCHAR(20) DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'REVIEW', 'UAT', 'PRODUCTION', 'RETIRED')),
    created_by UUID REFERENCES EDC_users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE EDC_form_sections (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    form_template_id UUID NOT NULL REFERENCES EDC_form_templates(id) ON DELETE CASCADE,
    title VARCHAR(200) NOT NULL,
    description TEXT,
    category VARCHAR(50),
    order_index INTEGER NOT NULL DEFAULT 0,
    is_required BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 5. Form Fields
CREATE TABLE EDC_form_fields (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    section_id UUID NOT NULL REFERENCES EDC_form_sections(id) ON DELETE CASCADE,
    field_type VARCHAR(50) NOT NULL,
    label VARCHAR(200) NOT NULL,
    name VARCHAR(100) NOT NULL,
    placeholder TEXT,
    helper_text TEXT,
    is_required BOOLEAN DEFAULT false,
    order_index INTEGER NOT NULL DEFAULT 0,
    validation_rules JSONB DEFAULT '{}',
    field_options JSONB DEFAULT '{}', -- For dropdown options, etc.
    units VARCHAR(20),
    normal_range VARCHAR(100),
    clinical_category VARCHAR(50),
    cdisc_variable VARCHAR(50),
    meddra_code VARCHAR(20),
    criticality VARCHAR(20) DEFAULT 'medium' CHECK (criticality IN ('low', 'medium', 'high', 'critical')),
    audit_required BOOLEAN DEFAULT false,
    source_document_required BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 6. Study Forms (Form instances for specific studies)
CREATE TABLE EDC_study_forms (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    study_id UUID NOT NULL REFERENCES EDC_studies(id),
    form_template_id UUID NOT NULL REFERENCES EDC_form_templates(id),
    visit_number INTEGER,
    visit_name VARCHAR(100),
    is_required BOOLEAN DEFAULT true,
    status VARCHAR(20) DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'INACTIVE', 'LOCKED')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 7. Subjects
CREATE TABLE EDC_subjects (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    study_id UUID NOT NULL REFERENCES EDC_studies(id),
    site_id UUID NOT NULL REFERENCES EDC_sites(id),
    subject_number VARCHAR(50) NOT NULL,
    subject_initials VARCHAR(10),
    status VARCHAR(20) DEFAULT 'SCREENED' CHECK (status IN ('SCREENED', 'ENROLLED', 'ACTIVE', 'COMPLETED', 'WITHDRAWN', 'DISCONTINUED')),
    enrollment_date DATE,
    randomization_number VARCHAR(50),
    treatment_arm VARCHAR(50),
    visits_completed INTEGER DEFAULT 0,
    visits_pending INTEGER DEFAULT 0,
    data_completion_percentage INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(study_id, subject_number)
);

-- 8. Subject Visits
CREATE TABLE EDC_subject_visits (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    subject_id UUID NOT NULL REFERENCES EDC_subjects(id),
    visit_number INTEGER NOT NULL,
    visit_name VARCHAR(100) NOT NULL,
    visit_date DATE,
    visit_window_start DATE,
    visit_window_end DATE,
    status VARCHAR(20) DEFAULT 'SCHEDULED' CHECK (status IN ('SCHEDULED', 'IN_PROGRESS', 'COMPLETED', 'MISSED', 'CANCELLED')),
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 9. Form Data (Subject responses)
CREATE TABLE EDC_form_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    subject_id UUID NOT NULL REFERENCES EDC_subjects(id),
    study_form_id UUID NOT NULL REFERENCES EDC_study_forms(id),
    visit_id UUID REFERENCES EDC_subject_visits(id),
    form_data JSONB NOT NULL DEFAULT '{}',
    status VARCHAR(20) DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'COMPLETE', 'VERIFIED', 'LOCKED')),
    completed_by UUID REFERENCES EDC_users(id),
    completed_at TIMESTAMP WITH TIME ZONE,
    verified_by UUID REFERENCES EDC_users(id),
    verified_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 10. Queries/Data Clarifications
CREATE TABLE EDC_queries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    form_data_id UUID NOT NULL REFERENCES EDC_form_data(id),
    field_name VARCHAR(100) NOT NULL,
    query_type VARCHAR(50) NOT NULL CHECK (query_type IN ('MISSING', 'INCONSISTENT', 'OUT_OF_RANGE', 'CLARIFICATION')),
    query_text TEXT NOT NULL,
    priority VARCHAR(20) DEFAULT 'MEDIUM' CHECK (priority IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    status VARCHAR(20) DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'ANSWERED', 'RESOLVED', 'CANCELLED')),
    raised_by UUID NOT NULL REFERENCES EDC_users(id),
    assigned_to UUID REFERENCES EDC_users(id),
    response_text TEXT,
    response_date TIMESTAMP WITH TIME ZONE,
    resolution_date TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 11. Audit Trail
CREATE TABLE EDC_audit_trail (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name VARCHAR(100) NOT NULL,
    record_id UUID NOT NULL,
    action VARCHAR(20) NOT NULL CHECK (action IN ('CREATE', 'UPDATE', 'DELETE', 'LOGIN', 'LOGOUT')),
    old_values JSONB,
    new_values JSONB,
    user_id UUID REFERENCES EDC_users(id),
    ip_address INET,
    user_agent TEXT,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    reason TEXT
);

-- 12. Study Metrics (for dashboards)
CREATE TABLE EDC_study_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    study_id UUID NOT NULL REFERENCES EDC_studies(id),
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC,
    metric_type VARCHAR(50) NOT NULL, -- 'enrollment', 'completion', 'queries', etc.
    calculation_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 13. Data Verification Checks
CREATE TABLE EDC_data_verification (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    study_id UUID REFERENCES EDC_studies(id),
    phase VARCHAR(20) NOT NULL,
    check_name VARCHAR(200) NOT NULL,
    check_description TEXT,
    status VARCHAR(20) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PASSED', 'WARNING', 'FAILED')),
    details TEXT,
    verified_by UUID REFERENCES EDC_users(id),
    verified_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 14. Closure Activities (for Close-out phase)
CREATE TABLE EDC_closure_activities (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    study_id UUID NOT NULL REFERENCES EDC_studies(id),
    activity_name VARCHAR(200) NOT NULL,
    description TEXT,
    status VARCHAR(20) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED')),
    completion_percentage INTEGER DEFAULT 0,
    assigned_to UUID REFERENCES EDC_users(id),
    due_date DATE,
    completed_date DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Insert initial data
INSERT INTO EDC_users (username, email, password_hash, first_name, last_name, role, phase, permissions) VALUES
('startup_admin', 'startup@edc.com', '$2b$10$hashedpassword1', 'Sarah', 'Johnson', 'Study Designer', 'START_UP', 
 '{"createStudies": true, "designForms": true, "setupSites": true, "configureWorkflows": true, "viewReports": true, "manageUsers": false}'),
('conduct_crc', 'conduct@edc.com', '$2b$10$hashedpassword2', 'Michael', 'Chen', 'Clinical Research Coordinator', 'CONDUCT', 
 '{"enrollSubjects": true, "enterData": true, "manageQueries": true, "viewReports": true, "exportData": false, "lockDatabase": false}'),
('closeout_manager', 'closeout@edc.com', '$2b$10$hashedpassword3', 'Emily', 'Rodriguez', 'Data Manager', 'CLOSE_OUT', 
 '{"closeStudy": true, "exportData": true, "generateReports": true, "verifyData": true, "lockDatabase": true, "archiveData": true}');

-- Insert study data
INSERT INTO EDC_studies (protocol_number, title, description, phase, status, sponsor, indication, projected_enrollment, current_enrollment, primary_investigator, created_by) VALUES
('PROTO-2024-001', 'Phase III Hypertension Study', 'A randomized, double-blind, placebo-controlled study evaluating the efficacy and safety of investigational drug in patients with hypertension', 'Phase III', 'ACTIVE', 'Pharma Corp Inc', 'Hypertension', 300, 75, 'Dr. James Wilson', (SELECT id FROM EDC_users WHERE username = 'startup_admin')),
('PROTO-2024-002', 'Phase II Diabetes Study', 'A multicenter study to assess the effectiveness of new diabetes treatment', 'Phase II', 'ACTIVE', 'MedTech Solutions', 'Type 2 Diabetes', 150, 45, 'Dr. Lisa Park', (SELECT id FROM EDC_users WHERE username = 'startup_admin'));

-- Insert sites
INSERT INTO EDC_sites (study_id, site_number, name, address, city, country, principal_investigator, contact_email, status, subjects_enrolled, data_complete_percentage) VALUES
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001'), '001', 'City Medical Center', '123 Medical Drive', 'New York', 'USA', 'Dr. Robert Smith', 'contact@citymed.com', 'ACTIVE', 25, 85),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001'), '002', 'Regional Hospital', '456 Health Avenue', 'Los Angeles', 'USA', 'Dr. Maria Garcia', 'info@regionalhospital.com', 'ACTIVE', 30, 78),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001'), '003', 'University Research Center', '789 Research Blvd', 'Chicago', 'USA', 'Dr. David Johnson', 'research@university.edu', 'ACTIVE', 20, 92),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-002'), '001', 'Diabetes Clinic', '321 Care Street', 'Boston', 'USA', 'Dr. Jennifer Brown', 'care@diabetesclinic.com', 'ACTIVE', 25, 88),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-002'), '002', 'Metro Health Institute', '654 Wellness Road', 'Seattle', 'USA', 'Dr. Thomas Lee', 'info@metrohealth.com', 'ACTIVE', 20, 82);

-- Insert form templates
INSERT INTO EDC_form_templates (name, description, category, version, status, created_by) VALUES
('Demographics Form', 'Standard demographic data collection', 'ICH-GCP', '1.0.0', 'PRODUCTION', (SELECT id FROM EDC_users WHERE username = 'startup_admin')),
('Adverse Events Form', 'Comprehensive AE reporting with MedDRA coding', 'Safety', '1.2.0', 'PRODUCTION', (SELECT id FROM EDC_users WHERE username = 'startup_admin')),
('Vital Signs Form', 'Clinical vital signs assessment', 'Clinical', '1.1.0', 'PRODUCTION', (SELECT id FROM EDC_users WHERE username = 'startup_admin')),
('Laboratory Values', 'Clinical laboratory test results', 'Labs', '1.0.0', 'PRODUCTION', (SELECT id FROM EDC_users WHERE username = 'startup_admin')),
('Medical History', 'Patient medical history collection', 'Clinical', '1.0.0', 'PRODUCTION', (SELECT id FROM EDC_users WHERE username = 'startup_admin'));

-- Insert form sections
INSERT INTO EDC_form_sections (form_template_id, title, description, category, order_index) VALUES
((SELECT id FROM EDC_form_templates WHERE name = 'Demographics Form'), 'Subject Information', 'Basic subject demographics', 'demographics', 1),
((SELECT id FROM EDC_form_templates WHERE name = 'Vital Signs Form'), 'Vital Signs', 'Standard vital signs measurements', 'clinical', 1),
((SELECT id FROM EDC_form_templates WHERE name = 'Adverse Events Form'), 'Adverse Event Details', 'AE reporting information', 'safety', 1),
((SELECT id FROM EDC_form_templates WHERE name = 'Laboratory Values'), 'Hematology', 'Blood count parameters', 'lab', 1),
((SELECT id FROM EDC_form_templates WHERE name = 'Laboratory Values'), 'Chemistry', 'Blood chemistry panel', 'lab', 2);

-- Insert form fields
INSERT INTO EDC_form_fields (section_id, field_type, label, name, is_required, validation_rules, units, normal_range, cdisc_variable, criticality, audit_required, order_index) VALUES
-- Demographics fields
((SELECT id FROM EDC_form_sections WHERE title = 'Subject Information'), 'text', 'Subject Initials', 'subjectInitials', true, '{"maxLength": 3, "pattern": "^[A-Z]{2,3}$"}', null, null, 'USUBJID', 'critical', true, 1),
((SELECT id FROM EDC_form_sections WHERE title = 'Subject Information'), 'date', 'Date of Birth', 'dateOfBirth', true, '{}', null, null, 'BRTHDTC', 'high', true, 2),
((SELECT id FROM EDC_form_sections WHERE title = 'Subject Information'), 'select', 'Sex', 'sex', true, '{}', null, null, 'SEX', 'high', false, 3),
-- Vital Signs fields
((SELECT id FROM EDC_form_sections WHERE title = 'Vital Signs'), 'number', 'Temperature', 'temperature', true, '{"min": 35, "max": 42}', '°C', '36.1-37.2', 'VSTEMP', 'medium', false, 1),
((SELECT id FROM EDC_form_sections WHERE title = 'Vital Signs'), 'number', 'Systolic Blood Pressure', 'sysBP', true, '{"min": 70, "max": 200}', 'mmHg', '<120', 'VSSBP', 'medium', false, 2),
((SELECT id FROM EDC_form_sections WHERE title = 'Vital Signs'), 'number', 'Diastolic Blood Pressure', 'diaBP', true, '{"min": 40, "max": 120}', 'mmHg', '<80', 'VSDBP', 'medium', false, 3),
((SELECT id FROM EDC_form_sections WHERE title = 'Vital Signs'), 'number', 'Heart Rate', 'heartRate', true, '{"min": 40, "max": 150}', 'bpm', '60-100', 'VSPULSE', 'medium', false, 4),
-- Adverse Event fields
((SELECT id FROM EDC_form_sections WHERE title = 'Adverse Event Details'), 'text', 'Event Term', 'eventTerm', true, '{}', null, null, 'AETERM', 'high', true, 1),
((SELECT id FROM EDC_form_sections WHERE title = 'Adverse Event Details'), 'select', 'Severity', 'severity', true, '{}', null, null, 'AESEV', 'high', true, 2),
((SELECT id FROM EDC_form_sections WHERE title = 'Adverse Event Details'), 'date', 'Start Date', 'startDate', true, '{}', null, null, 'AESTDTC', 'critical', true, 3);

-- Update field options for select fields
UPDATE EDC_form_fields SET field_options = '{"options": ["Male", "Female"]}' WHERE name = 'sex';
UPDATE EDC_form_fields SET field_options = '{"options": ["Mild", "Moderate", "Severe"]}' WHERE name = 'severity';

-- Insert subjects
INSERT INTO EDC_subjects (study_id, site_id, subject_number, subject_initials, status, enrollment_date, treatment_arm, visits_completed, visits_pending) VALUES
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001'), (SELECT id FROM EDC_sites WHERE site_number = '001' AND study_id = (SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001')), 'S001-001', 'AB', 'ACTIVE', '2024-01-15', 'Treatment A', 3, 2),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001'), (SELECT id FROM EDC_sites WHERE site_number = '001' AND study_id = (SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001')), 'S001-002', 'CD', 'ACTIVE', '2024-01-20', 'Placebo', 4, 1),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001'), (SELECT id FROM EDC_sites WHERE site_number = '002' AND study_id = (SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001')), 'S002-001', 'EF', 'COMPLETED', '2024-01-10', 'Treatment A', 5, 0),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-002'), (SELECT id FROM EDC_sites WHERE site_number = '001' AND study_id = (SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-002')), 'S001-001', 'GH', 'ACTIVE', '2024-02-01', 'Treatment B', 2, 3);

-- Insert study metrics
INSERT INTO EDC_study_metrics (study_id, metric_name, metric_value, metric_type) VALUES
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001'), 'Total Subjects Enrolled', 75, 'enrollment'),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001'), 'Data Completion Rate', 85.5, 'completion'),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001'), 'Open Queries', 12, 'queries'),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001'), 'Sites Active', 3, 'sites'),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-002'), 'Total Subjects Enrolled', 45, 'enrollment'),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-002'), 'Data Completion Rate', 78.2, 'completion'),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-002'), 'Open Queries', 8, 'queries'),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-002'), 'Sites Active', 2, 'sites');

-- Insert data verification checks
INSERT INTO EDC_data_verification (study_id, phase, check_name, check_description, status, details) VALUES
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001'), 'START_UP', 'Protocol Setup', 'Study protocol configuration verified', 'PASSED', 'All protocol parameters configured correctly'),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001'), 'START_UP', 'Form Design', 'CRF forms designed and validated', 'PASSED', 'All 5 forms completed and validated'),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001'), 'CONDUCT', 'Data Entry', 'Subject data entry validation', 'PASSED', '85% data completion rate achieved'),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001'), 'CONDUCT', 'Query Resolution', 'Open query resolution status', 'WARNING', '12 queries pending resolution'),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-002'), 'CLOSE_OUT', 'Database Lock', 'Final database lock preparation', 'PENDING', 'Awaiting final data verification');

-- Insert closure activities
INSERT INTO EDC_closure_activities (study_id, activity_name, description, status, completion_percentage, due_date) VALUES
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001'), 'Final Data Review', 'Complete review of all subject data', 'IN_PROGRESS', 75, '2024-12-31'),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001'), 'Query Resolution', 'Resolve all outstanding queries', 'IN_PROGRESS', 85, '2024-12-15'),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001'), 'Database Lock', 'Lock study database', 'PENDING', 0, '2025-01-15'),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-001'), 'Final Report Generation', 'Generate clinical study report', 'PENDING', 0, '2025-02-28'),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-002'), 'Data Export', 'Export final datasets', 'COMPLETED', 100, '2024-11-30'),
((SELECT id FROM EDC_studies WHERE protocol_number = 'PROTO-2024-002'), 'Archive Preparation', 'Prepare study documents for archiving', 'IN_PROGRESS', 60, '2024-12-31');

-- Create indexes for performance
CREATE INDEX idx_edc_users_username ON EDC_users(username);
CREATE INDEX idx_edc_users_phase ON EDC_users(phase);
CREATE INDEX idx_edc_studies_protocol ON EDC_studies(protocol_number);
CREATE INDEX idx_edc_subjects_study_site ON EDC_subjects(study_id, site_id);
CREATE INDEX idx_edc_form_data_subject ON EDC_form_data(subject_id);
CREATE INDEX idx_edc_audit_trail_table_record ON EDC_audit_trail(table_name, record_id);
CREATE INDEX idx_edc_queries_status ON EDC_queries(status);

-- Create RLS policies (Row Level Security) - Optional but recommended
ALTER TABLE EDC_users ENABLE ROW LEVEL SECURITY;
ALTER TABLE EDC_studies ENABLE ROW LEVEL SECURITY;
ALTER TABLE EDC_form_data ENABLE ROW LEVEL SECURITY;

-- Create functions for updated_at triggers
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at
CREATE TRIGGER update_edc_users_updated_at BEFORE UPDATE ON EDC_users FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_edc_studies_updated_at BEFORE UPDATE ON EDC_studies FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_edc_sites_updated_at BEFORE UPDATE ON EDC_sites FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_edc_subjects_updated_at BEFORE UPDATE ON EDC_subjects FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_edc_form_data_updated_at BEFORE UPDATE ON EDC_form_data FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_edc_queries_updated_at BEFORE UPDATE ON EDC_queries FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions (adjust as needed)
-- GRANT USAGE ON SCHEMA public TO anon, authenticated;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO authenticated;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO anon;

COMMENT ON SCHEMA public IS 'EDC Clinical Trials System Database Schema';