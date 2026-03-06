-- Enhanced EDC Database Schema for New Features
-- This script creates the additional tables needed for the enhanced functionality

-- UAT (User Acceptance Testing) Workspaces
CREATE TABLE IF NOT EXISTS EDC_uat_workspaces (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    form_id UUID REFERENCES EDC_form_templates(id),
    study_id UUID REFERENCES EDC_studies(id),
    form_version TEXT NOT NULL,
    status VARCHAR(20) DEFAULT 'in_uat' CHECK (status IN ('draft', 'in_uat', 'uat_passed', 'uat_failed', 'production')),
    test_plan JSONB,
    metadata JSONB DEFAULT '{}',
    created_by UUID REFERENCES EDC_users(id),
    created_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    promoted_by UUID REFERENCES EDC_users(id),
    promoted_date TIMESTAMPTZ,
    promotion_notes TEXT,
    e_signature JSONB
);

-- UAT Test Cases
CREATE TABLE IF NOT EXISTS EDC_uat_test_cases (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    workspace_id UUID REFERENCES EDC_uat_workspaces(id),
    field_id UUID REFERENCES EDC_form_fields(id),
    test_type VARCHAR(50) NOT NULL,
    test_name VARCHAR(255) NOT NULL,
    test_description TEXT,
    expected_result TEXT,
    actual_result TEXT,
    test_data JSONB,
    status VARCHAR(20) DEFAULT 'not_started' CHECK (status IN ('not_started', 'in_progress', 'passed', 'failed', 'blocked')),
    test_notes TEXT,
    executed_by UUID REFERENCES EDC_users(id),
    executed_date TIMESTAMPTZ,
    execution_data JSONB,
    created_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- UAT Defects
CREATE TABLE IF NOT EXISTS EDC_uat_defects (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    workspace_id UUID REFERENCES EDC_uat_workspaces(id),
    test_case_id UUID REFERENCES EDC_uat_test_cases(id),
    defect_title VARCHAR(255) NOT NULL,
    defect_description TEXT NOT NULL,
    severity VARCHAR(20) DEFAULT 'medium' CHECK (severity IN ('critical', 'major', 'minor', 'cosmetic')),
    status VARCHAR(20) DEFAULT 'open' CHECK (status IN ('open', 'in_progress', 'fixed', 're_tested', 'closed', 'rejected')),
    steps_to_reproduce TEXT,
    attachments JSONB,
    resolution TEXT,
    fix_notes TEXT,
    reported_by UUID REFERENCES EDC_users(id),
    reported_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    assigned_to UUID REFERENCES EDC_users(id),
    fixed_by UUID REFERENCES EDC_users(id),
    fixed_date TIMESTAMPTZ,
    closed_by UUID REFERENCES EDC_users(id),
    closed_date TIMESTAMPTZ,
    updated_by UUID REFERENCES EDC_users(id),
    updated_date TIMESTAMPTZ
);

-- Validation Rules (Enhanced)
CREATE TABLE IF NOT EXISTS EDC_validation_rules (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    form_id UUID REFERENCES EDC_form_templates(id),
    field_id UUID REFERENCES EDC_form_fields(id),
    rule_type VARCHAR(50) NOT NULL,
    rule_name VARCHAR(255) NOT NULL,
    condition TEXT NOT NULL,
    parameters JSONB DEFAULT '{}',
    severity VARCHAR(20) DEFAULT 'error' CHECK (severity IN ('error', 'warning', 'note')),
    message TEXT NOT NULL,
    active BOOLEAN DEFAULT true,
    created_by UUID REFERENCES EDC_users(id),
    created_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_by UUID REFERENCES EDC_users(id),
    updated_date TIMESTAMPTZ
);

-- Enhanced Queries Table (if not exists)
CREATE TABLE IF NOT EXISTS EDC_queries (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    study_id UUID REFERENCES EDC_studies(id),
    site_id UUID REFERENCES EDC_sites(id),
    subject_id UUID REFERENCES EDC_subjects(id),
    visit_id UUID REFERENCES EDC_visits(id),
    form_id UUID REFERENCES EDC_form_templates(id),
    field_id UUID REFERENCES EDC_form_fields(id),
    query_type VARCHAR(50) DEFAULT 'manual' CHECK (query_type IN ('manual', 'auto', 'pre_submission', 'post_submission')),
    severity VARCHAR(20) DEFAULT 'major' CHECK (severity IN ('critical', 'major', 'minor', 'note')),
    status VARCHAR(20) DEFAULT 'open' CHECK (status IN ('open', 'assigned', 'answered', 'reviewed', 'resolved', 'closed', 'accepted_as_valid')),
    query_text TEXT NOT NULL,
    response_text TEXT,
    justification TEXT,
    value_questioned TEXT,
    expected_value TEXT,
    metadata JSONB DEFAULT '{}',
    raised_by UUID REFERENCES EDC_users(id),
    raised_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    assigned_to UUID REFERENCES EDC_users(id),
    responded_by UUID REFERENCES EDC_users(id),
    responded_date TIMESTAMPTZ,
    reviewed_by UUID REFERENCES EDC_users(id),
    reviewed_date TIMESTAMPTZ,
    closed_by UUID REFERENCES EDC_users(id),
    closed_date TIMESTAMPTZ,
    updated_by UUID REFERENCES EDC_users(id),
    updated_date TIMESTAMPTZ,
    sla_due_date TIMESTAMPTZ,
    priority VARCHAR(20) DEFAULT 'medium' CHECK (priority IN ('low', 'medium', 'high', 'urgent'))
);

-- Enhanced Audit Trail (add missing columns if table exists)
DO $$ 
BEGIN 
    -- Add checksum column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'edc_audit_trail' AND column_name = 'checksum') THEN
        ALTER TABLE EDC_audit_trail ADD COLUMN checksum VARCHAR(64);
    END IF;
    
    -- Add data category column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'edc_audit_trail' AND column_name = 'data_category') THEN
        ALTER TABLE EDC_audit_trail ADD COLUMN data_category VARCHAR(50) DEFAULT 'form_data';
    END IF;
    
    -- Add risk level column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'edc_audit_trail' AND column_name = 'risk_level') THEN
        ALTER TABLE EDC_audit_trail ADD COLUMN risk_level VARCHAR(20) DEFAULT 'low' CHECK (risk_level IN ('low', 'medium', 'high', 'critical'));
    END IF;
    
    -- Add metadata column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'edc_audit_trail' AND column_name = 'metadata') THEN
        ALTER TABLE EDC_audit_trail ADD COLUMN metadata JSONB DEFAULT '{}';
    END IF;
    
    -- Add user agent column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'edc_audit_trail' AND column_name = 'user_agent') THEN
        ALTER TABLE EDC_audit_trail ADD COLUMN user_agent TEXT;
    END IF;
    
    -- Add session ID column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'edc_audit_trail' AND column_name = 'session_id') THEN
        ALTER TABLE EDC_audit_trail ADD COLUMN session_id VARCHAR(255);
    END IF;
    
    -- Add study/site/subject references if they don't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'edc_audit_trail' AND column_name = 'study_id') THEN
        ALTER TABLE EDC_audit_trail ADD COLUMN study_id UUID REFERENCES EDC_studies(id);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'edc_audit_trail' AND column_name = 'site_id') THEN
        ALTER TABLE EDC_audit_trail ADD COLUMN site_id UUID REFERENCES EDC_sites(id);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'edc_audit_trail' AND column_name = 'subject_id') THEN
        ALTER TABLE EDC_audit_trail ADD COLUMN subject_id UUID REFERENCES EDC_subjects(id);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'edc_audit_trail' AND column_name = 'e_signature') THEN
        ALTER TABLE EDC_audit_trail ADD COLUMN e_signature TEXT;
    END IF;
END $$;

-- Study Metrics for Enhanced Reporting
CREATE TABLE IF NOT EXISTS EDC_study_metrics_enhanced (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    study_id UUID REFERENCES EDC_studies(id),
    metric_date DATE DEFAULT CURRENT_DATE,
    total_subjects INTEGER DEFAULT 0,
    enrolled_subjects INTEGER DEFAULT 0,
    completed_subjects INTEGER DEFAULT 0,
    withdrawn_subjects INTEGER DEFAULT 0,
    total_visits INTEGER DEFAULT 0,
    completed_visits INTEGER DEFAULT 0,
    missed_visits INTEGER DEFAULT 0,
    total_forms INTEGER DEFAULT 0,
    completed_forms INTEGER DEFAULT 0,
    forms_with_queries INTEGER DEFAULT 0,
    open_queries INTEGER DEFAULT 0,
    closed_queries INTEGER DEFAULT 0,
    sla_breached_queries INTEGER DEFAULT 0,
    data_entry_completion_rate DECIMAL(5,2) DEFAULT 0,
    query_resolution_rate DECIMAL(5,2) DEFAULT 0,
    avg_query_resolution_days DECIMAL(5,2) DEFAULT 0,
    created_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Form Field Validation Parameters (Enhanced)
DO $$ 
BEGIN 
    -- Add validation parameters column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'edc_form_fields' AND column_name = 'validation_params') THEN
        ALTER TABLE EDC_form_fields ADD COLUMN validation_params JSONB DEFAULT '{}';
    END IF;
    
    -- Add cross field rules column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'edc_form_fields' AND column_name = 'cross_field_rules') THEN
        ALTER TABLE EDC_form_fields ADD COLUMN cross_field_rules JSONB DEFAULT '[]';
    END IF;
    
    -- Add pattern message column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'edc_form_fields' AND column_name = 'pattern_message') THEN
        ALTER TABLE EDC_form_fields ADD COLUMN pattern_message TEXT;
    END IF;
    
    -- Add category column for field grouping
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'edc_form_fields' AND column_name = 'category') THEN
        ALTER TABLE EDC_form_fields ADD COLUMN category VARCHAR(50);
    END IF;
    
    -- Add valid/invalid examples for validation testing
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'edc_form_fields' AND column_name = 'valid_examples') THEN
        ALTER TABLE EDC_form_fields ADD COLUMN valid_examples JSONB DEFAULT '[]';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'edc_form_fields' AND column_name = 'invalid_examples') THEN
        ALTER TABLE EDC_form_fields ADD COLUMN invalid_examples JSONB DEFAULT '[]';
    END IF;
END $$;

-- Update Users table for enhanced roles
DO $$ 
BEGIN 
    -- Add phase column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'edc_users' AND column_name = 'phase') THEN
        ALTER TABLE EDC_users ADD COLUMN phase VARCHAR(20);
    END IF;
    
    -- Update existing users to have proper phases
    UPDATE EDC_users SET phase = 'setup' WHERE username = 'admin';
    UPDATE EDC_users SET phase = 'conduct' WHERE username IN ('doctor', 'data_manager');
    UPDATE EDC_users SET phase = 'setup' WHERE username = 'startup_admin';
    UPDATE EDC_users SET phase = 'conduct' WHERE username IN ('conduct_crc', 'closeout_manager');
END $$;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_uat_workspaces_form_id ON EDC_uat_workspaces(form_id);
CREATE INDEX IF NOT EXISTS idx_uat_workspaces_status ON EDC_uat_workspaces(status);
CREATE INDEX IF NOT EXISTS idx_uat_test_cases_workspace_id ON EDC_uat_test_cases(workspace_id);
CREATE INDEX IF NOT EXISTS idx_uat_defects_workspace_id ON EDC_uat_defects(workspace_id);
CREATE INDEX IF NOT EXISTS idx_uat_defects_status ON EDC_uat_defects(status);
CREATE INDEX IF NOT EXISTS idx_queries_study_id ON EDC_queries(study_id);
CREATE INDEX IF NOT EXISTS idx_queries_status ON EDC_queries(status);
CREATE INDEX IF NOT EXISTS idx_queries_severity ON EDC_queries(severity);
CREATE INDEX IF NOT EXISTS idx_queries_raised_date ON EDC_queries(raised_date);
CREATE INDEX IF NOT EXISTS idx_queries_sla_due_date ON EDC_queries(sla_due_date);
CREATE INDEX IF NOT EXISTS idx_validation_rules_form_id ON EDC_validation_rules(form_id);
CREATE INDEX IF NOT EXISTS idx_validation_rules_active ON EDC_validation_rules(active);
CREATE INDEX IF NOT EXISTS idx_audit_trail_checksum ON EDC_audit_trail(checksum);
CREATE INDEX IF NOT EXISTS idx_audit_trail_risk_level ON EDC_audit_trail(risk_level);
CREATE INDEX IF NOT EXISTS idx_audit_trail_study_id ON EDC_audit_trail(study_id);

-- Insert sample validation rules for demo
INSERT INTO EDC_validation_rules (form_id, field_id, rule_type, rule_name, condition, message, created_by) 
SELECT 
    ft.id as form_id,
    ff.id as field_id,
    'range' as rule_type,
    'Temperature Range Check' as rule_name,
    'value >= 34 AND value <= 43' as condition,
    'Body temperature must be between 34°C and 43°C' as message,
    u.id as created_by
FROM EDC_form_templates ft
JOIN EDC_form_fields ff ON ft.id = ff.form_template_id
JOIN EDC_users u ON u.username = 'admin'
WHERE ff.field_name = 'body_temperature'
AND NOT EXISTS (
    SELECT 1 FROM EDC_validation_rules vr 
    WHERE vr.field_id = ff.id AND vr.rule_name = 'Temperature Range Check'
);

INSERT INTO EDC_validation_rules (form_id, field_id, rule_type, rule_name, condition, message, severity, created_by) 
SELECT 
    ft.id as form_id,
    ff.id as field_id,
    'cross_field' as rule_type,
    'Blood Pressure Validation' as rule_name,
    'systolic > diastolic' as condition,
    'Systolic pressure must be greater than diastolic pressure' as message,
    'error' as severity,
    u.id as created_by
FROM EDC_form_templates ft
JOIN EDC_form_fields ff ON ft.id = ff.form_template_id
JOIN EDC_users u ON u.username = 'admin'
WHERE ff.field_name = 'bp_systolic'
AND NOT EXISTS (
    SELECT 1 FROM EDC_validation_rules vr 
    WHERE vr.field_id = ff.id AND vr.rule_name = 'Blood Pressure Validation'
);

-- Insert sample users for new roles if they don't exist
INSERT INTO EDC_users (username, password_hash, first_name, last_name, email, role, phase, status, permissions, created_date)
SELECT 'admin', '$2b$10$hash', 'Study', 'Administrator', 'admin@clinicaltrial.com', 'Study Administrator', 'setup', 'ACTIVE', 
       '["create_studies", "design_forms", "manage_uat", "promote_forms"]', NOW()
WHERE NOT EXISTS (SELECT 1 FROM EDC_users WHERE username = 'admin');

INSERT INTO EDC_users (username, password_hash, first_name, last_name, email, role, phase, status, permissions, created_date)
SELECT 'doctor', '$2b$10$hash', 'Clinical', 'Doctor', 'doctor@hospital.com', 'Doctor (CRC/PI)', 'conduct', 'ACTIVE', 
       '["enroll_subjects", "enter_data", "respond_queries", "view_patient_data", "e_sign_forms"]', NOW()
WHERE NOT EXISTS (SELECT 1 FROM EDC_users WHERE username = 'doctor');

INSERT INTO EDC_users (username, password_hash, first_name, last_name, email, role, phase, status, permissions, created_date)
SELECT 'data_manager', '$2b$10$hash', 'Data', 'Manager', 'dm@cro.com', 'Data Manager', 'conduct', 'ACTIVE', 
       '["review_data", "raise_queries", "monitor_quality", "export_reports", "manage_discrepancies"]', NOW()
WHERE NOT EXISTS (SELECT 1 FROM EDC_users WHERE username = 'data_manager');

-- Create a view for query SLA monitoring
CREATE OR REPLACE VIEW EDC_query_sla_view AS
SELECT 
    q.*,
    EXTRACT(days FROM (COALESCE(q.closed_date, NOW()) - q.raised_date)) as days_open,
    CASE 
        WHEN q.severity = 'critical' THEN 2
        WHEN q.severity = 'major' THEN 5
        WHEN q.severity = 'minor' THEN 10
        ELSE 15
    END as sla_days,
    CASE 
        WHEN q.status = 'closed' THEN 'Closed'
        WHEN EXTRACT(days FROM (NOW() - q.raised_date)) > 
             CASE 
                WHEN q.severity = 'critical' THEN 2
                WHEN q.severity = 'major' THEN 5
                WHEN q.severity = 'minor' THEN 10
                ELSE 15
             END THEN 'SLA Breached'
        WHEN EXTRACT(days FROM (NOW() - q.raised_date)) >= 
             (CASE 
                WHEN q.severity = 'critical' THEN 2
                WHEN q.severity = 'major' THEN 5
                WHEN q.severity = 'minor' THEN 10
                ELSE 15
             END * 0.8) THEN 'SLA Warning'
        ELSE 'On Track'
    END as sla_status
FROM EDC_queries q;

-- Create a summary view for study metrics
CREATE OR REPLACE VIEW EDC_study_summary_view AS
SELECT 
    s.id as study_id,
    s.study_name,
    s.study_code,
    s.status as study_status,
    COUNT(DISTINCT sub.id) as total_subjects,
    COUNT(DISTINCT CASE WHEN sub.status = 'ENROLLED' THEN sub.id END) as enrolled_subjects,
    COUNT(DISTINCT q.id) as total_queries,
    COUNT(DISTINCT CASE WHEN q.status != 'closed' THEN q.id END) as open_queries,
    COUNT(DISTINCT CASE WHEN q.severity = 'critical' THEN q.id END) as critical_queries,
    COALESCE(AVG(EXTRACT(days FROM (q.closed_date - q.raised_date))), 0) as avg_query_resolution_days
FROM EDC_studies s
LEFT JOIN EDC_subjects sub ON s.id = sub.study_id
LEFT JOIN EDC_queries q ON s.id = q.study_id
GROUP BY s.id, s.study_name, s.study_code, s.status;

-- Grant appropriate permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO postgres;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO postgres;
GRANT SELECT ON EDC_query_sla_view TO postgres;
GRANT SELECT ON EDC_study_summary_view TO postgres;