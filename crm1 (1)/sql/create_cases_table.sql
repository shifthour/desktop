-- Cases table for tracking issues and problems identified in the support system
-- Cases represent any issues that were identified and need to be documented for future reference

CREATE TABLE IF NOT EXISTS cases (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    case_number VARCHAR(50) UNIQUE NOT NULL,
    case_date DATE NOT NULL,
    title VARCHAR(500) NOT NULL,
    description TEXT NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    contact_person VARCHAR(255),
    contact_phone VARCHAR(20),
    contact_email VARCHAR(255),
    product_name VARCHAR(500),
    serial_number VARCHAR(100),
    model_number VARCHAR(100),
    case_category VARCHAR(100) NOT NULL,
    issue_type VARCHAR(100) NOT NULL,
    severity VARCHAR(20) DEFAULT 'Medium',
    priority VARCHAR(20) DEFAULT 'Medium',
    status VARCHAR(50) DEFAULT 'Open',
    environment_details TEXT,
    error_messages TEXT,
    reproduction_steps TEXT,
    customer_impact TEXT,
    business_impact VARCHAR(100),
    reported_by VARCHAR(255),
    assigned_to VARCHAR(255),
    escalated_to VARCHAR(255),
    escalation_date DATE,
    escalation_reason TEXT,
    tags JSONB,
    attachments JSONB,
    related_products JSONB,
    customer_environment TEXT,
    version_info VARCHAR(255),
    configuration_details TEXT,
    symptoms TEXT,
    frequency VARCHAR(50),
    first_occurrence DATE,
    last_occurrence DATE,
    affected_users INTEGER,
    workaround TEXT,
    internal_notes TEXT,
    customer_notes TEXT,
    resolution_required BOOLEAN DEFAULT TRUE,
    follow_up_required BOOLEAN DEFAULT FALSE,
    follow_up_date DATE,
    closed_date DATE,
    closed_by VARCHAR(255),
    closure_reason TEXT,
    region VARCHAR(100),
    site_details TEXT,
    company_id UUID NOT NULL,
    created_by UUID,
    updated_by UUID,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add indexes for better performance
CREATE INDEX IF NOT EXISTS idx_cases_company_id ON cases(company_id);
CREATE INDEX IF NOT EXISTS idx_cases_status ON cases(status);
CREATE INDEX IF NOT EXISTS idx_cases_assigned_to ON cases(assigned_to);
CREATE INDEX IF NOT EXISTS idx_cases_case_date ON cases(case_date);
CREATE INDEX IF NOT EXISTS idx_cases_severity ON cases(severity);
CREATE INDEX IF NOT EXISTS idx_cases_priority ON cases(priority);
CREATE INDEX IF NOT EXISTS idx_cases_customer_name ON cases(customer_name);
CREATE INDEX IF NOT EXISTS idx_cases_product_name ON cases(product_name);
CREATE INDEX IF NOT EXISTS idx_cases_serial_number ON cases(serial_number);
CREATE INDEX IF NOT EXISTS idx_cases_case_category ON cases(case_category);
CREATE INDEX IF NOT EXISTS idx_cases_issue_type ON cases(issue_type);

-- Add check constraints
ALTER TABLE cases ADD CONSTRAINT cases_status_check 
    CHECK (status IN ('Open', 'In Progress', 'Under Investigation', 'Pending Customer', 'Pending Internal', 'Resolved', 'Closed', 'Escalated', 'Cancelled'));

ALTER TABLE cases ADD CONSTRAINT cases_severity_check 
    CHECK (severity IN ('Low', 'Medium', 'High', 'Critical'));

ALTER TABLE cases ADD CONSTRAINT cases_priority_check 
    CHECK (priority IN ('Low', 'Medium', 'High', 'Critical'));

ALTER TABLE cases ADD CONSTRAINT cases_issue_type_check 
    CHECK (issue_type IN ('Software Bug', 'Hardware Issue', 'Performance Problem', 'Configuration Issue', 'User Error', 'Training Need', 'Feature Request', 'Documentation Issue', 'Compatibility Issue', 'Installation Problem', 'Network Issue', 'Security Issue', 'Data Issue', 'Integration Problem', 'Other'));

ALTER TABLE cases ADD CONSTRAINT cases_business_impact_check 
    CHECK (business_impact IN ('No Impact', 'Low Impact', 'Medium Impact', 'High Impact', 'Critical Impact'));

ALTER TABLE cases ADD CONSTRAINT cases_frequency_check 
    CHECK (frequency IN ('One Time', 'Occasional', 'Frequent', 'Constant', 'Unknown'));

-- Sample data for testing
INSERT INTO cases (
    case_number, case_date, title, description, customer_name, contact_person, 
    contact_phone, product_name, serial_number, case_category, issue_type, 
    severity, priority, status, symptoms, assigned_to, company_id
) VALUES 
(
    'CASE-2025-001', '2025-01-27', 'Fibrinometer calibration showing inconsistent readings',
    'Equipment showing inconsistent readings after recent software update. Calibration values drift during operation causing unreliable test results.',
    'Kerala Agricultural University', 'Dr. Priya Sharma', '+91-9876543213', 
    'Fibrinometer Model FG-2000', 'FG-2000-001', 'Technical Support', 'Software Bug',
    'High', 'High', 'Open', 'Calibration drift, error codes E001 and E003, inconsistent readings',
    'Rajesh Kumar', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'CASE-2025-002', '2025-01-25', 'Missing accessories in centrifuge shipment',
    'Customer received centrifuge but rotor accessories and safety lid were missing from the shipment. This is preventing equipment installation and commissioning.',
    'Eurofins Advinus', 'Mr. Mahesh Patel', '+91-9876543214', 
    'High-Speed Centrifuge HSC-3000', 'HSC-3000-002', 'Delivery Issue', 'Installation Problem',
    'Medium', 'High', 'In Progress', 'Missing rotor set, safety lid, and user manual',
    'Anjali Menon', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'CASE-2025-003', '2025-01-20', 'Software license activation failing for premium features',
    'Customer upgraded to premium license but unable to activate advanced features. License server showing authentication errors.',
    'JNCASR', 'Dr. Anu Rang', '+91-9876543215', 
    'SpectroPro Analysis Software', 'SP-SOFT-001', 'Software Issue', 'Configuration Issue',
    'High', 'Medium', 'Escalated', 'Error: License authentication failed, premium features disabled',
    'Technical Team', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'CASE-2025-004', '2025-01-18', 'Microscope objective lens manufacturing defect',
    'Newly delivered microscope objective lens showing optical distortion and poor image quality. Appears to be manufacturing defect.',
    'Bio-Rad Laboratories', 'Mr. Sanjay Patel', '+91-9876543216', 
    'High-Resolution Microscope Objective 100x', 'OBJ-100X-001', 'Warranty', 'Hardware Issue',
    'Medium', 'Medium', 'Under Investigation', 'Optical distortion, blurry images, poor resolution',
    'Warranty Team', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'CASE-2025-005', '2025-01-15', 'Training request for new laboratory staff',
    'Customer hired 5 new lab technicians and requires comprehensive training on equipment operation and safety procedures.',
    'TSAR Labcare', 'Ms. Pauline D''Souza', '+91-9876543217', 
    'Complete Laboratory Setup', 'LAB-SETUP-001', 'Training', 'Training Need',
    'Low', 'Medium', 'Resolved', 'New staff need equipment training and safety certification',
    'Sanjay Verma', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
);

-- Update trigger for updated_at
CREATE OR REPLACE FUNCTION update_cases_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_cases_updated_at_trigger
    BEFORE UPDATE ON cases
    FOR EACH ROW
    EXECUTE FUNCTION update_cases_updated_at();

COMMENT ON TABLE cases IS 'Cases for tracking issues and problems identified in support';
COMMENT ON COLUMN cases.case_number IS 'Unique case tracking number';
COMMENT ON COLUMN cases.issue_type IS 'Type of issue identified';
COMMENT ON COLUMN cases.customer_impact IS 'Description of impact on customer operations';
COMMENT ON COLUMN cases.business_impact IS 'Business impact level of the issue';
COMMENT ON COLUMN cases.symptoms IS 'Observable symptoms of the issue';
COMMENT ON COLUMN cases.reproduction_steps IS 'Steps to reproduce the issue';
COMMENT ON COLUMN cases.workaround IS 'Temporary workaround for the issue';