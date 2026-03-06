-- Complaints table for tracking customer complaints and service requests
-- This table links to installations, amc_contracts, and sales_orders

CREATE TABLE IF NOT EXISTS complaints (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    complaint_number VARCHAR(50) UNIQUE NOT NULL,
    complaint_date DATE NOT NULL,
    sales_order_id UUID REFERENCES sales_orders(id),
    sales_order_number VARCHAR(100),
    installation_id UUID REFERENCES installations(id),
    amc_contract_id UUID REFERENCES amc_contracts(id),
    account_name VARCHAR(255) NOT NULL,
    contact_person VARCHAR(255),
    contact_phone VARCHAR(20),
    contact_email VARCHAR(255),
    product_name VARCHAR(500) NOT NULL,
    serial_number VARCHAR(100),
    model_number VARCHAR(100),
    complaint_type VARCHAR(100) NOT NULL,
    complaint_category VARCHAR(100),
    severity VARCHAR(20) DEFAULT 'Medium',
    priority VARCHAR(20) DEFAULT 'Medium',
    status VARCHAR(50) DEFAULT 'New',
    subject VARCHAR(500),
    description TEXT NOT NULL,
    root_cause TEXT,
    resolution_details TEXT,
    resolution_date DATE,
    resolution_time_hours INTEGER,
    site_details TEXT,
    service_address TEXT,
    assigned_to VARCHAR(255),
    service_engineer VARCHAR(255),
    escalated_to VARCHAR(255),
    escalation_date DATE,
    escalation_reason TEXT,
    customer_satisfaction_rating INTEGER CHECK (customer_satisfaction_rating >= 1 AND customer_satisfaction_rating <= 5),
    customer_feedback TEXT,
    internal_notes TEXT,
    follow_up_required BOOLEAN DEFAULT FALSE,
    follow_up_date DATE,
    warranty_claim BOOLEAN DEFAULT FALSE,
    amc_covered BOOLEAN DEFAULT FALSE,
    chargeable BOOLEAN DEFAULT FALSE,
    service_charge DECIMAL(10,2) DEFAULT 0,
    parts_used JSONB,
    service_report TEXT,
    photos JSONB,
    attachments JSONB,
    closed_date DATE,
    closed_by VARCHAR(255),
    closure_notes TEXT,
    region VARCHAR(100),
    company_id UUID NOT NULL,
    created_by UUID,
    updated_by UUID,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add indexes for better performance
CREATE INDEX IF NOT EXISTS idx_complaints_company_id ON complaints(company_id);
CREATE INDEX IF NOT EXISTS idx_complaints_sales_order_id ON complaints(sales_order_id);
CREATE INDEX IF NOT EXISTS idx_complaints_installation_id ON complaints(installation_id);
CREATE INDEX IF NOT EXISTS idx_complaints_amc_contract_id ON complaints(amc_contract_id);
CREATE INDEX IF NOT EXISTS idx_complaints_status ON complaints(status);
CREATE INDEX IF NOT EXISTS idx_complaints_assigned_to ON complaints(assigned_to);
CREATE INDEX IF NOT EXISTS idx_complaints_complaint_date ON complaints(complaint_date);
CREATE INDEX IF NOT EXISTS idx_complaints_severity ON complaints(severity);
CREATE INDEX IF NOT EXISTS idx_complaints_serial_number ON complaints(serial_number);

-- Add check constraints
ALTER TABLE complaints ADD CONSTRAINT complaints_status_check 
    CHECK (status IN ('New', 'Acknowledged', 'In Progress', 'Pending Parts', 'Resolved', 'Closed', 'Escalated', 'Cancelled'));

ALTER TABLE complaints ADD CONSTRAINT complaints_severity_check 
    CHECK (severity IN ('Low', 'Medium', 'High', 'Critical'));

ALTER TABLE complaints ADD CONSTRAINT complaints_priority_check 
    CHECK (priority IN ('Low', 'Medium', 'High', 'Critical'));

ALTER TABLE complaints ADD CONSTRAINT complaints_complaint_type_check 
    CHECK (complaint_type IN ('Under Warranty', 'Under AMC', 'No Warranty/AMC', 'Preventive Maintenance', 'Training Request', 'Spare Parts', 'Technical Support'));

-- Sample data for testing
INSERT INTO complaints (
    complaint_number, complaint_date, account_name, contact_person, 
    contact_phone, product_name, serial_number, complaint_type, 
    severity, status, subject, description, assigned_to, 
    priority, warranty_claim, amc_covered, company_id
) VALUES 
(
    '2025-001', '2025-01-27', 'JNCASR',
    'Dr. Anu Rang', '+91-9876543213', 'ND 1000 Spectrophotometer', 'ND-1000-001',
    'No Warranty/AMC', 'High', 'In Progress', 'Equipment not functioning properly',
    'Equipment showing error codes and requires immediate attention for critical research work', 
    'Hari Kumar K', 'High', FALSE, FALSE, 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    '2025-002', '2025-01-20', 'NIIVEDI',
    'Lab Manager', '+91-9876543214', 'VERTICAL AUTOCLAVE (BSE-A65)', 'BSE-A65-002',
    'Under Warranty', 'High', 'Resolved', 'Autoclave door seal issue',
    'Steam leakage observed from door seal, affecting sterilization efficiency', 
    'Hari Kumar K', 'High', TRUE, FALSE, 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    '2025-003', '2025-01-13', 'Sri Devaraj Urs Medical College',
    'Dr. Dayanand', '+91-9876543215', '540NM filter for Elisa Reader', 'ELISA-540-001',
    'No Warranty/AMC', 'Medium', 'Closed', 'Filter replacement required',
    'Filter replacement required for proper functioning of Elisa Reader', 
    'Hari Kumar K', 'Medium', FALSE, FALSE, 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    '2025-004', '2025-01-13', 'Christian Medical College',
    'Mr. Dwarkesh', '+91-9876543216', 'Laboratory Equipment', 'LAB-EQ-001',
    'Under AMC', 'Low', 'Closed', 'Routine maintenance request',
    'Annual maintenance contract service request for laboratory equipment', 
    'Hari Kumar K', 'Low', FALSE, TRUE, 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
);

-- Update trigger for updated_at
CREATE OR REPLACE FUNCTION update_complaints_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_complaints_updated_at_trigger
    BEFORE UPDATE ON complaints
    FOR EACH ROW
    EXECUTE FUNCTION update_complaints_updated_at();

COMMENT ON TABLE complaints IS 'Customer complaints and service requests tracking';
COMMENT ON COLUMN complaints.complaint_number IS 'Unique complaint tracking number';
COMMENT ON COLUMN complaints.resolution_time_hours IS 'Time taken to resolve complaint in hours';
COMMENT ON COLUMN complaints.warranty_claim IS 'Whether this is a warranty claim';
COMMENT ON COLUMN complaints.amc_covered IS 'Whether this complaint is covered under AMC';
COMMENT ON COLUMN complaints.chargeable IS 'Whether service charges apply';