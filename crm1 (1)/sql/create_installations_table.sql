-- Installations table for tracking equipment installations
-- This table links to sales_orders and tracks installation progress

CREATE TABLE IF NOT EXISTS installations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    installation_number VARCHAR(50) UNIQUE NOT NULL,
    installation_date DATE NOT NULL,
    sales_order_id UUID REFERENCES sales_orders(id),
    sales_order_number VARCHAR(100),
    account_name VARCHAR(255) NOT NULL,
    contact_person VARCHAR(255),
    contact_phone VARCHAR(20),
    contact_email VARCHAR(255),
    product_name VARCHAR(500) NOT NULL,
    serial_number VARCHAR(100),
    model_number VARCHAR(100),
    warranty_status VARCHAR(50) DEFAULT 'Under Warranty',
    warranty_start_date DATE,
    warranty_end_date DATE,
    status VARCHAR(50) DEFAULT 'Scheduled',
    site_details TEXT,
    installation_address TEXT,
    assigned_to VARCHAR(255),
    technician_name VARCHAR(255),
    technician_team VARCHAR(100),
    installation_start_date DATE,
    installation_completion_date DATE,
    estimated_completion_date DATE,
    installation_notes TEXT,
    equipment_checklist JSONB,
    client_sign_off BOOLEAN DEFAULT FALSE,
    client_feedback TEXT,
    photos JSONB,
    documents JSONB,
    priority VARCHAR(20) DEFAULT 'Medium',
    region VARCHAR(100),
    company_id UUID NOT NULL,
    created_by UUID,
    updated_by UUID,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add indexes for better performance
CREATE INDEX IF NOT EXISTS idx_installations_company_id ON installations(company_id);
CREATE INDEX IF NOT EXISTS idx_installations_sales_order_id ON installations(sales_order_id);
CREATE INDEX IF NOT EXISTS idx_installations_status ON installations(status);
CREATE INDEX IF NOT EXISTS idx_installations_assigned_to ON installations(assigned_to);
CREATE INDEX IF NOT EXISTS idx_installations_installation_date ON installations(installation_date);
CREATE INDEX IF NOT EXISTS idx_installations_serial_number ON installations(serial_number);

-- Add check constraints
ALTER TABLE installations ADD CONSTRAINT installations_status_check 
    CHECK (status IN ('Scheduled', 'In Progress', 'Completed', 'Pending', 'On Hold', 'Cancelled'));

ALTER TABLE installations ADD CONSTRAINT installations_warranty_status_check 
    CHECK (warranty_status IN ('Under Warranty', 'Warranty Expired', 'Extended Warranty', 'No Warranty'));

ALTER TABLE installations ADD CONSTRAINT installations_priority_check 
    CHECK (priority IN ('Low', 'Medium', 'High', 'Critical'));

-- Sample data for testing
INSERT INTO installations (
    installation_number, installation_date, sales_order_number, account_name, 
    contact_person, contact_phone, product_name, serial_number, 
    warranty_status, status, site_details, assigned_to, technician_name, 
    technician_team, installation_completion_date, priority, company_id
) VALUES 
(
    'INST-2025-001', '2025-01-20', 'SO-2025-001', 'Kerala Agricultural University',
    'Dr. Priya Sharma', '+91-9876543210', 'TRICOLOR MULTICHANNEL FIBRINOMETER', 'TCF-2025-001',
    'Under Warranty', 'Completed', 'Main Laboratory, 2nd Floor, Biotechnology Department', 
    'Hari Kumar K', 'Technical Team A', 'Installation Team 1', '2025-01-22', 'High', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'INST-2025-002', '2025-01-18', 'SO-2025-002', 'Eurofins Advinus',
    'Dr. Research Head', '+91-9876543211', 'LABORATORY FREEZE DRYER/LYOPHILIZER', 'LFD-2025-002',
    'Under Warranty', 'In Progress', 'Research Wing, Clean Room 3', 
    'Pauline', 'Technical Team B', 'Installation Team 2', NULL, 'Medium', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'INST-2025-003', '2025-01-15', 'SO-2025-003', 'TSAR Labcare',
    'Mr. Mahesh', '+91-9876543212', 'ND 1000 Spectrophotometer', 'ND-2025-003',
    'Under Warranty', 'Scheduled', 'Quality Control Laboratory, 3rd Floor', 
    'Hari Kumar K', 'Technical Team A', 'Installation Team 1', NULL, 'Medium', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'INST-2025-004', '2025-01-12', 'SO-2025-004', 'JNCASR',
    'Dr. Anu Rang', '+91-9876543213', 'LAF-02 Application Equipment', 'LAF-2025-004',
    'Under Warranty', 'Pending', 'Materials Science Laboratory', 
    'Pauline', NULL, NULL, NULL, 'Low', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
);

-- Update trigger for updated_at
CREATE OR REPLACE FUNCTION update_installations_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_installations_updated_at_trigger
    BEFORE UPDATE ON installations
    FOR EACH ROW
    EXECUTE FUNCTION update_installations_updated_at();

COMMENT ON TABLE installations IS 'Tracks equipment installations for sales orders';
COMMENT ON COLUMN installations.installation_number IS 'Unique installation tracking number';
COMMENT ON COLUMN installations.sales_order_id IS 'Reference to the sales order that generated this installation';
COMMENT ON COLUMN installations.warranty_status IS 'Current warranty status of the installed equipment';
COMMENT ON COLUMN installations.client_sign_off IS 'Whether client has signed off on completed installation';