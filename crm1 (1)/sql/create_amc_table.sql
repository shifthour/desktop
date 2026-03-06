-- AMC (Annual Maintenance Contract) table for tracking maintenance contracts
-- This table links to installations and sales_orders for contract management

CREATE TABLE IF NOT EXISTS amc_contracts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    amc_number VARCHAR(50) UNIQUE NOT NULL,
    contract_type VARCHAR(50) DEFAULT 'AMC',
    sales_order_id UUID REFERENCES sales_orders(id),
    sales_order_number VARCHAR(100),
    installation_id UUID REFERENCES installations(id),
    account_name VARCHAR(255) NOT NULL,
    contact_person VARCHAR(255),
    contact_phone VARCHAR(20),
    contact_email VARCHAR(255),
    product_name VARCHAR(500) NOT NULL,
    serial_number VARCHAR(100),
    model_number VARCHAR(100),
    contract_start_date DATE NOT NULL,
    contract_end_date DATE NOT NULL,
    contract_value DECIMAL(15,2) NOT NULL DEFAULT 0,
    currency VARCHAR(10) DEFAULT 'INR',
    payment_terms VARCHAR(100),
    payment_schedule VARCHAR(50) DEFAULT 'Annual',
    status VARCHAR(50) DEFAULT 'Active',
    renewal_status VARCHAR(50) DEFAULT 'Not Due',
    renewal_date DATE,
    renewal_reminder_date DATE,
    auto_renewal BOOLEAN DEFAULT FALSE,
    site_details TEXT,
    service_address TEXT,
    assigned_to VARCHAR(255),
    service_engineer VARCHAR(255),
    service_frequency VARCHAR(50) DEFAULT 'Quarterly',
    last_service_date DATE,
    next_service_date DATE,
    service_inclusions TEXT,
    service_exclusions TEXT,
    emergency_support BOOLEAN DEFAULT TRUE,
    response_time VARCHAR(50) DEFAULT '24 hours',
    escalation_matrix JSONB,
    contract_notes TEXT,
    terms_conditions TEXT,
    attachments JSONB,
    priority VARCHAR(20) DEFAULT 'Medium',
    region VARCHAR(100),
    company_id UUID NOT NULL,
    created_by UUID,
    updated_by UUID,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add indexes for better performance
CREATE INDEX IF NOT EXISTS idx_amc_contracts_company_id ON amc_contracts(company_id);
CREATE INDEX IF NOT EXISTS idx_amc_contracts_sales_order_id ON amc_contracts(sales_order_id);
CREATE INDEX IF NOT EXISTS idx_amc_contracts_installation_id ON amc_contracts(installation_id);
CREATE INDEX IF NOT EXISTS idx_amc_contracts_status ON amc_contracts(status);
CREATE INDEX IF NOT EXISTS idx_amc_contracts_assigned_to ON amc_contracts(assigned_to);
CREATE INDEX IF NOT EXISTS idx_amc_contracts_contract_end_date ON amc_contracts(contract_end_date);
CREATE INDEX IF NOT EXISTS idx_amc_contracts_serial_number ON amc_contracts(serial_number);
CREATE INDEX IF NOT EXISTS idx_amc_contracts_renewal_date ON amc_contracts(renewal_date);

-- Add check constraints
ALTER TABLE amc_contracts ADD CONSTRAINT amc_status_check 
    CHECK (status IN ('Active', 'Expired', 'Pending Renewal', 'Cancelled', 'Draft', 'Suspended'));

ALTER TABLE amc_contracts ADD CONSTRAINT amc_renewal_status_check 
    CHECK (renewal_status IN ('Not Due', 'Due Soon', 'Overdue', 'Renewed', 'Cancelled'));

ALTER TABLE amc_contracts ADD CONSTRAINT amc_priority_check 
    CHECK (priority IN ('Low', 'Medium', 'High', 'Critical'));

ALTER TABLE amc_contracts ADD CONSTRAINT amc_payment_schedule_check 
    CHECK (payment_schedule IN ('Monthly', 'Quarterly', 'Half-Yearly', 'Annual'));

ALTER TABLE amc_contracts ADD CONSTRAINT amc_service_frequency_check 
    CHECK (service_frequency IN ('Monthly', 'Quarterly', 'Half-Yearly', 'Annual', 'On-Call'));

-- Sample data for testing
INSERT INTO amc_contracts (
    amc_number, contract_type, sales_order_number, account_name, 
    contact_person, contact_phone, product_name, serial_number, 
    contract_start_date, contract_end_date, contract_value, 
    status, site_details, assigned_to, service_engineer,
    service_frequency, next_service_date, priority, company_id
) VALUES 
(
    'AMC-2025-001', 'AMC', 'SO-2024-156', 'Kerala Agricultural University',
    'Dr. Priya Sharma', '+91-9876543210', 'TRICOLOR MULTICHANNEL FIBRINOMETER', 'TCF-2024-001',
    '2025-04-01', '2026-03-31', 85000.00,
    'Active', 'Main Laboratory, Bangalore', 'Hari Kumar K', 'Service Team A',
    'Quarterly', '2025-04-15', 'High', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'AMC-2025-002', 'AMC', 'SO-2024-189', 'Eurofins Advinus',
    'Dr. Research Head', '+91-9876543211', 'LABORATORY FREEZE DRYER', 'LFD-2024-003',
    '2025-05-15', '2026-05-14', 120000.00,
    'Active', 'Research Wing, Bangalore', 'Pauline', 'Service Team B',
    'Quarterly', '2025-05-30', 'Medium', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'AMC-2024-045', 'AMC', 'SO-2023-234', 'TSAR Labcare',
    'Mr. Mahesh', '+91-9876543212', 'ND 1000 Spectrophotometer', 'ND-2023-012',
    '2024-01-01', '2024-12-31', 47500.00,
    'Expired', 'Quality Control Laboratory', 'Hari Kumar K', 'Service Team A',
    'Half-Yearly', '2024-12-31', 'Medium', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'AMC-2025-003', 'AMC', 'SO-2025-012', 'JNCASR',
    'Dr. Anu Rang', '+91-9876543213', 'LAF-02 Application Equipment', 'LAF-2025-001',
    '2025-06-01', '2026-05-31', 32500.00,
    'Pending Renewal', 'Materials Science Lab', 'Pauline', 'Service Team C',
    'Quarterly', '2025-06-15', 'Low', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
);

-- Update trigger for updated_at
CREATE OR REPLACE FUNCTION update_amc_contracts_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_amc_contracts_updated_at_trigger
    BEFORE UPDATE ON amc_contracts
    FOR EACH ROW
    EXECUTE FUNCTION update_amc_contracts_updated_at();

COMMENT ON TABLE amc_contracts IS 'Annual Maintenance Contracts for installed equipment';
COMMENT ON COLUMN amc_contracts.amc_number IS 'Unique AMC contract number';
COMMENT ON COLUMN amc_contracts.installation_id IS 'Reference to the installation this AMC covers';
COMMENT ON COLUMN amc_contracts.auto_renewal IS 'Whether the contract automatically renews';
COMMENT ON COLUMN amc_contracts.emergency_support IS 'Whether emergency support is included';