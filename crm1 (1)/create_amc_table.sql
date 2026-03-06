-- Create AMC Contracts Table
CREATE TABLE IF NOT EXISTS amc_contracts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Auto-generated reference number
    amc_number VARCHAR(50) UNIQUE,
    
    -- Reference to installation (optional)
    installation_id UUID,
    installation_number VARCHAR(50),
    
    -- Customer Information
    customer_name VARCHAR(255) NOT NULL,
    contact_person VARCHAR(255) NOT NULL,
    customer_phone VARCHAR(20),
    customer_email VARCHAR(255),
    service_address TEXT NOT NULL,
    city VARCHAR(100),
    state VARCHAR(100),
    pincode VARCHAR(10),
    
    -- Equipment Information (JSONB array)
    equipment_details JSONB DEFAULT '[]'::jsonb,
    
    -- Contract Details
    contract_type VARCHAR(50) DEFAULT 'comprehensive',
    contract_value DECIMAL(15,2) NOT NULL,
    payment_terms TEXT,
    
    -- Contract Period
    start_date DATE NOT NULL,
    end_date DATE,
    duration_months INTEGER DEFAULT 12,
    
    -- Service Details
    service_frequency VARCHAR(20) DEFAULT 'quarterly',
    number_of_services INTEGER DEFAULT 4,
    services_completed INTEGER DEFAULT 0,
    
    -- Coverage Details
    labour_charges_included BOOLEAN DEFAULT true,
    spare_parts_included BOOLEAN DEFAULT false,
    emergency_support BOOLEAN DEFAULT false,
    response_time_hours INTEGER DEFAULT 24,
    
    -- Status
    status VARCHAR(20) DEFAULT 'Active',
    
    -- Assignment
    assigned_technician VARCHAR(255),
    service_manager VARCHAR(255),
    
    -- Additional Information
    terms_and_conditions TEXT,
    exclusions TEXT,
    special_instructions TEXT,
    
    -- Auto-renewal
    auto_renewal BOOLEAN DEFAULT false,
    renewal_notice_days INTEGER DEFAULT 30,
    
    -- Source information (for tracking where AMC was created from)
    source_type VARCHAR(20), -- 'installation', 'direct'
    source_reference VARCHAR(50),
    
    -- Company and user tracking
    company_id UUID NOT NULL,
    created_by UUID NOT NULL,
    updated_by UUID,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_amc_contracts_company_id ON amc_contracts(company_id);
CREATE INDEX IF NOT EXISTS idx_amc_contracts_customer_name ON amc_contracts(customer_name);
CREATE INDEX IF NOT EXISTS idx_amc_contracts_status ON amc_contracts(status);
CREATE INDEX IF NOT EXISTS idx_amc_contracts_start_date ON amc_contracts(start_date);
CREATE INDEX IF NOT EXISTS idx_amc_contracts_end_date ON amc_contracts(end_date);
CREATE INDEX IF NOT EXISTS idx_amc_contracts_installation_id ON amc_contracts(installation_id);
CREATE INDEX IF NOT EXISTS idx_amc_contracts_amc_number ON amc_contracts(amc_number);

-- Create trigger to auto-generate AMC number
CREATE OR REPLACE FUNCTION generate_amc_number()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.amc_number IS NULL OR NEW.amc_number = '' THEN
        NEW.amc_number := 'AMC-' || TO_CHAR(NOW(), 'YYYY') || '-' || 
                         LPAD((
                             SELECT COALESCE(MAX(CAST(SUBSTRING(amc_number FROM 'AMC-\d{4}-(\d+)') AS INTEGER)), 0) + 1
                             FROM amc_contracts 
                             WHERE amc_number ~ '^AMC-\d{4}-\d+$'
                             AND EXTRACT(YEAR FROM created_at) = EXTRACT(YEAR FROM NOW())
                         )::TEXT, 4, '0');
    END IF;
    
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
DROP TRIGGER IF EXISTS amc_contracts_auto_number ON amc_contracts;
CREATE TRIGGER amc_contracts_auto_number
    BEFORE INSERT OR UPDATE ON amc_contracts
    FOR EACH ROW
    EXECUTE FUNCTION generate_amc_number();

-- Add comments for documentation
COMMENT ON TABLE amc_contracts IS 'Annual Maintenance Contract records';
COMMENT ON COLUMN amc_contracts.amc_number IS 'Auto-generated unique AMC reference number (AMC-YYYY-NNNN)';
COMMENT ON COLUMN amc_contracts.equipment_details IS 'JSON array of equipment covered under AMC';
COMMENT ON COLUMN amc_contracts.contract_value IS 'Total contract value in rupees';
COMMENT ON COLUMN amc_contracts.service_frequency IS 'How often service is performed (monthly, quarterly, half_yearly, yearly)';
COMMENT ON COLUMN amc_contracts.source_type IS 'Source of AMC creation (installation, direct)';