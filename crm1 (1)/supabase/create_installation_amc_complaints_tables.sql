-- Create tables for Installations, AMC, and Complaints management

-- ========================================
-- INSTALLATIONS TABLE
-- ========================================

CREATE TABLE installations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE NOT NULL,
    installation_number TEXT NOT NULL UNIQUE,
    
    -- Reference to source document (sales order or invoice)
    source_type TEXT CHECK (source_type IN ('sales_order', 'invoice', 'direct')) NOT NULL,
    source_reference TEXT, -- Sales Order ID or Invoice ID
    
    -- Customer Information
    customer_name TEXT NOT NULL,
    contact_person TEXT NOT NULL,
    customer_phone TEXT,
    customer_email TEXT,
    installation_address TEXT NOT NULL,
    city TEXT,
    state TEXT,
    pincode TEXT,
    
    -- Product/Equipment Information
    product_name TEXT NOT NULL,
    product_model TEXT,
    serial_number TEXT,
    quantity INTEGER NOT NULL DEFAULT 1,
    equipment_value DECIMAL(15,2),
    
    -- Installation Details
    installation_type TEXT CHECK (installation_type IN ('new_installation', 'replacement', 'upgrade', 'repair')) NOT NULL DEFAULT 'new_installation',
    priority TEXT CHECK (priority IN ('Low', 'Medium', 'High', 'Critical')) NOT NULL DEFAULT 'Medium',
    status TEXT CHECK (status IN ('Scheduled', 'In Progress', 'Completed', 'On Hold', 'Cancelled', 'Rescheduled')) NOT NULL DEFAULT 'Scheduled',
    
    -- Scheduling
    scheduled_date DATE,
    scheduled_time TIME,
    estimated_duration INTEGER, -- in hours
    actual_start_time TIMESTAMP,
    actual_end_time TIMESTAMP,
    
    -- Assignment
    assigned_technician TEXT,
    technician_phone TEXT,
    team_members TEXT[], -- Array of team member names
    
    -- Installation Details
    pre_installation_notes TEXT,
    installation_notes TEXT,
    post_installation_notes TEXT,
    customer_feedback TEXT,
    customer_signature_url TEXT, -- URL to signed document
    
    -- Warranty Information
    warranty_period INTEGER, -- in months
    warranty_start_date DATE,
    warranty_end_date DATE,
    warranty_terms TEXT,
    
    -- Additional Information
    special_instructions TEXT,
    required_tools TEXT[],
    safety_requirements TEXT,
    
    -- Financial
    installation_cost DECIMAL(15,2),
    additional_charges DECIMAL(15,2),
    total_cost DECIMAL(15,2),
    
    -- Status tracking
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    created_by UUID REFERENCES users(id),
    updated_by UUID REFERENCES users(id)
);

-- Create indexes for installations
CREATE INDEX idx_installations_company_id ON installations(company_id);
CREATE INDEX idx_installations_source_reference ON installations(source_reference);
CREATE INDEX idx_installations_status ON installations(status);
CREATE INDEX idx_installations_scheduled_date ON installations(scheduled_date);
CREATE INDEX idx_installations_assigned_technician ON installations(assigned_technician);

-- Generate installation number trigger
CREATE OR REPLACE FUNCTION generate_installation_number()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.installation_number IS NULL OR NEW.installation_number = '' THEN
        NEW.installation_number := 'INST-' || TO_CHAR(NOW(), 'YYYY') || '-' || LPAD(NEXTVAL('installation_number_seq')::TEXT, 6, '0');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE SEQUENCE installation_number_seq START 1;
CREATE TRIGGER trigger_generate_installation_number
    BEFORE INSERT ON installations
    FOR EACH ROW EXECUTE FUNCTION generate_installation_number();

-- ========================================
-- AMC (Annual Maintenance Contracts) TABLE
-- ========================================

CREATE TABLE amc_contracts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE NOT NULL,
    amc_number TEXT NOT NULL UNIQUE,
    
    -- Reference to installation
    installation_id UUID REFERENCES installations(id) ON DELETE SET NULL,
    
    -- Customer Information
    customer_name TEXT NOT NULL,
    contact_person TEXT NOT NULL,
    customer_phone TEXT,
    customer_email TEXT,
    service_address TEXT NOT NULL,
    city TEXT,
    state TEXT,
    pincode TEXT,
    
    -- Equipment Information
    equipment_details JSONB, -- Array of equipment with details
    
    -- Contract Details
    contract_type TEXT CHECK (contract_type IN ('comprehensive', 'non_comprehensive', 'preventive_only')) NOT NULL,
    contract_value DECIMAL(15,2) NOT NULL,
    payment_terms TEXT,
    
    -- Contract Period
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    duration_months INTEGER NOT NULL,
    
    -- Service Details
    service_frequency TEXT CHECK (service_frequency IN ('monthly', 'quarterly', 'half_yearly', 'yearly')) NOT NULL DEFAULT 'quarterly',
    number_of_services INTEGER NOT NULL,
    services_completed INTEGER DEFAULT 0,
    
    -- Coverage Details
    labour_charges_included BOOLEAN DEFAULT true,
    spare_parts_included BOOLEAN DEFAULT false,
    emergency_support BOOLEAN DEFAULT false,
    response_time_hours INTEGER DEFAULT 24,
    
    -- Status
    status TEXT CHECK (status IN ('Active', 'Expired', 'Cancelled', 'Suspended', 'Renewal Due')) NOT NULL DEFAULT 'Active',
    
    -- Assignment
    assigned_technician TEXT,
    service_manager TEXT,
    
    -- Additional Information
    terms_and_conditions TEXT,
    exclusions TEXT,
    special_instructions TEXT,
    
    -- Auto-renewal
    auto_renewal BOOLEAN DEFAULT false,
    renewal_notice_days INTEGER DEFAULT 30,
    
    -- Status tracking
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    created_by UUID REFERENCES users(id),
    updated_by UUID REFERENCES users(id)
);

-- Create indexes for AMC contracts
CREATE INDEX idx_amc_contracts_company_id ON amc_contracts(company_id);
CREATE INDEX idx_amc_contracts_installation_id ON amc_contracts(installation_id);
CREATE INDEX idx_amc_contracts_status ON amc_contracts(status);
CREATE INDEX idx_amc_contracts_end_date ON amc_contracts(end_date);
CREATE INDEX idx_amc_contracts_customer_name ON amc_contracts(customer_name);

-- Generate AMC number trigger
CREATE OR REPLACE FUNCTION generate_amc_number()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.amc_number IS NULL OR NEW.amc_number = '' THEN
        NEW.amc_number := 'AMC-' || TO_CHAR(NOW(), 'YYYY') || '-' || LPAD(NEXTVAL('amc_number_seq')::TEXT, 6, '0');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE SEQUENCE amc_number_seq START 1;
CREATE TRIGGER trigger_generate_amc_number
    BEFORE INSERT ON amc_contracts
    FOR EACH ROW EXECUTE FUNCTION generate_amc_number();

-- ========================================
-- AMC SERVICES TABLE (Track individual service visits)
-- ========================================

CREATE TABLE amc_services (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE NOT NULL,
    amc_contract_id UUID REFERENCES amc_contracts(id) ON DELETE CASCADE NOT NULL,
    service_number TEXT NOT NULL,
    
    -- Service Details
    service_date DATE,
    service_type TEXT CHECK (service_type IN ('preventive', 'breakdown', 'emergency', 'inspection')) NOT NULL,
    status TEXT CHECK (status IN ('Scheduled', 'In Progress', 'Completed', 'Cancelled', 'Rescheduled')) NOT NULL DEFAULT 'Scheduled',
    
    -- Assignment
    assigned_technician TEXT,
    technician_phone TEXT,
    
    -- Service Report
    work_performed TEXT,
    parts_replaced JSONB, -- Array of parts with details
    issues_found TEXT,
    recommendations TEXT,
    customer_feedback TEXT,
    service_completion_time TIMESTAMP,
    
    -- Ratings
    customer_rating INTEGER CHECK (customer_rating >= 1 AND customer_rating <= 5),
    service_quality_rating INTEGER CHECK (service_quality_rating >= 1 AND service_quality_rating <= 5),
    
    -- Files and documents
    service_report_url TEXT,
    before_photos TEXT[],
    after_photos TEXT[],
    customer_signature_url TEXT,
    
    -- Next service
    next_service_due DATE,
    
    -- Status tracking
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    created_by UUID REFERENCES users(id),
    updated_by UUID REFERENCES users(id)
);

-- Create indexes for AMC services
CREATE INDEX idx_amc_services_company_id ON amc_services(company_id);
CREATE INDEX idx_amc_services_contract_id ON amc_services(amc_contract_id);
CREATE INDEX idx_amc_services_service_date ON amc_services(service_date);
CREATE INDEX idx_amc_services_status ON amc_services(status);

-- ========================================
-- COMPLAINTS TABLE
-- ========================================

CREATE TABLE complaints (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE NOT NULL,
    complaint_number TEXT NOT NULL UNIQUE,
    
    -- Reference Information
    installation_id UUID REFERENCES installations(id) ON DELETE SET NULL,
    amc_contract_id UUID REFERENCES amc_contracts(id) ON DELETE SET NULL,
    source_reference TEXT, -- Can reference sales order, invoice, etc.
    
    -- Customer Information
    customer_name TEXT NOT NULL,
    contact_person TEXT NOT NULL,
    customer_phone TEXT,
    customer_email TEXT,
    customer_address TEXT,
    city TEXT,
    state TEXT,
    pincode TEXT,
    
    -- Complaint Details
    complaint_title TEXT NOT NULL,
    complaint_description TEXT NOT NULL,
    complaint_type TEXT CHECK (complaint_type IN ('product_quality', 'service_quality', 'installation_issue', 'billing_issue', 'delivery_issue', 'warranty_claim', 'other')) NOT NULL,
    severity TEXT CHECK (severity IN ('Low', 'Medium', 'High', 'Critical')) NOT NULL DEFAULT 'Medium',
    category TEXT CHECK (category IN ('Technical', 'Commercial', 'Service', 'Quality', 'Delivery')) NOT NULL,
    
    -- Product/Service Information
    product_name TEXT,
    product_model TEXT,
    serial_number TEXT,
    purchase_date DATE,
    warranty_status TEXT CHECK (warranty_status IN ('Under Warranty', 'Warranty Expired', 'No Warranty')),
    
    -- Status and Assignment
    status TEXT CHECK (status IN ('Open', 'Assigned', 'In Progress', 'Resolved', 'Closed', 'Escalated', 'On Hold')) NOT NULL DEFAULT 'Open',
    priority TEXT CHECK (priority IN ('Low', 'Medium', 'High', 'Critical')) NOT NULL DEFAULT 'Medium',
    assigned_to TEXT,
    department TEXT CHECK (department IN ('Technical', 'Sales', 'Service', 'Quality', 'Management')),
    
    -- Resolution Details
    resolution_description TEXT,
    resolution_date DATE,
    resolution_time_hours DECIMAL(10,2),
    customer_satisfaction_rating INTEGER CHECK (customer_satisfaction_rating >= 1 AND customer_satisfaction_rating <= 5),
    
    -- Follow-up
    follow_up_required BOOLEAN DEFAULT false,
    follow_up_date DATE,
    follow_up_notes TEXT,
    
    -- Communication
    communication_history JSONB DEFAULT '[]'::jsonb, -- Array of communication logs
    
    -- Files and attachments
    attachments TEXT[], -- URLs to uploaded files
    resolution_attachments TEXT[],
    
    -- Escalation
    escalation_level INTEGER DEFAULT 0,
    escalated_to TEXT,
    escalation_date DATE,
    escalation_reason TEXT,
    
    -- Financial Impact
    compensation_amount DECIMAL(15,2),
    refund_amount DECIMAL(15,2),
    cost_to_resolve DECIMAL(15,2),
    
    -- Additional Information
    root_cause TEXT,
    preventive_action TEXT,
    lessons_learned TEXT,
    
    -- Status tracking
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    created_by UUID REFERENCES users(id),
    updated_by UUID REFERENCES users(id)
);

-- Create indexes for complaints
CREATE INDEX idx_complaints_company_id ON complaints(company_id);
CREATE INDEX idx_complaints_installation_id ON complaints(installation_id);
CREATE INDEX idx_complaints_amc_contract_id ON complaints(amc_contract_id);
CREATE INDEX idx_complaints_status ON complaints(status);
CREATE INDEX idx_complaints_priority ON complaints(priority);
CREATE INDEX idx_complaints_assigned_to ON complaints(assigned_to);
CREATE INDEX idx_complaints_customer_name ON complaints(customer_name);

-- Generate complaint number trigger
CREATE OR REPLACE FUNCTION generate_complaint_number()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.complaint_number IS NULL OR NEW.complaint_number = '' THEN
        NEW.complaint_number := 'COMP-' || TO_CHAR(NOW(), 'YYYY') || '-' || LPAD(NEXTVAL('complaint_number_seq')::TEXT, 6, '0');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE SEQUENCE complaint_number_seq START 1;
CREATE TRIGGER trigger_generate_complaint_number
    BEFORE INSERT ON complaints
    FOR EACH ROW EXECUTE FUNCTION generate_complaint_number();

-- ========================================
-- UPDATE TRIGGERS
-- ========================================

-- Update triggers for all tables
CREATE TRIGGER update_installations_updated_at 
    BEFORE UPDATE ON installations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_amc_contracts_updated_at 
    BEFORE UPDATE ON amc_contracts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_amc_services_updated_at 
    BEFORE UPDATE ON amc_services
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_complaints_updated_at 
    BEFORE UPDATE ON complaints
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ========================================
-- ROW LEVEL SECURITY POLICIES
-- ========================================

-- Enable RLS on all tables
ALTER TABLE installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE amc_contracts ENABLE ROW LEVEL SECURITY;
ALTER TABLE amc_services ENABLE ROW LEVEL SECURITY;
ALTER TABLE complaints ENABLE ROW LEVEL SECURITY;

-- RLS Policies for installations
CREATE POLICY "Users can access installations from their company" ON installations
    FOR ALL USING (
        company_id IN (
            SELECT company_id FROM users WHERE id = auth.uid()
        )
    );

-- RLS Policies for AMC contracts
CREATE POLICY "Users can access AMC contracts from their company" ON amc_contracts
    FOR ALL USING (
        company_id IN (
            SELECT company_id FROM users WHERE id = auth.uid()
        )
    );

-- RLS Policies for AMC services
CREATE POLICY "Users can access AMC services from their company" ON amc_services
    FOR ALL USING (
        company_id IN (
            SELECT company_id FROM users WHERE id = auth.uid()
        )
    );

-- RLS Policies for complaints
CREATE POLICY "Users can access complaints from their company" ON complaints
    FOR ALL USING (
        company_id IN (
            SELECT company_id FROM users WHERE id = auth.uid()
        )
    );

-- ========================================
-- SAMPLE DATA (Optional)
-- ========================================

-- Insert sample installation data
INSERT INTO installations (
    company_id, source_type, source_reference, customer_name, contact_person, 
    customer_phone, customer_email, installation_address, city, state, pincode,
    product_name, product_model, serial_number, quantity, equipment_value,
    installation_type, priority, status, scheduled_date, assigned_technician,
    warranty_period, warranty_start_date, warranty_end_date
) VALUES (
    'de19ccb7-e90d-4507-861d-a3aecf5e3f29', 'sales_order', 'SO-2025-001', 
    'ABC Laboratories', 'Dr. John Smith', '+91-9876543210', 'john.smith@abclabs.com',
    '123 Science Park, Tech City', 'Bangalore', 'Karnataka', '560001',
    'PIPETMAN Multichannel', 'GILSON P200', 'GLN20250001', 1, 250000.00,
    'new_installation', 'High', 'Scheduled', '2025-01-25', 'Ravi Kumar',
    24, '2025-01-25', '2027-01-25'
);

-- Insert sample AMC contract
INSERT INTO amc_contracts (
    company_id, installation_id, customer_name, contact_person, 
    customer_phone, customer_email, service_address, city, state, pincode,
    equipment_details, contract_type, contract_value, start_date, end_date, 
    duration_months, service_frequency, number_of_services, status
) VALUES (
    'de19ccb7-e90d-4507-861d-a3aecf5e3f29', 
    (SELECT id FROM installations WHERE customer_name = 'ABC Laboratories' LIMIT 1),
    'ABC Laboratories', 'Dr. John Smith', '+91-9876543210', 'john.smith@abclabs.com',
    '123 Science Park, Tech City', 'Bangalore', 'Karnataka', '560001',
    '[{"equipment": "PIPETMAN Multichannel", "model": "GILSON P200", "serial": "GLN20250001"}]'::jsonb,
    'comprehensive', 50000.00, '2025-01-25', '2026-01-25', 12, 
    'quarterly', 4, 'Active'
);

-- Insert sample complaint
INSERT INTO complaints (
    company_id, installation_id, customer_name, contact_person,
    customer_phone, customer_email, complaint_title, complaint_description,
    complaint_type, severity, category, product_name, product_model, 
    serial_number, status, priority, assigned_to, department
) VALUES (
    'de19ccb7-e90d-4507-861d-a3aecf5e3f29',
    (SELECT id FROM installations WHERE customer_name = 'ABC Laboratories' LIMIT 1),
    'ABC Laboratories', 'Dr. John Smith', '+91-9876543210', 'john.smith@abclabs.com',
    'Equipment calibration issue', 'The pipette is not dispensing accurate volumes after recent service',
    'service_quality', 'High', 'Technical', 'PIPETMAN Multichannel', 'GILSON P200',
    'GLN20250001', 'Open', 'High', 'Ravi Kumar', 'Technical'
);

-- Create views for better data access
CREATE VIEW installation_summary AS
SELECT 
    i.*,
    ac.id as amc_id,
    ac.amc_number,
    ac.status as amc_status,
    ac.end_date as amc_end_date,
    COUNT(c.id) as complaint_count
FROM installations i
LEFT JOIN amc_contracts ac ON i.id = ac.installation_id
LEFT JOIN complaints c ON i.id = c.installation_id
GROUP BY i.id, ac.id, ac.amc_number, ac.status, ac.end_date;

-- Create a view for dashboard statistics
CREATE VIEW service_dashboard_stats AS
SELECT 
    company_id,
    COUNT(CASE WHEN status = 'Scheduled' THEN 1 END) as scheduled_installations,
    COUNT(CASE WHEN status = 'In Progress' THEN 1 END) as inprogress_installations,
    COUNT(CASE WHEN status = 'Completed' THEN 1 END) as completed_installations,
    COUNT(*) as total_installations,
    AVG(CASE WHEN actual_end_time IS NOT NULL AND actual_start_time IS NOT NULL 
        THEN EXTRACT(EPOCH FROM (actual_end_time - actual_start_time))/3600 END) as avg_installation_time
FROM installations
GROUP BY company_id;