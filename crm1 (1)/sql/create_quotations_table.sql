-- Create quotations table for CRM system
CREATE TABLE IF NOT EXISTS public.quotations (
    -- Primary Key
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Quotation Information
    quote_number VARCHAR(50) UNIQUE NOT NULL,
    quote_series VARCHAR(10) DEFAULT 'QUO',
    revision_number INTEGER DEFAULT 1,
    
    -- Customer Information
    customer_name VARCHAR(255) NOT NULL,
    contact_person VARCHAR(255) NOT NULL,
    customer_email VARCHAR(255),
    customer_phone VARCHAR(20),
    billing_address TEXT,
    shipping_address TEXT,
    gst_number VARCHAR(20),
    pan_number VARCHAR(15),
    
    -- Quotation Details
    quote_date DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_until DATE NOT NULL,
    deal_ref UUID REFERENCES deals(id),
    lead_ref UUID REFERENCES leads(id),
    inquiry_source VARCHAR(100),
    
    -- Product and Pricing
    products_quoted TEXT NOT NULL,
    specifications TEXT,
    quantity INTEGER DEFAULT 1,
    subtotal DECIMAL(15,2) DEFAULT 0,
    discount_amount DECIMAL(15,2) DEFAULT 0,
    tax_amount DECIMAL(15,2) DEFAULT 0,
    total_amount DECIMAL(15,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'INR',
    
    -- Status and Stage
    status VARCHAR(50) DEFAULT 'draft' 
        CHECK (status IN ('draft', 'sent', 'viewed', 'negotiation', 'approved', 'rejected', 'expired', 'converted')),
    stage VARCHAR(50) DEFAULT 'preparation'
        CHECK (stage IN ('preparation', 'review', 'sent', 'follow_up', 'negotiation', 'closure')),
    probability INTEGER DEFAULT 50 CHECK (probability >= 0 AND probability <= 100),
    
    -- Payment and Terms
    payment_terms VARCHAR(100),
    delivery_terms VARCHAR(100),
    warranty_period VARCHAR(50),
    installation_included BOOLEAN DEFAULT false,
    training_included BOOLEAN DEFAULT false,
    
    -- Communication and Follow-up
    last_sent_date TIMESTAMP,
    follow_up_date DATE,
    competitor_info TEXT,
    win_loss_reason TEXT,
    
    -- Assignment and Management
    assigned_to VARCHAR(255),
    sales_manager VARCHAR(255),
    priority VARCHAR(20) DEFAULT 'medium'
        CHECK (priority IN ('low', 'medium', 'high', 'urgent')),
    
    -- Additional Information
    internal_notes TEXT,
    customer_notes TEXT,
    terms_conditions TEXT,
    
    -- Company and Audit
    company_id UUID NOT NULL,
    created_by VARCHAR(255),
    updated_by VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_quotations_quote_number ON quotations(quote_number);
CREATE INDEX IF NOT EXISTS idx_quotations_customer_name ON quotations(customer_name);
CREATE INDEX IF NOT EXISTS idx_quotations_status ON quotations(status);
CREATE INDEX IF NOT EXISTS idx_quotations_assigned_to ON quotations(assigned_to);
CREATE INDEX IF NOT EXISTS idx_quotations_quote_date ON quotations(quote_date);
CREATE INDEX IF NOT EXISTS idx_quotations_valid_until ON quotations(valid_until);
CREATE INDEX IF NOT EXISTS idx_quotations_company_id ON quotations(company_id);
CREATE INDEX IF NOT EXISTS idx_quotations_deal_ref ON quotations(deal_ref);
CREATE INDEX IF NOT EXISTS idx_quotations_lead_ref ON quotations(lead_ref);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_quotations_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER trigger_quotations_updated_at
    BEFORE UPDATE ON quotations
    FOR EACH ROW
    EXECUTE FUNCTION update_quotations_updated_at();

-- Insert sample data for testing
INSERT INTO quotations (
    quote_number, customer_name, contact_person, customer_email, customer_phone,
    quote_date, valid_until, products_quoted, total_amount, status, stage,
    assigned_to, company_id, created_by
) VALUES 
(
    'QUO-2025-001',
    'Kerala Agricultural University',
    'Dr. Priya Sharma',
    'priya@kau.ac.in',
    '+91-9876543210',
    '2025-01-05',
    '2025-02-05',
    'TRICOLOR MULTICHANNEL FIBRINOMETER with accessories and installation',
    850000,
    'approved',
    'closure',
    'Hari Kumar K',
    'de19ccb7-e90d-4507-861d-a3aecf5e3f29',
    'System'
),
(
    'QUO-2025-002',
    'Eurofins Advinus',
    'Dr. Research Head',
    'research@eurofins.com',
    '+91-9876543211',
    '2025-01-03',
    '2025-02-03',
    'LABORATORY FREEZE DRYER/LYOPHILIZER with training package',
    1275000,
    'approved',
    'closure',
    'Pauline',
    'de19ccb7-e90d-4507-861d-a3aecf5e3f29',
    'System'
),
(
    'QUO-2025-003',
    'TSAR Labcare',
    'Mr. Mahesh',
    'mahesh@tsarlabcare.com',
    '+91-9876543212',
    '2024-12-28',
    '2025-01-28',
    'ND 1000 Spectrophotometer with maintenance contract',
    475000,
    'approved',
    'closure',
    'Hari Kumar K',
    'de19ccb7-e90d-4507-861d-a3aecf5e3f29',
    'System'
),
(
    'QUO-2025-004',
    'JNCASR',
    'Dr. Anu Rang',
    'anu@jncasr.ac.in',
    '+91-9876543213',
    '2025-01-08',
    '2025-02-08',
    'Automated Media Preparator with installation and training',
    725000,
    'sent',
    'follow_up',
    'Vijay Muppala',
    'de19ccb7-e90d-4507-861d-a3aecf5e3f29',
    'System'
),
(
    'QUO-2025-005',
    'Bio-Rad Laboratories',
    'Mr. Sanjay Patel',
    'sanjay@biorad.com',
    '+91-9876543214',
    '2025-01-10',
    '2025-02-10',
    'Bio-Safety Cabinet Class II with HEPA filters',
    580000,
    'negotiation',
    'negotiation',
    'Prashanth Sandilya',
    'de19ccb7-e90d-4507-861d-a3aecf5e3f29',
    'System'
);

-- Grant necessary permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON quotations TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON quotations TO anon;

COMMENT ON TABLE quotations IS 'Stores quotation information for the CRM system';
COMMENT ON COLUMN quotations.quote_number IS 'Unique quotation number (e.g., QUO-2025-001)';
COMMENT ON COLUMN quotations.status IS 'Current status of the quotation';
COMMENT ON COLUMN quotations.stage IS 'Current stage in the quotation process';
COMMENT ON COLUMN quotations.probability IS 'Probability of winning the quotation (0-100%)';
COMMENT ON COLUMN quotations.deal_ref IS 'Reference to related deal if quotation is converted';
COMMENT ON COLUMN quotations.lead_ref IS 'Reference to related lead that generated this quotation';