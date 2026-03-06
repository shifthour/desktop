-- Complete leads table setup script
-- Execute this script in Supabase SQL Editor

-- Create leads table with all required fields
CREATE TABLE IF NOT EXISTS leads (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    
    -- Account and Contact Relations
    account_id UUID REFERENCES accounts(id) ON DELETE SET NULL,
    account_name TEXT,
    contact_id UUID REFERENCES contacts(id) ON DELETE SET NULL,
    contact_name TEXT,
    department TEXT,
    
    -- Contact Information
    phone TEXT,
    email TEXT,
    whatsapp TEXT,
    
    -- Lead Details
    lead_source TEXT,
    product_id UUID REFERENCES products(id) ON DELETE SET NULL,
    product_name TEXT,
    lead_status TEXT DEFAULT 'New',
    priority TEXT DEFAULT 'medium',
    
    -- Assignment and Tracking
    assigned_to TEXT,
    lead_date DATE DEFAULT CURRENT_DATE,
    closing_date DATE,
    
    -- Financial Information
    budget DECIMAL(15,2),
    quantity INTEGER,
    price_per_unit DECIMAL(15,2),
    
    -- Location Information
    location TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    address TEXT,
    
    -- Additional Fields
    buyer_ref TEXT,
    expected_closing_date DATE,
    next_followup_date DATE,
    notes TEXT,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_leads_company_id ON leads(company_id);
CREATE INDEX IF NOT EXISTS idx_leads_account_id ON leads(account_id);
CREATE INDEX IF NOT EXISTS idx_leads_contact_id ON leads(contact_id);
CREATE INDEX IF NOT EXISTS idx_leads_product_id ON leads(product_id);
CREATE INDEX IF NOT EXISTS idx_leads_lead_status ON leads(lead_status);
CREATE INDEX IF NOT EXISTS idx_leads_assigned_to ON leads(assigned_to);
CREATE INDEX IF NOT EXISTS idx_leads_lead_date ON leads(lead_date);
CREATE INDEX IF NOT EXISTS idx_leads_priority ON leads(priority);

-- Create function to update updated_at timestamp (if not exists)
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = TIMEZONE('utc'::text, NOW());
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to update the updated_at timestamp
DROP TRIGGER IF EXISTS update_leads_updated_at ON leads;
CREATE TRIGGER update_leads_updated_at 
    BEFORE UPDATE ON leads
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions
GRANT ALL ON leads TO authenticated;
GRANT SELECT ON leads TO anon;

-- Row Level Security (Optional - uncomment if needed)
-- ALTER TABLE leads ENABLE ROW LEVEL SECURITY;

-- Create policies for company-based access (if RLS is enabled)
-- CREATE POLICY "Users can access leads from their company" ON leads
--     FOR ALL
--     USING (company_id IN (
--         SELECT company_id FROM users WHERE id = auth.uid()
--     ));