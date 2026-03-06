-- Create deals table for CRM system
-- This implements the Dual Table Model where leads remain in leads table
-- and qualified leads create new records in deals table with reference

CREATE TABLE IF NOT EXISTS deals (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    
    -- Basic deal information
    deal_name VARCHAR(255) NOT NULL,
    account_name VARCHAR(255) NOT NULL,
    contact_person VARCHAR(255) NOT NULL,
    product VARCHAR(255) NOT NULL,
    value DECIMAL(15,2) NOT NULL DEFAULT 0,
    
    -- Deal progression tracking
    stage VARCHAR(50) NOT NULL DEFAULT 'Qualification',
    probability INTEGER NOT NULL DEFAULT 25 CHECK (probability >= 0 AND probability <= 100),
    expected_close_date DATE,
    
    -- Assignment and priority
    assigned_to VARCHAR(255),
    priority VARCHAR(20) DEFAULT 'Medium' CHECK (priority IN ('High', 'Medium', 'Low')),
    
    -- Deal status
    status VARCHAR(20) DEFAULT 'Active' CHECK (status IN ('Active', 'Won', 'Lost', 'On Hold')),
    
    -- Source and tracking
    source VARCHAR(100),
    source_lead_id UUID REFERENCES leads(id), -- Foreign key to original lead
    last_activity VARCHAR(500),
    notes TEXT,
    
    -- Audit fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by VARCHAR(255),
    updated_by VARCHAR(255),
    
    -- Company association (for multi-tenant support)
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_deals_company_id ON deals(company_id);
CREATE INDEX IF NOT EXISTS idx_deals_assigned_to ON deals(assigned_to);
CREATE INDEX IF NOT EXISTS idx_deals_stage ON deals(stage);
CREATE INDEX IF NOT EXISTS idx_deals_status ON deals(status);
CREATE INDEX IF NOT EXISTS idx_deals_source_lead_id ON deals(source_lead_id);
CREATE INDEX IF NOT EXISTS idx_deals_expected_close_date ON deals(expected_close_date);
CREATE INDEX IF NOT EXISTS idx_deals_created_at ON deals(created_at);

-- Create trigger to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_deals_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_deals_updated_at
    BEFORE UPDATE ON deals
    FOR EACH ROW
    EXECUTE FUNCTION update_deals_updated_at();

-- Enable Row Level Security (RLS)
ALTER TABLE deals ENABLE ROW LEVEL SECURITY;

-- Create RLS policies (assuming company-based access control)
CREATE POLICY "Users can view deals from their company" ON deals
    FOR SELECT USING (
        company_id = (
            SELECT company_id FROM users 
            WHERE id = auth.uid()
        )
    );

CREATE POLICY "Users can insert deals for their company" ON deals
    FOR INSERT WITH CHECK (
        company_id = (
            SELECT company_id FROM users 
            WHERE id = auth.uid()
        )
    );

CREATE POLICY "Users can update deals from their company" ON deals
    FOR UPDATE USING (
        company_id = (
            SELECT company_id FROM users 
            WHERE id = auth.uid()
        )
    );

CREATE POLICY "Users can delete deals from their company" ON deals
    FOR DELETE USING (
        company_id = (
            SELECT company_id FROM users 
            WHERE id = auth.uid()
        )
    );

-- Grant necessary permissions
GRANT ALL ON deals TO authenticated;
GRANT ALL ON deals TO service_role;

-- Add comment for documentation
COMMENT ON TABLE deals IS 'Deals table for tracking qualified leads through sales pipeline. Links to original leads via source_lead_id.';
COMMENT ON COLUMN deals.source_lead_id IS 'References the original lead record that was converted to this deal';
COMMENT ON COLUMN deals.stage IS 'Current stage in sales pipeline: Qualification, Demo, Proposal, Negotiation, Closed Won, Closed Lost';
COMMENT ON COLUMN deals.probability IS 'Percentage probability of closing this deal (0-100)';