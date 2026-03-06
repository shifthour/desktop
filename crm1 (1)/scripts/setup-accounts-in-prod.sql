-- Setup accounts table in production Supabase
-- Run this in the Supabase SQL editor

-- Create accounts table if it doesn't exist
CREATE TABLE IF NOT EXISTS accounts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    
    -- Basic Information
    account_name TEXT NOT NULL,
    industry TEXT,
    account_type TEXT DEFAULT 'Customer',
    website TEXT,
    phone TEXT,
    description TEXT,
    
    -- Address Information
    billing_street TEXT,
    billing_city TEXT,
    billing_state TEXT,
    billing_country TEXT,
    billing_postal_code TEXT,
    shipping_street TEXT,
    shipping_city TEXT,
    shipping_state TEXT,
    shipping_country TEXT,
    shipping_postal_code TEXT,
    
    -- Financial Information
    annual_revenue DECIMAL(15,2),
    employee_count INTEGER,
    turnover_range TEXT,
    credit_days INTEGER,
    credit_amount DECIMAL(15,2),
    
    -- Tax Information
    gstin TEXT,
    pan_number TEXT,
    vat_tin TEXT,
    cst_number TEXT,
    
    -- Relationships
    parent_account_id UUID REFERENCES accounts(id),
    owner_id UUID REFERENCES users(id),
    
    -- Status and Classification
    status TEXT DEFAULT 'Active',
    source TEXT,
    rating TEXT,
    
    -- Historical data (for migration)
    original_id TEXT,
    original_created_by TEXT,
    original_modified_by TEXT,
    original_created_at TIMESTAMPTZ,
    original_modified_at TIMESTAMPTZ,
    assigned_to_names TEXT,
    
    -- System fields
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Unique constraint
    CONSTRAINT unique_account_name_per_company UNIQUE(company_id, account_name)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_accounts_company_id ON accounts(company_id);
CREATE INDEX IF NOT EXISTS idx_accounts_owner_id ON accounts(owner_id);
CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status);
CREATE INDEX IF NOT EXISTS idx_accounts_industry ON accounts(industry);
CREATE INDEX IF NOT EXISTS idx_accounts_created_at ON accounts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_accounts_account_name ON accounts(account_name);
CREATE INDEX IF NOT EXISTS idx_accounts_billing_city ON accounts(billing_city);

-- Enable RLS
ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;

-- Create RLS policies
-- Allow service role full access (for API calls)
CREATE POLICY "Allow service role access" ON accounts
    FOR ALL
    TO service_role
    USING (true);

-- Allow authenticated users to see accounts from their company
CREATE POLICY "Users can view company accounts" ON accounts
    FOR SELECT
    TO authenticated
    USING (
        company_id IN (
            SELECT company_id FROM users 
            WHERE users.id = auth.uid()
        )
    );

-- Allow company admins to manage accounts in their company
CREATE POLICY "Company admins can manage accounts" ON accounts
    FOR ALL
    TO authenticated
    USING (
        company_id IN (
            SELECT company_id FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_admin = true
        )
    );

-- Create trigger for updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_accounts_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Check if setup was successful
SELECT 'Accounts table created successfully!' as message;
SELECT COUNT(*) as existing_accounts_count FROM accounts;