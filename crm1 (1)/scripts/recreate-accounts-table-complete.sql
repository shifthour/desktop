-- Complete script to drop and recreate accounts table with new structure
-- WARNING: This will DELETE ALL existing accounts data
-- Make sure to backup if needed!

-- Step 1: Drop all dependent views and tables
DROP VIEW IF EXISTS account_summary CASCADE;
DROP VIEW IF EXISTS accounts_view CASCADE;
DROP VIEW IF EXISTS account_stats CASCADE;
DROP TABLE IF EXISTS account_activities CASCADE;
DROP TABLE IF EXISTS account_contacts CASCADE;
DROP TABLE IF EXISTS account_notes CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;

-- Step 2: Create the accounts table with the new structure
CREATE TABLE accounts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Basic Information
    account_name TEXT NOT NULL,
    account_type TEXT DEFAULT 'Customer',
    industry TEXT,
    website TEXT,
    phone TEXT,
    fax TEXT,
    
    -- Address Information
    billing_street TEXT,
    billing_city TEXT,
    billing_state TEXT,
    billing_country TEXT DEFAULT 'India',
    billing_postal_code TEXT,
    
    shipping_street TEXT,
    shipping_city TEXT,
    shipping_state TEXT,
    shipping_country TEXT DEFAULT 'India',
    shipping_postal_code TEXT,
    
    -- Additional Information
    country TEXT DEFAULT 'India', -- Main country field
    status TEXT DEFAULT 'Active',
    rating TEXT,
    ownership TEXT,
    
    -- System Fields
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
    owner_id UUID REFERENCES users(id),
    created_by UUID REFERENCES users(id),
    updated_by UUID REFERENCES users(id),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT unique_account_per_company UNIQUE(company_id, account_name)
);

-- Step 3: Create indexes for better performance
CREATE INDEX idx_accounts_company_id ON accounts(company_id);
CREATE INDEX idx_accounts_owner_id ON accounts(owner_id);
CREATE INDEX idx_accounts_status ON accounts(status);
CREATE INDEX idx_accounts_country ON accounts(country);
CREATE INDEX idx_accounts_billing_country ON accounts(billing_country);
CREATE INDEX idx_accounts_created_at ON accounts(created_at);
CREATE INDEX idx_accounts_account_name ON accounts(account_name);

-- Step 4: Enable Row Level Security
ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;

-- Step 5: Create RLS policies
-- Policy for viewing accounts (users can only see their company's accounts)
CREATE POLICY "Users can view their company accounts" ON accounts
    FOR SELECT
    USING (
        company_id IN (
            SELECT company_id FROM users WHERE id = auth.uid()
        )
    );

-- Policy for inserting accounts
CREATE POLICY "Users can insert accounts for their company" ON accounts
    FOR INSERT
    WITH CHECK (
        company_id IN (
            SELECT company_id FROM users WHERE id = auth.uid()
        )
    );

-- Policy for updating accounts
CREATE POLICY "Users can update their company accounts" ON accounts
    FOR UPDATE
    USING (
        company_id IN (
            SELECT company_id FROM users WHERE id = auth.uid()
        )
    );

-- Policy for deleting accounts
CREATE POLICY "Users can delete their company accounts" ON accounts
    FOR DELETE
    USING (
        company_id IN (
            SELECT company_id FROM users WHERE id = auth.uid()
        )
    );

-- Step 6: Create a trigger to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_accounts_updated_at BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Step 7: Create a simple view for account summary (without employee_count)
CREATE VIEW account_summary AS
SELECT 
    a.id,
    a.account_name,
    a.industry,
    a.status,
    a.billing_city,
    a.billing_state,
    a.billing_country,
    a.country,
    a.phone,
    a.website,
    a.company_id,
    a.owner_id,
    a.created_at,
    u.full_name as owner_name,
    u.email as owner_email
FROM accounts a
LEFT JOIN users u ON a.owner_id = u.id;

-- Step 8: Grant necessary permissions
GRANT ALL ON accounts TO authenticated;
GRANT ALL ON account_summary TO authenticated;

-- Step 9: Insert some sample data (optional - comment out if not needed)
-- This is just for testing, you can skip this if you're importing from Excel
/*
INSERT INTO accounts (
    account_name,
    industry,
    billing_city,
    billing_state,
    billing_country,
    country,
    phone,
    website,
    status,
    company_id,
    owner_id
) VALUES 
(
    'Sample Company Ltd',
    'Biotechnology',
    'Mumbai',
    'Maharashtra',
    'India',
    'India',
    '+91 9876543210',
    'www.samplecompany.com',
    'Active',
    '22adbd06-8ce1-49ea-9a03-d0b46720c624', -- Demo company ID
    '45b84401-ca13-4627-ac8f-42a11374633c'  -- Demo admin user ID
);
*/

-- Step 10: Verify the new structure
SELECT 
    column_name, 
    data_type, 
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_name = 'accounts' 
    AND table_schema = 'public'
ORDER BY ordinal_position;

-- Show table count
SELECT COUNT(*) as total_accounts FROM accounts;