-- Accounts Table Migration
-- This creates the accounts table for CRM system

-- Step 1: Create accounts table
CREATE TABLE IF NOT EXISTS accounts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    
    -- Core fields from Excel mapping
    account_name TEXT NOT NULL,
    industry TEXT,
    account_type TEXT CHECK (account_type IN ('Customer', 'Prospect', 'Partner', 'Competitor', 'Vendor', 'Other')),
    website TEXT,
    phone TEXT,
    address TEXT,
    description TEXT,
    
    -- Address fields (billing)
    billing_street TEXT,
    billing_city TEXT,
    billing_state TEXT,
    billing_country TEXT,
    billing_postal_code TEXT,
    
    -- Address fields (shipping)
    shipping_street TEXT,
    shipping_city TEXT,
    shipping_state TEXT,
    shipping_country TEXT,
    shipping_postal_code TEXT,
    
    -- Business information
    annual_revenue DECIMAL(15, 2),
    employee_count INTEGER,
    turnover_range TEXT,
    credit_days INTEGER,
    credit_amount DECIMAL(15, 2),
    
    -- Tax/Compliance fields
    gstin TEXT,
    pan_number TEXT,
    vat_tin TEXT,
    cst_number TEXT,
    
    -- Relationship fields
    parent_account_id UUID REFERENCES accounts(id),
    owner_id UUID REFERENCES users(id),
    
    -- Status and metadata
    status TEXT DEFAULT 'Active' CHECK (status IN ('Active', 'Inactive', 'Suspended')),
    source TEXT,
    rating TEXT CHECK (rating IN ('Hot', 'Warm', 'Cold')),
    
    -- Import tracking fields
    original_id TEXT, -- To track ID from old system
    original_created_by TEXT,
    original_modified_by TEXT,
    original_created_at TEXT,
    original_modified_at TEXT,
    assigned_to_names TEXT, -- Store multiple assignees as text for now
    
    -- System fields
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    
    -- Constraints
    CONSTRAINT unique_account_name_per_company UNIQUE(company_id, account_name)
);

-- Step 2: Create indexes for performance
CREATE INDEX idx_accounts_company_id ON accounts(company_id);
CREATE INDEX idx_accounts_owner_id ON accounts(owner_id);
CREATE INDEX idx_accounts_account_name ON accounts(account_name);
CREATE INDEX idx_accounts_billing_city ON accounts(billing_city);
CREATE INDEX idx_accounts_billing_state ON accounts(billing_state);
CREATE INDEX idx_accounts_industry ON accounts(industry);
CREATE INDEX idx_accounts_status ON accounts(status);
CREATE INDEX idx_accounts_created_at ON accounts(created_at DESC);
CREATE INDEX idx_accounts_parent_account_id ON accounts(parent_account_id);

-- Step 3: Enable Row Level Security
ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;

-- Step 4: Create RLS Policies

-- Super admins can access all accounts
CREATE POLICY "Super admins can access all accounts" ON accounts
    FOR ALL USING (
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_super_admin = true
        )
    );

-- Company admins can access accounts in their company
CREATE POLICY "Company admins can access company accounts" ON accounts
    FOR ALL USING (
        company_id IN (
            SELECT company_id FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_admin = true
        )
    );

-- Users can view accounts they own or are assigned to
CREATE POLICY "Users can view assigned accounts" ON accounts
    FOR SELECT USING (
        owner_id = auth.uid()
        OR 
        company_id IN (
            SELECT company_id FROM users 
            WHERE users.id = auth.uid()
        )
    );

-- Step 5: Create trigger for updated_at
CREATE TRIGGER update_accounts_updated_at BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Step 6: Create account activities table for tracking changes
CREATE TABLE IF NOT EXISTS account_activities (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(id),
    activity_type TEXT NOT NULL,
    description TEXT,
    old_value JSONB,
    new_value JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL
);

CREATE INDEX idx_account_activities_account_id ON account_activities(account_id);
CREATE INDEX idx_account_activities_created_at ON account_activities(created_at DESC);

-- Step 7: Create view for account summary with stats
CREATE OR REPLACE VIEW account_summary AS
SELECT 
    a.*,
    u.full_name as owner_name,
    u.email as owner_email,
    p.account_name as parent_account_name,
    COUNT(DISTINCT aa.id) as activity_count
FROM accounts a
LEFT JOIN users u ON a.owner_id = u.id
LEFT JOIN accounts p ON a.parent_account_id = p.id
LEFT JOIN account_activities aa ON a.id = aa.account_id
GROUP BY a.id, u.full_name, u.email, p.account_name;

-- Step 8: Create function to log account activities
CREATE OR REPLACE FUNCTION log_account_activity()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO account_activities(account_id, user_id, activity_type, description, new_value)
        VALUES (NEW.id, auth.uid(), 'created', 'Account created', to_jsonb(NEW));
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO account_activities(account_id, user_id, activity_type, description, old_value, new_value)
        VALUES (NEW.id, auth.uid(), 'updated', 'Account updated', to_jsonb(OLD), to_jsonb(NEW));
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Step 9: Create trigger for activity logging
CREATE TRIGGER log_account_changes
AFTER INSERT OR UPDATE ON accounts
FOR EACH ROW EXECUTE FUNCTION log_account_activity();

-- Step 10: Grant permissions
GRANT ALL ON accounts TO authenticated;
GRANT ALL ON account_activities TO authenticated;
GRANT SELECT ON account_summary TO authenticated;