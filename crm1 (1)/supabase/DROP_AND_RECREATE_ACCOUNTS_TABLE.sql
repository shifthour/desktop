-- =====================================================
-- DROP AND RECREATE ACCOUNTS TABLE
-- WARNING: This will delete all existing account data!
-- =====================================================

-- Step 1: Drop dependent objects
DROP TRIGGER IF EXISTS log_account_changes ON accounts;
DROP TRIGGER IF EXISTS update_accounts_updated_at ON accounts;
DROP TRIGGER IF EXISTS trigger_generate_account_id ON accounts;
DROP TRIGGER IF EXISTS trigger_update_account_modified ON accounts;

DROP VIEW IF EXISTS account_summary;

DROP TABLE IF EXISTS account_activities CASCADE;

-- Step 2: Drop the accounts table
DROP TABLE IF EXISTS accounts CASCADE;

-- Step 3: Drop sequence if exists
DROP SEQUENCE IF EXISTS accounts_seq;

-- Step 4: Recreate accounts table with ALL fields
CREATE TABLE accounts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,

    -- ==========================================
    -- MANDATORY FIELDS (9)
    -- ==========================================
    account_id TEXT,
    account_name TEXT NOT NULL,
    headquarters_address TEXT,
    main_phone TEXT,
    customer_segment TEXT CHECK (customer_segment IN (
        'Government', 'Academic', 'Healthcare_Public', 'Healthcare_Private',
        'Pharma', 'Biotech', 'Manufacturing', 'Research_Private'
    )),
    account_type TEXT CHECK (account_type IN (
        'Customer', 'Manufacturer', 'Distributor', 'End_Customer', 'Research_Institution', 'Hospital', 'University', 'Government_Department'
    )),
    acct_industry TEXT CHECK (acct_industry IN (
        'Research', 'Pharma Biopharma', 'Chemicals Petrochemicals', 'Healthcare Diagnostics',
        'Instrument Manufacturing', 'Environmental Testing', 'Education', 'Forensics', 'Testing Labs'
    )),
    acct_sub_industry TEXT,
    account_status TEXT DEFAULT 'Active' CHECK (account_status IN ('Active', 'Inactive', 'Prospect', 'Customer', 'Suspended')),

    -- ==========================================
    -- BASIC INFO (OPTIONAL)
    -- ==========================================
    preferred_language TEXT,

    -- ==========================================
    -- ADDRESSES
    -- ==========================================
    -- Shipping Address
    shipping_address TEXT,
    shipping_street TEXT,
    shipping_street2 TEXT,
    shipping_city TEXT,
    shipping_state TEXT,
    shipping_country TEXT,
    shipping_postal_code TEXT,

    -- Billing Address
    billing_address TEXT,
    billing_street TEXT,
    billing_street2 TEXT,
    billing_city TEXT,
    billing_state TEXT,
    billing_country TEXT,
    billing_postal_code TEXT,

    -- ==========================================
    -- CONTACT INFORMATION
    -- ==========================================
    primary_email TEXT,
    preferred_contact_method TEXT,
    email_opt_in TEXT,
    alternate_phone TEXT,
    website TEXT,
    phone TEXT,
    address TEXT,
    description TEXT,

    -- ==========================================
    -- BUSINESS DETAILS
    -- ==========================================
    territory TEXT,
    communication_frequency TEXT,
    communication_channel TEXT,
    organization_size TEXT,
    vendor_qualification TEXT,
    industry TEXT,
    turnover_range TEXT,
    credit_days INTEGER,
    credit_amount DECIMAL(15, 2),
    employee_count INTEGER,
    business_model TEXT,
    region TEXT,
    account_source TEXT,
    lead_source TEXT,

    -- ==========================================
    -- FINANCIAL INFORMATION
    -- ==========================================
    tax_id_number TEXT,
    gstin TEXT,
    pan_number TEXT,
    cin TEXT,
    udyam_registration TEXT,
    gem_seller_id TEXT,
    tax_classification TEXT,
    preferred_currency TEXT DEFAULT 'INR',
    revenue_currency TEXT DEFAULT 'INR',
    annual_revenue DECIMAL(15, 2),
    acc_budget_capacity DECIMAL(15, 2),
    payment_terms TEXT,
    credit_limit DECIMAL(15, 2),
    credit_status TEXT,
    vat_tin TEXT,
    cst_number TEXT,

    -- ==========================================
    -- ADVANCED FIELDS
    -- ==========================================
    academic_affiliation TEXT,
    grant_funding_source TEXT,
    regulatory_authority TEXT,
    procurement_cycle TEXT,

    -- ==========================================
    -- RELATIONSHIP FIELDS
    -- ==========================================
    parent_account_id UUID REFERENCES accounts(id),
    owner_id UUID REFERENCES users(id),

    -- ==========================================
    -- STATUS AND METADATA
    -- ==========================================
    status TEXT DEFAULT 'Active' CHECK (status IN ('Active', 'Inactive', 'Suspended')),
    source TEXT,
    rating TEXT CHECK (rating IN ('Hot', 'Warm', 'Cold')),

    -- ==========================================
    -- IMPORT TRACKING FIELDS
    -- ==========================================
    original_id TEXT,
    original_created_by TEXT,
    original_modified_by TEXT,
    original_created_at TEXT,
    original_modified_at TEXT,
    assigned_to_names TEXT,

    -- ==========================================
    -- SYSTEM FIELDS
    -- ==========================================
    created_by UUID REFERENCES users(id),
    created_by_id UUID REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    created_date TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    last_modified_by_id UUID REFERENCES users(id),
    last_modified_date TIMESTAMP WITH TIME ZONE,

    -- ==========================================
    -- CONSTRAINTS
    -- ==========================================
    CONSTRAINT unique_account_name_per_company UNIQUE(company_id, account_name)
);

-- Step 5: Create indexes for performance
CREATE INDEX idx_accounts_company_id ON accounts(company_id);
CREATE INDEX idx_accounts_owner_id ON accounts(owner_id);
CREATE INDEX idx_accounts_account_name ON accounts(account_name);
CREATE INDEX idx_accounts_billing_city ON accounts(billing_city);
CREATE INDEX idx_accounts_billing_state ON accounts(billing_state);
CREATE INDEX idx_accounts_industry ON accounts(industry);
CREATE INDEX idx_accounts_status ON accounts(status);
CREATE INDEX idx_accounts_created_at ON accounts(created_at DESC);
CREATE INDEX idx_accounts_parent_account_id ON accounts(parent_account_id);
CREATE INDEX idx_accounts_customer_segment ON accounts(customer_segment);
CREATE INDEX idx_accounts_account_type ON accounts(account_type);
CREATE INDEX idx_accounts_acct_industry ON accounts(acct_industry);
CREATE INDEX idx_accounts_territory ON accounts(territory);
CREATE INDEX idx_accounts_primary_email ON accounts(primary_email);

-- Step 6: Enable Row Level Security
ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;

-- Step 7: Create RLS Policies
CREATE POLICY "Super admins can access all accounts" ON accounts
    FOR ALL USING (
        EXISTS (
            SELECT 1 FROM users
            WHERE users.id = auth.uid()
            AND users.is_super_admin = true
        )
    );

CREATE POLICY "Company admins can access company accounts" ON accounts
    FOR ALL USING (
        company_id IN (
            SELECT company_id FROM users
            WHERE users.id = auth.uid()
            AND users.is_admin = true
        )
    );

CREATE POLICY "Users can view assigned accounts" ON accounts
    FOR SELECT USING (
        owner_id = auth.uid()
        OR
        company_id IN (
            SELECT company_id FROM users
            WHERE users.id = auth.uid()
        )
    );

-- Step 8: Create sequence for account_id
CREATE SEQUENCE accounts_seq START 1000;

-- Step 9: Create trigger to auto-generate account_id
CREATE OR REPLACE FUNCTION generate_account_id()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.account_id IS NULL OR NEW.account_id = '' THEN
        NEW.account_id := 'ACC-' || TO_CHAR(NOW(), 'YYYY') || '-' ||
                          LPAD(NEXTVAL('accounts_seq')::TEXT, 4, '0');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_generate_account_id
    BEFORE INSERT ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION generate_account_id();

-- Step 10: Create trigger for updated_at
CREATE TRIGGER update_accounts_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Step 11: Create trigger for last_modified fields
CREATE OR REPLACE FUNCTION update_account_modified_fields()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_modified_date := NOW();
    NEW.last_modified_by_id := auth.uid();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_account_modified
    BEFORE UPDATE ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION update_account_modified_fields();

-- Step 12: Recreate account_activities table
CREATE TABLE account_activities (
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

-- Step 13: Create view for account summary
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

-- Step 14: Create function to log account activities
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

CREATE TRIGGER log_account_changes
    AFTER INSERT OR UPDATE ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION log_account_activity();

-- Step 15: Grant permissions
GRANT ALL ON accounts TO authenticated;
GRANT ALL ON account_activities TO authenticated;
GRANT SELECT ON account_summary TO authenticated;

-- =====================================================
-- DONE! Accounts table recreated successfully
-- =====================================================
-- Now run the field configuration script:
-- EXECUTE_THIS_COMPLETE_SETUP.sql
-- =====================================================
