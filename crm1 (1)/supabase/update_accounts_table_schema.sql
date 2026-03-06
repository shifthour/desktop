-- =====================================================
-- Update Accounts Table Schema
-- Add missing columns for new field configurations
-- =====================================================

-- Add new columns if they don't exist
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS account_id TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS headquarters_address TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS main_phone TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS customer_segment TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS account_type TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS acct_industry TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS acct_sub_industry TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS account_status TEXT DEFAULT 'Active';

-- Additional optional fields
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS preferred_language TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS created_by_id UUID REFERENCES users(id);
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS last_modified_by_id UUID REFERENCES users(id);
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS last_modified_date TIMESTAMP WITH TIME ZONE;

-- Address fields (some may already exist)
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS shipping_address TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS shipping_street2 TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS billing_street2 TEXT;

-- Contact fields
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS primary_email TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS preferred_contact_method TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS email_opt_in TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS alternate_phone TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS website TEXT;

-- Business details
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS territory TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS communication_frequency TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS communication_channel TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS organization_size TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS vendor_qualification TEXT;

-- Financial fields
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS tax_id_number TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS cin TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS udyam_registration TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS gem_seller_id TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS tax_classification TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS preferred_currency TEXT DEFAULT 'INR';
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS revenue_currency TEXT DEFAULT 'INR';
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS acc_budget_capacity DECIMAL(15, 2);
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS payment_terms TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS credit_limit DECIMAL(15, 2);
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS credit_status TEXT;

-- Advanced fields
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS academic_affiliation TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS grant_funding_source TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS regulatory_authority TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS procurement_cycle TEXT;

-- Create indexes for commonly queried fields
CREATE INDEX IF NOT EXISTS idx_accounts_customer_segment ON accounts(customer_segment);
CREATE INDEX IF NOT EXISTS idx_accounts_account_type ON accounts(account_type);
CREATE INDEX IF NOT EXISTS idx_accounts_acct_industry ON accounts(acct_industry);
CREATE INDEX IF NOT EXISTS idx_accounts_territory ON accounts(territory);
CREATE INDEX IF NOT EXISTS idx_accounts_primary_email ON accounts(primary_email);

-- Add check constraints for mandatory fields
ALTER TABLE accounts DROP CONSTRAINT IF EXISTS chk_customer_segment;
ALTER TABLE accounts ADD CONSTRAINT chk_customer_segment
    CHECK (customer_segment IS NULL OR customer_segment IN (
        'Government', 'Academic', 'Healthcare_Public', 'Healthcare_Private',
        'Pharma', 'Biotech', 'Manufacturing', 'Research_Private'
    ));

ALTER TABLE accounts DROP CONSTRAINT IF EXISTS chk_account_type;
ALTER TABLE accounts ADD CONSTRAINT chk_account_type
    CHECK (account_type IS NULL OR account_type IN (
        'Govt Industry', 'Govt Institution', 'Private Industry', 'Private Institution'
    ));

ALTER TABLE accounts DROP CONSTRAINT IF EXISTS chk_acct_industry;
ALTER TABLE accounts ADD CONSTRAINT chk_acct_industry
    CHECK (acct_industry IS NULL OR acct_industry IN (
        'Research', 'Pharma Biopharma', 'Chemicals Petrochemicals', 'Healthcare Diagnostics',
        'Instrument Manufacturing', 'Environmental Testing', 'Education', 'Forensics', 'Testing Labs'
    ));

-- Create trigger to auto-generate account_id if not provided
CREATE OR REPLACE FUNCTION generate_account_id()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.account_id IS NULL OR NEW.account_id = '' THEN
        -- Generate account ID in format: ACC-YYYY-NNNN
        NEW.account_id := 'ACC-' || TO_CHAR(NOW(), 'YYYY') || '-' ||
                          LPAD(NEXTVAL('accounts_seq')::TEXT, 4, '0');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create sequence if it doesn't exist
CREATE SEQUENCE IF NOT EXISTS accounts_seq START 1000;

-- Create trigger
DROP TRIGGER IF EXISTS trigger_generate_account_id ON accounts;
CREATE TRIGGER trigger_generate_account_id
    BEFORE INSERT ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION generate_account_id();

-- Update trigger for last_modified fields
CREATE OR REPLACE FUNCTION update_account_modified_fields()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_modified_date := NOW();
    NEW.last_modified_by_id := auth.uid();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_account_modified ON accounts;
CREATE TRIGGER trigger_update_account_modified
    BEFORE UPDATE ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION update_account_modified_fields();
