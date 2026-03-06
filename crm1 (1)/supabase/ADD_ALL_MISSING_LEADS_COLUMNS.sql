-- =====================================================
-- ADD ALL MISSING COLUMNS TO LEADS TABLE
-- =====================================================
-- This script adds ALL the columns needed for the dynamic
-- leads form based on the field configurations
-- =====================================================

-- Basic Info Fields
ALTER TABLE leads ADD COLUMN IF NOT EXISTS lead_title VARCHAR(255);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS account UUID REFERENCES accounts(id) ON DELETE SET NULL;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS contact UUID REFERENCES contacts(id) ON DELETE SET NULL;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS first_name VARCHAR(100);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS last_name VARCHAR(100);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS department VARCHAR(150);

-- Lead Details Fields
ALTER TABLE leads ADD COLUMN IF NOT EXISTS lead_source VARCHAR(100);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS lead_type VARCHAR(100);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS lead_status VARCHAR(100);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS lead_stage VARCHAR(100);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS nurture_level VARCHAR(100);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS priority_level VARCHAR(50);

-- Contact Info Fields
ALTER TABLE leads ADD COLUMN IF NOT EXISTS email_address VARCHAR(255);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS phone_number VARCHAR(50);

-- Additional Info Fields
ALTER TABLE leads ADD COLUMN IF NOT EXISTS consent_status BOOLEAN DEFAULT false;

-- User tracking fields
ALTER TABLE leads ADD COLUMN IF NOT EXISTS userId UUID REFERENCES users(id) ON DELETE SET NULL;

-- Add indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_leads_account ON leads(account);
CREATE INDEX IF NOT EXISTS idx_leads_contact ON leads(contact);
CREATE INDEX IF NOT EXISTS idx_leads_first_name ON leads(first_name);
CREATE INDEX IF NOT EXISTS idx_leads_last_name ON leads(last_name);
CREATE INDEX IF NOT EXISTS idx_leads_email ON leads(email_address);
CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(lead_status);
CREATE INDEX IF NOT EXISTS idx_leads_stage ON leads(lead_stage);

-- Verification query
SELECT '✅ Checking which columns were added:' AS info;

SELECT
  column_name,
  data_type,
  is_nullable
FROM information_schema.columns
WHERE table_name = 'leads'
  AND column_name IN (
    'lead_title', 'account', 'contact', 'first_name', 'last_name', 'department',
    'lead_source', 'lead_type', 'lead_status', 'lead_stage',
    'nurture_level', 'priority_level', 'email_address', 'phone_number',
    'consent_status'
  )
ORDER BY column_name;

SELECT '✅ All missing columns added to leads table successfully!' AS status;
