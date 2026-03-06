-- =====================================================
-- ADD MISSING COLUMNS TO LEADS TABLE
-- =====================================================
-- This script adds the new columns needed for the dynamic
-- leads form with account/contact dropdowns
-- =====================================================

-- Add 'account' column (stores account ID from accounts table)
ALTER TABLE leads
ADD COLUMN IF NOT EXISTS account UUID REFERENCES accounts(id) ON DELETE SET NULL;

-- Add 'contact' column (stores contact ID from contacts table)
ALTER TABLE leads
ADD COLUMN IF NOT EXISTS contact UUID REFERENCES contacts(id) ON DELETE SET NULL;

-- Add 'first_name' column
ALTER TABLE leads
ADD COLUMN IF NOT EXISTS first_name VARCHAR(100);

-- Add 'last_name' column
ALTER TABLE leads
ADD COLUMN IF NOT EXISTS last_name VARCHAR(100);

-- Add 'phone_number' column
ALTER TABLE leads
ADD COLUMN IF NOT EXISTS phone_number VARCHAR(50);

-- Add 'email_address' column
ALTER TABLE leads
ADD COLUMN IF NOT EXISTS email_address VARCHAR(255);

-- Add 'consent_status' column (for GDPR/marketing consent)
ALTER TABLE leads
ADD COLUMN IF NOT EXISTS consent_status BOOLEAN DEFAULT false;

-- Add indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_leads_account ON leads(account);
CREATE INDEX IF NOT EXISTS idx_leads_contact ON leads(contact);
CREATE INDEX IF NOT EXISTS idx_leads_first_name ON leads(first_name);
CREATE INDEX IF NOT EXISTS idx_leads_last_name ON leads(last_name);

-- Verification query
SELECT
  column_name,
  data_type,
  is_nullable
FROM information_schema.columns
WHERE table_name = 'leads'
  AND column_name IN ('account', 'contact', 'first_name', 'last_name', 'phone_number', 'email_address', 'consent_status')
ORDER BY column_name;

SELECT '✅ Missing columns added to leads table successfully!' AS status;
