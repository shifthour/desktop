-- =====================================================
-- ADD CUSTOM FIELDS COLUMN TO LEADS TABLE
-- =====================================================
-- This adds a JSONB column to store optional/custom fields
-- that are dynamically configured through the admin panel
-- =====================================================

-- Add custom_fields column to leads table
ALTER TABLE leads
ADD COLUMN IF NOT EXISTS custom_fields JSONB DEFAULT '{}'::jsonb;

-- Create index for better performance when querying custom fields
CREATE INDEX IF NOT EXISTS idx_leads_custom_fields ON leads USING gin(custom_fields);

-- Verification
SELECT '✅ Checking custom_fields column in leads table:' AS info;

SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'leads' AND column_name = 'custom_fields';

SELECT '✅ Custom fields column added successfully!' AS status;
