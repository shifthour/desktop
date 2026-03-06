-- =====================================================
-- ADD ASSIGNED_TO COLUMN TO DEALS TABLE
-- =====================================================
-- This script adds the assigned_to column to deals table
-- to track which account/user is assigned to each deal
-- =====================================================

-- Add assigned_to column to deals table
ALTER TABLE deals ADD COLUMN IF NOT EXISTS assigned_to VARCHAR(255);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_deals_assigned_to ON deals(assigned_to);

-- Add comment
COMMENT ON COLUMN deals.assigned_to IS 'Account or user assigned to this deal';

-- Verification query
SELECT '✅ Checking assigned_to column in deals table:' AS info;

SELECT
  column_name,
  data_type,
  is_nullable
FROM information_schema.columns
WHERE table_name = 'deals'
  AND column_name = 'assigned_to';

SELECT '✅ assigned_to column added to deals table successfully!' AS status;
