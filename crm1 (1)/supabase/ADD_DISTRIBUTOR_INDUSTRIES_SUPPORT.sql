-- =====================================================
-- ADD DISTRIBUTOR MULTIPLE INDUSTRIES SUPPORT
-- =====================================================
-- This script adds support for distributors to have
-- multiple industry-subindustry pairs
-- =====================================================

-- Step 1: Add industries JSONB column to accounts table
ALTER TABLE accounts
ADD COLUMN IF NOT EXISTS industries JSONB DEFAULT '[]'::jsonb;

-- Add comment to explain the structure
COMMENT ON COLUMN accounts.industries IS
'Array of industry-subindustry pairs for distributor accounts.
Structure: [{"industry": "Healthcare", "subIndustry": "Medical Devices"}, ...]
For non-distributor accounts, this field should be empty or null.';

-- Step 2: Create index for better query performance
CREATE INDEX IF NOT EXISTS idx_accounts_industries ON accounts USING GIN (industries);

-- Step 3: Verification
SELECT
    column_name,
    data_type,
    column_default,
    is_nullable
FROM information_schema.columns
WHERE table_name = 'accounts'
AND column_name = 'industries';

SELECT '✅ DISTRIBUTOR INDUSTRIES COLUMN ADDED SUCCESSFULLY!' AS status;

-- Step 4: Example data structure
SELECT '📋 Example industries data structure:' AS info;
SELECT '[
  {"industry": "Healthcare", "subIndustry": "Medical Devices"},
  {"industry": "Manufacturing", "subIndustry": "Electronics"},
  {"industry": "Technology", "subIndustry": "Software"}
]'::jsonb AS example_industries_format;
