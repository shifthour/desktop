-- =====================================================
-- FIX LEAD ACCOUNT AND CONTACT IDs
-- =====================================================
-- This script updates existing leads to set account_id and contact_id
-- based on the account_name and contact_name values
-- =====================================================

-- First, clean non-breaking spaces (CHR(160)) from contact_name and account_name
UPDATE leads
SET contact_name = REPLACE(contact_name, CHR(160), ' ')
WHERE contact_name LIKE '%' || CHR(160) || '%';

UPDATE leads
SET account_name = REPLACE(account_name, CHR(160), ' ')
WHERE account_name LIKE '%' || CHR(160) || '%';

-- Update account_id based on account_name
UPDATE leads l
SET account_id = a.id
FROM accounts a
WHERE l.account_name IS NOT NULL
  AND l.account_id IS NULL
  AND LOWER(TRIM(a.account_name)) = LOWER(TRIM(l.account_name))
  AND a.company_id = l.company_id;

-- Update contact_id based on contact_name (matching against first_name + last_name)
UPDATE leads l
SET contact_id = c.id
FROM contacts c
WHERE l.contact_name IS NOT NULL
  AND l.contact_id IS NULL
  AND LOWER(TRIM(CONCAT(c.first_name, ' ', c.last_name))) = LOWER(TRIM(l.contact_name))
  AND c.company_id = l.company_id;

-- Verification
SELECT '✅ Updated leads with account IDs:' AS status;
SELECT
  id,
  account_name,
  account_id,
  contact_name,
  contact_id
FROM leads
WHERE account_name IS NOT NULL OR contact_name IS NOT NULL
ORDER BY created_at DESC
LIMIT 10;

SELECT '✅ FIX COMPLETE!' AS status;
