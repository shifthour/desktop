-- Test import with CORRECT user and company IDs
-- Temporarily disable RLS
ALTER TABLE accounts DISABLE ROW LEVEL SECURITY;

-- Insert 3 test accounts with correct IDs
INSERT INTO accounts (
  company_id, account_name, industry, website, phone, description,
  billing_city, billing_state, billing_country,
  owner_id, status, account_type
) VALUES
(
  '22adbd06-8ce1-49ea-9a03-d0b46720c624',
  '3B BlackBio Biotech India Ltd.',
  'Biotechnology',
  'www.3bblackbio.com',
  '+91-755-4077847',
  'Biotech company in Bhopal',
  'BHOPAL',
  'Madhya Pradesh',
  'India',
  '45b84401-ca13-4627-ac8f-42a11374633c',
  'Active',
  'Customer'
),
(
  '22adbd06-8ce1-49ea-9a03-d0b46720c624',
  '3M India Ltd.',
  'Manufacturing',
  NULL,
  NULL,
  'Multinational manufacturing company',
  'BANGALORE',
  'Karnataka',
  'India',
  '45b84401-ca13-4627-ac8f-42a11374633c',
  'Active',
  'Customer'
),
(
  '22adbd06-8ce1-49ea-9a03-d0b46720c624',
  'A E International',
  NULL,
  NULL,
  NULL,
  'International trading company',
  NULL,
  'DHAKA',
  'Bangladesh',
  '45b84401-ca13-4627-ac8f-42a11374633c',
  'Active',
  'Customer'
);

-- Re-enable RLS
ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;

-- Check if test accounts were inserted
SELECT COUNT(*) as test_accounts FROM accounts 
WHERE company_id = '22adbd06-8ce1-49ea-9a03-d0b46720c624';

-- Show the inserted accounts
SELECT account_name, billing_city, billing_state, billing_country 
FROM accounts 
WHERE company_id = '22adbd06-8ce1-49ea-9a03-d0b46720c624';