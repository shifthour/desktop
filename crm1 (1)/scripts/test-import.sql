-- Test import with just 5 accounts
-- Run this first to test the process

-- Temporarily disable RLS
ALTER TABLE accounts DISABLE ROW LEVEL SECURITY;

-- Insert 5 test accounts
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
  '6dd33b6b-1524-4b77-b8ff-2b4a61aeff8f',
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
  '6dd33b6b-1524-4b77-b8ff-2b4a61aeff8f',
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
  '6dd33b6b-1524-4b77-b8ff-2b4a61aeff8f',
  'Active',
  'Customer'
);

-- Re-enable RLS
ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;

-- Check if test accounts were inserted
SELECT COUNT(*) as test_accounts FROM accounts 
WHERE company_id = '22adbd06-8ce1-49ea-9a03-d0b46720c624';