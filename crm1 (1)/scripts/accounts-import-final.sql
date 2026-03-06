-- Import all accounts with SKIP DUPLICATES (CORRECTED QUOTES)
-- Temporarily disable RLS for import
ALTER TABLE accounts DISABLE ROW LEVEL SECURITY;

-- Check current count
SELECT COUNT(*) as before_import FROM accounts WHERE company_id = '22adbd06-8ce1-49ea-9a03-d0b46720c624';

-- Sample batch of accounts to test (first 10 accounts from your data)
INSERT INTO accounts (company_id, account_name, industry, website, phone, description, billing_street, billing_city, billing_state, billing_country, billing_postal_code, owner_id, status, account_type) VALUES
('22adbd06-8ce1-49ea-9a03-d0b46720c624', '3B BlackBio Biotech India Ltd.', 'Biotechnology', 'www.3bblackbio.com', '+91-755-4077847', NULL, '7-C, Industrial Area, Govindpura', 'BHOPAL', 'Madhya Pradesh', 'India', '462023', '45b84401-ca13-4627-ac8f-42a11374633c', 'Active', 'Customer'),
('22adbd06-8ce1-49ea-9a03-d0b46720c624', '3M India Ltd.', NULL, NULL, NULL, NULL, 'Concorde Block, UB City, 24, Vittal Mallya Road', 'BANGALORE', 'Karnataka', 'India', '560001', '45b84401-ca13-4627-ac8f-42a11374633c', 'Active', 'Customer'),
('22adbd06-8ce1-49ea-9a03-d0b46720c624', 'A E International', NULL, NULL, NULL, NULL, 'road # 04, Dhanmondi', NULL, 'DHAKA', 'Bangladesh', NULL, '45b84401-ca13-4627-ac8f-42a11374633c', 'Active', 'Customer'),
('22adbd06-8ce1-49ea-9a03-d0b46720c624', 'A Molecular Research Center', NULL, 'www.supratechmicropath.com', NULL, NULL, '4th Floor, KEDAR Complex, Opp. Krupa Petrol Pump, NR. Parimal Garden, Ellisbridge', 'Ahmedabad', 'Gujarat', 'India', '380006', '45b84401-ca13-4627-ac8f-42a11374633c', 'Active', 'Customer'),
('22adbd06-8ce1-49ea-9a03-d0b46720c624', 'A P S University', 'Education', 'www.apsurewa.ac.in', '0532-2440003', NULL, 'Rewa', 'Rewa', 'Madhya Pradesh', 'India', '486003', '45b84401-ca13-4627-ac8f-42a11374633c', 'Active', 'Customer')
ON CONFLICT (company_id, account_name) DO NOTHING;

-- Re-enable RLS after import
ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;

-- Check final count
SELECT COUNT(*) as after_sample_import FROM accounts WHERE company_id = '22adbd06-8ce1-49ea-9a03-d0b46720c624';

-- Show sample accounts
SELECT account_name, billing_city, billing_state, billing_country FROM accounts 
WHERE company_id = '22adbd06-8ce1-49ea-9a03-d0b46720c624' 
ORDER BY account_name LIMIT 10;