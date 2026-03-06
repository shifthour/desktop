-- =====================================================
-- COMPLETE CONTACTS MANAGEMENT SYSTEM SETUP
-- =====================================================
-- This script creates:
-- 1. contacts table with all mandatory and optional fields
-- 2. contact_field_configurations table
-- 3. Seeds default field configurations
-- 4. Sets up proper constraints and indexes
-- =====================================================

-- Step 1: Create contacts table
CREATE TABLE IF NOT EXISTS contacts (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  contact_id VARCHAR(50) UNIQUE, -- Auto-generated contact ID (e.g., CONT-001)

  -- Foreign Keys
  company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  account_id UUID REFERENCES accounts(id) ON DELETE SET NULL, -- Which account/company this contact belongs to
  owner_id UUID REFERENCES users(id) ON DELETE SET NULL, -- Who owns/manages this contact
  created_by UUID REFERENCES users(id) ON DELETE SET NULL,

  -- MANDATORY FIELDS
  company_name VARCHAR(255) NOT NULL, -- Will be populated from selected account
  first_name VARCHAR(100) NOT NULL,
  last_name VARCHAR(100) NOT NULL,
  email_primary VARCHAR(255) NOT NULL,
  phone_mobile VARCHAR(50) NOT NULL,
  lifecycle_stage VARCHAR(100) NOT NULL CHECK (lifecycle_stage IN ('QUALIFIED LEAD', 'Contact', 'Lead', 'MQL', 'SQL', 'Opportunity', 'Customer', 'Evangelist', 'Former Customer')),
  current_contact_status VARCHAR(50) NOT NULL DEFAULT 'Active' CHECK (current_contact_status IN ('Active', 'Inactive', 'Do Not Contact', 'Bounced', 'Unsubscribed')),

  -- OPTIONAL FIELDS
  email_secondary VARCHAR(255),
  phone_work VARCHAR(50),
  job_title VARCHAR(150),
  preferred_contact_method VARCHAR(50) CHECK (preferred_contact_method IN ('Email', 'Phone', 'SMS', 'LinkedIn', 'In-Person', 'Video Call')),
  preferred_contact_time VARCHAR(100),

  -- Address fields
  address_street TEXT,
  address_city VARCHAR(100),
  address_state VARCHAR(100),
  address_postal_code VARCHAR(20),
  country VARCHAR(100),

  -- Social and Web
  linkedin_url VARCHAR(255),
  website_url VARCHAR(255),
  twitter_handle VARCHAR(100),
  profile_image_url TEXT,

  -- System fields
  created_at TIMESTAMP DEFAULT NOW(),
  modified_date TIMESTAMP DEFAULT NOW(),

  -- Indexes for better performance
  CONSTRAINT contacts_email_company_unique UNIQUE (email_primary, company_id)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_contacts_company_id ON contacts(company_id);
CREATE INDEX IF NOT EXISTS idx_contacts_account_id ON contacts(account_id);
CREATE INDEX IF NOT EXISTS idx_contacts_owner_id ON contacts(owner_id);
CREATE INDEX IF NOT EXISTS idx_contacts_email ON contacts(email_primary);
CREATE INDEX IF NOT EXISTS idx_contacts_lifecycle ON contacts(lifecycle_stage);
CREATE INDEX IF NOT EXISTS idx_contacts_status ON contacts(current_contact_status);

-- Step 2: Create contact_field_configurations table
CREATE TABLE IF NOT EXISTS contact_field_configurations (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  field_name VARCHAR(100) NOT NULL,
  field_label VARCHAR(150) NOT NULL,
  field_type VARCHAR(50) NOT NULL, -- text, email, tel, select, textarea, url, date
  is_mandatory BOOLEAN DEFAULT false,
  is_enabled BOOLEAN DEFAULT true,
  field_section VARCHAR(100) NOT NULL, -- basic_info, contact, address, professional, social, advanced
  display_order INTEGER DEFAULT 0,
  field_options TEXT[], -- For dropdown/select fields
  placeholder VARCHAR(255),
  validation_rules JSONB,
  help_text TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),

  CONSTRAINT contact_field_company_unique UNIQUE (field_name, company_id)
);

CREATE INDEX IF NOT EXISTS idx_contact_fields_company ON contact_field_configurations(company_id);
CREATE INDEX IF NOT EXISTS idx_contact_fields_section ON contact_field_configurations(field_section);

-- Step 3: Function to generate contact ID
CREATE OR REPLACE FUNCTION generate_contact_id()
RETURNS TRIGGER AS $$
DECLARE
  next_id INTEGER;
  new_contact_id VARCHAR(50);
BEGIN
  -- Get the next sequence number for this company
  SELECT COALESCE(MAX(CAST(SUBSTRING(contact_id FROM 6) AS INTEGER)), 0) + 1
  INTO next_id
  FROM contacts
  WHERE company_id = NEW.company_id;

  -- Generate the contact ID (e.g., CONT-001)
  new_contact_id := 'CONT-' || LPAD(next_id::TEXT, 3, '0');

  NEW.contact_id := new_contact_id;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for auto-generating contact_id
DROP TRIGGER IF EXISTS trigger_generate_contact_id ON contacts;
CREATE TRIGGER trigger_generate_contact_id
  BEFORE INSERT ON contacts
  FOR EACH ROW
  WHEN (NEW.contact_id IS NULL)
  EXECUTE FUNCTION generate_contact_id();

-- Step 4: Seed default field configurations for all companies
-- This will insert field configurations for each company

-- BASIC INFORMATION SECTION (Mandatory Fields)
INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'company_name',
  'Company/Account Name',
  'select',
  true,
  true,
  'basic_info',
  1,
  'Select account',
  'Select the account this contact belongs to'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'company_name' AND company_id = c.id
);

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'first_name',
  'First Name',
  'text',
  true,
  true,
  'basic_info',
  2,
  'Enter first name',
  'Contact''s first name'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'first_name' AND company_id = c.id
);

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'last_name',
  'Last Name',
  'text',
  true,
  true,
  'basic_info',
  3,
  'Enter last name',
  'Contact''s last name'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'last_name' AND company_id = c.id
);

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'email_primary',
  'Primary Email',
  'email',
  true,
  true,
  'basic_info',
  4,
  'Enter primary email',
  'Primary email address for contact'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'email_primary' AND company_id = c.id
);

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'phone_mobile',
  'Mobile Phone',
  'tel',
  true,
  true,
  'basic_info',
  5,
  'Enter mobile number',
  'Primary mobile phone number'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'phone_mobile' AND company_id = c.id
);

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, field_options, placeholder, help_text)
SELECT
  c.id,
  'lifecycle_stage',
  'Lifecycle Stage',
  'select',
  true,
  true,
  'basic_info',
  6,
  ARRAY['QUALIFIED LEAD', 'Contact', 'Lead', 'MQL', 'SQL', 'Opportunity', 'Customer', 'Evangelist', 'Former Customer'],
  'Select lifecycle stage',
  'Current stage in customer lifecycle'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'lifecycle_stage' AND company_id = c.id
);

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, field_options, placeholder, help_text)
SELECT
  c.id,
  'current_contact_status',
  'Contact Status',
  'select',
  true,
  true,
  'basic_info',
  7,
  ARRAY['Active', 'Inactive', 'Do Not Contact', 'Bounced', 'Unsubscribed'],
  'Select contact status',
  'Current status of this contact'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'current_contact_status' AND company_id = c.id
);

-- OPTIONAL FIELDS

-- Contact Information Section
INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'email_secondary',
  'Secondary Email',
  'email',
  false,
  true,
  'contact',
  10,
  'Enter secondary email',
  'Alternative email address'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'email_secondary' AND company_id = c.id
);

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'phone_work',
  'Work Phone',
  'tel',
  false,
  true,
  'contact',
  11,
  'Enter work phone number',
  'Office/work phone number'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'phone_work' AND company_id = c.id
);

-- Professional Information Section
INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'job_title',
  'Job Title',
  'text',
  false,
  true,
  'professional',
  20,
  'Enter job title',
  'Current job title or role'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'job_title' AND company_id = c.id
);

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, field_options, placeholder, help_text)
SELECT
  c.id,
  'preferred_contact_method',
  'Preferred Contact Method',
  'select',
  false,
  true,
  'professional',
  21,
  ARRAY['Email', 'Phone', 'SMS', 'LinkedIn', 'In-Person', 'Video Call'],
  'Select preferred method',
  'How this contact prefers to be reached'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'preferred_contact_method' AND company_id = c.id
);

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'preferred_contact_time',
  'Preferred Contact Time',
  'text',
  false,
  true,
  'professional',
  22,
  'e.g., Weekdays 9AM-5PM',
  'Best time to contact'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'preferred_contact_time' AND company_id = c.id
);

-- Address Section
INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'address_street',
  'Street Address',
  'textarea',
  false,
  true,
  'address',
  30,
  'Enter street address',
  'Street address or location'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'address_street' AND company_id = c.id
);

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'address_city',
  'City',
  'text',
  false,
  true,
  'address',
  31,
  'Enter city',
  'City name'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'address_city' AND company_id = c.id
);

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'address_state',
  'State/Province',
  'text',
  false,
  true,
  'address',
  32,
  'Enter state or province',
  'State or province'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'address_state' AND company_id = c.id
);

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'address_postal_code',
  'Postal Code',
  'text',
  false,
  true,
  'address',
  33,
  'Enter postal/ZIP code',
  'Postal or ZIP code'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'address_postal_code' AND company_id = c.id
);

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'country',
  'Country',
  'text',
  false,
  true,
  'address',
  34,
  'Enter country',
  'Country'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'country' AND company_id = c.id
);

-- Social Media Section
INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'linkedin_url',
  'LinkedIn URL',
  'url',
  false,
  true,
  'social',
  40,
  'Enter LinkedIn profile URL',
  'LinkedIn profile link'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'linkedin_url' AND company_id = c.id
);

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'website_url',
  'Website URL',
  'url',
  false,
  true,
  'social',
  41,
  'Enter personal/company website',
  'Personal or company website'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'website_url' AND company_id = c.id
);

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'twitter_handle',
  'Twitter Handle',
  'text',
  false,
  true,
  'social',
  42,
  'Enter Twitter/X handle',
  'Twitter or X username'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'twitter_handle' AND company_id = c.id
);

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'profile_image_url',
  'Profile Image URL',
  'url',
  false,
  true,
  'social',
  43,
  'Enter profile image URL',
  'URL to contact profile image'
FROM companies c
WHERE NOT EXISTS (
  SELECT 1 FROM contact_field_configurations
  WHERE field_name = 'profile_image_url' AND company_id = c.id
);

-- Step 5: Enable Row Level Security
ALTER TABLE contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE contact_field_configurations ENABLE ROW LEVEL SECURITY;

-- Drop existing policies if any
DROP POLICY IF EXISTS "Users can view contacts from their company" ON contacts;
DROP POLICY IF EXISTS "Users can insert contacts to their company" ON contacts;
DROP POLICY IF EXISTS "Users can update contacts from their company" ON contacts;
DROP POLICY IF EXISTS "Users can delete contacts from their company" ON contacts;

DROP POLICY IF EXISTS "Users can view contact field configs from their company" ON contact_field_configurations;
DROP POLICY IF EXISTS "Users can insert contact field configs to their company" ON contact_field_configurations;
DROP POLICY IF EXISTS "Users can update contact field configs from their company" ON contact_field_configurations;
DROP POLICY IF EXISTS "Users can delete contact field configs from their company" ON contact_field_configurations;

-- Create RLS policies for contacts
CREATE POLICY "Users can view contacts from their company"
  ON contacts FOR SELECT
  USING (company_id IN (SELECT company_id FROM users WHERE id = auth.uid()));

CREATE POLICY "Users can insert contacts to their company"
  ON contacts FOR INSERT
  WITH CHECK (company_id IN (SELECT company_id FROM users WHERE id = auth.uid()));

CREATE POLICY "Users can update contacts from their company"
  ON contacts FOR UPDATE
  USING (company_id IN (SELECT company_id FROM users WHERE id = auth.uid()));

CREATE POLICY "Users can delete contacts from their company"
  ON contacts FOR DELETE
  USING (company_id IN (SELECT company_id FROM users WHERE id = auth.uid()));

-- Create RLS policies for contact_field_configurations
CREATE POLICY "Users can view contact field configs from their company"
  ON contact_field_configurations FOR SELECT
  USING (company_id IN (SELECT company_id FROM users WHERE id = auth.uid()));

CREATE POLICY "Users can insert contact field configs to their company"
  ON contact_field_configurations FOR INSERT
  WITH CHECK (company_id IN (SELECT company_id FROM users WHERE id = auth.uid()));

CREATE POLICY "Users can update contact field configs from their company"
  ON contact_field_configurations FOR UPDATE
  USING (company_id IN (SELECT company_id FROM users WHERE id = auth.uid()));

CREATE POLICY "Users can delete contact field configs from their company"
  ON contact_field_configurations FOR DELETE
  USING (company_id IN (SELECT company_id FROM users WHERE id = auth.uid()));

-- Step 6: Verification queries
SELECT 'Contacts table created' AS status;
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'contacts'
ORDER BY ordinal_position;

SELECT 'Contact field configurations table created' AS status;
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'contact_field_configurations'
ORDER BY ordinal_position;

SELECT 'Field configurations seeded' AS status;
SELECT
  field_name,
  field_label,
  field_type,
  is_mandatory,
  field_section,
  display_order,
  COUNT(*) as company_count
FROM contact_field_configurations
GROUP BY field_name, field_label, field_type, is_mandatory, field_section, display_order
ORDER BY display_order;

-- Done!
SELECT '✅ COMPLETE CONTACTS SETUP FINISHED SUCCESSFULLY!' AS status;
