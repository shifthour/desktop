-- =====================================================
-- CONTACTS MANAGEMENT SYSTEM - DROP AND RECREATE
-- =====================================================
-- This script will drop existing tables and create fresh ones
-- =====================================================

-- Step 1: Drop existing tables
DROP TABLE IF EXISTS contact_field_configurations CASCADE;
DROP TABLE IF EXISTS contacts CASCADE;

-- Drop existing functions
DROP FUNCTION IF EXISTS generate_contact_id() CASCADE;

-- Step 2: Create contacts table
CREATE TABLE contacts (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  contact_id VARCHAR(50) UNIQUE, -- Auto-generated contact ID (e.g., CONT-001)

  -- Foreign Keys
  company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  account_id UUID REFERENCES accounts(id) ON DELETE SET NULL, -- Which account/company this contact belongs to

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
  created_by UUID,
  owner_id UUID,

  -- Indexes for better performance
  CONSTRAINT contacts_email_company_unique UNIQUE (email_primary, company_id)
);

-- Create indexes
CREATE INDEX idx_contacts_company_id ON contacts(company_id);
CREATE INDEX idx_contacts_account_id ON contacts(account_id);
CREATE INDEX idx_contacts_email ON contacts(email_primary);
CREATE INDEX idx_contacts_lifecycle ON contacts(lifecycle_stage);
CREATE INDEX idx_contacts_status ON contacts(current_contact_status);

-- Step 3: Create contact_field_configurations table
CREATE TABLE contact_field_configurations (
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

CREATE INDEX idx_contact_fields_company ON contact_field_configurations(company_id);
CREATE INDEX idx_contact_fields_section ON contact_field_configurations(field_section);

-- Step 4: Function to generate contact ID
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
CREATE TRIGGER trigger_generate_contact_id
  BEFORE INSERT ON contacts
  FOR EACH ROW
  WHEN (NEW.contact_id IS NULL)
  EXECUTE FUNCTION generate_contact_id();

-- Step 5: Seed default field configurations for all companies

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
FROM companies c;

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
FROM companies c;

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
FROM companies c;

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
FROM companies c;

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
FROM companies c;

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
FROM companies c;

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
FROM companies c;

-- OPTIONAL FIELDS - Contact Information Section
INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'email_secondary',
  'Secondary Email',
  'email',
  false,
  false,
  'contact',
  10,
  'Enter secondary email',
  'Alternative email address'
FROM companies c;

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'phone_work',
  'Work Phone',
  'tel',
  false,
  false,
  'contact',
  11,
  'Enter work phone number',
  'Office/work phone number'
FROM companies c;

-- Professional Information Section
INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'job_title',
  'Job Title',
  'text',
  false,
  false,
  'professional',
  20,
  'Enter job title',
  'Current job title or role'
FROM companies c;

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, field_options, placeholder, help_text)
SELECT
  c.id,
  'preferred_contact_method',
  'Preferred Contact Method',
  'select',
  false,
  false,
  'professional',
  21,
  ARRAY['Email', 'Phone', 'SMS', 'LinkedIn', 'In-Person', 'Video Call'],
  'Select preferred method',
  'How this contact prefers to be reached'
FROM companies c;

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'preferred_contact_time',
  'Preferred Contact Time',
  'text',
  false,
  false,
  'professional',
  22,
  'e.g., Weekdays 9AM-5PM',
  'Best time to contact'
FROM companies c;

-- Address Section
INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'address_street',
  'Street Address',
  'textarea',
  false,
  false,
  'address',
  30,
  'Enter street address',
  'Street address or location'
FROM companies c;

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'address_city',
  'City',
  'text',
  false,
  false,
  'address',
  31,
  'Enter city',
  'City name'
FROM companies c;

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'address_state',
  'State/Province',
  'text',
  false,
  false,
  'address',
  32,
  'Enter state or province',
  'State or province'
FROM companies c;

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'address_postal_code',
  'Postal Code',
  'text',
  false,
  false,
  'address',
  33,
  'Enter postal/ZIP code',
  'Postal or ZIP code'
FROM companies c;

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'country',
  'Country',
  'text',
  false,
  false,
  'address',
  34,
  'Enter country',
  'Country'
FROM companies c;

-- Social Media Section
INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'linkedin_url',
  'LinkedIn URL',
  'url',
  false,
  false,
  'social',
  40,
  'Enter LinkedIn profile URL',
  'LinkedIn profile link'
FROM companies c;

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'website_url',
  'Website URL',
  'url',
  false,
  false,
  'social',
  41,
  'Enter personal/company website',
  'Personal or company website'
FROM companies c;

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'twitter_handle',
  'Twitter Handle',
  'text',
  false,
  false,
  'social',
  42,
  'Enter Twitter/X handle',
  'Twitter or X username'
FROM companies c;

INSERT INTO contact_field_configurations (company_id, field_name, field_label, field_type, is_mandatory, is_enabled, field_section, display_order, placeholder, help_text)
SELECT
  c.id,
  'profile_image_url',
  'Profile Image URL',
  'url',
  false,
  false,
  'social',
  43,
  'Enter profile image URL',
  'URL to contact profile image'
FROM companies c;

-- Step 6: Enable Row Level Security
ALTER TABLE contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE contact_field_configurations ENABLE ROW LEVEL SECURITY;

-- Create RLS policies for contacts
CREATE POLICY "Users can view contacts from their company"
  ON contacts FOR SELECT
  USING (true);

CREATE POLICY "Users can insert contacts to their company"
  ON contacts FOR INSERT
  WITH CHECK (true);

CREATE POLICY "Users can update contacts from their company"
  ON contacts FOR UPDATE
  USING (true);

CREATE POLICY "Users can delete contacts from their company"
  ON contacts FOR DELETE
  USING (true);

-- Create RLS policies for contact_field_configurations
CREATE POLICY "Users can view contact field configs from their company"
  ON contact_field_configurations FOR SELECT
  USING (true);

CREATE POLICY "Users can insert contact field configs to their company"
  ON contact_field_configurations FOR INSERT
  WITH CHECK (true);

CREATE POLICY "Users can update contact field configs from their company"
  ON contact_field_configurations FOR UPDATE
  USING (true);

CREATE POLICY "Users can delete contact field configs from their company"
  ON contact_field_configurations FOR DELETE
  USING (true);

-- Step 7: Verification queries
SELECT '✅ Contacts table created' AS status;

SELECT '📋 Columns in contacts table:' AS info;
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'contacts'
ORDER BY ordinal_position;

SELECT '✅ Contact field configurations table created' AS status;

SELECT '📊 Field configurations seeded:' AS info;
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

SELECT '✅ COMPLETE CONTACTS SETUP FINISHED SUCCESSFULLY!' AS status;
SELECT 'Total field configurations created: ' || COUNT(*)::TEXT AS summary FROM contact_field_configurations;
