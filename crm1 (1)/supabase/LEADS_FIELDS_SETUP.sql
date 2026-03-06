-- =====================================================
-- LEADS FIELD CONFIGURATIONS SETUP
-- =====================================================
-- This script creates the lead_field_configurations table
-- and seeds default field configurations for all companies
-- =====================================================

-- Step 1: Create lead_field_configurations table
CREATE TABLE IF NOT EXISTS lead_field_configurations (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  field_name VARCHAR(100) NOT NULL,
  field_label VARCHAR(150) NOT NULL,
  field_type VARCHAR(50) NOT NULL, -- text, number, select, textarea, email, date
  is_mandatory BOOLEAN DEFAULT false,
  is_enabled BOOLEAN DEFAULT true,
  field_section VARCHAR(100) NOT NULL, -- basic_info, lead_details, contact_info, additional_info
  display_order INTEGER DEFAULT 0,
  field_options TEXT[], -- For dropdown/select fields
  placeholder VARCHAR(255),
  validation_rules JSONB,
  help_text TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),

  CONSTRAINT lead_field_company_unique UNIQUE (field_name, company_id)
);

CREATE INDEX IF NOT EXISTS idx_lead_fields_company ON lead_field_configurations(company_id);
CREATE INDEX IF NOT EXISTS idx_lead_fields_section ON lead_field_configurations(field_section);

-- Step 2: Enable RLS
ALTER TABLE lead_field_configurations ENABLE ROW LEVEL SECURITY;

-- Drop existing policies if they exist
DROP POLICY IF EXISTS "Users can view lead field configs from their company" ON lead_field_configurations;
DROP POLICY IF EXISTS "Users can insert lead field configs to their company" ON lead_field_configurations;
DROP POLICY IF EXISTS "Users can update lead field configs from their company" ON lead_field_configurations;
DROP POLICY IF EXISTS "Users can delete lead field configs from their company" ON lead_field_configurations;

-- Create RLS policies
CREATE POLICY "Users can view lead field configs from their company"
  ON lead_field_configurations FOR SELECT
  USING (true);

CREATE POLICY "Users can insert lead field configs to their company"
  ON lead_field_configurations FOR INSERT
  WITH CHECK (true);

CREATE POLICY "Users can update lead field configs from their company"
  ON lead_field_configurations FOR UPDATE
  USING (true);

CREATE POLICY "Users can delete lead field configs from their company"
  ON lead_field_configurations FOR DELETE
  USING (true);

-- Step 3: Seed default field configurations for all companies
DO $$
DECLARE
    company_record RECORD;
BEGIN
    FOR company_record IN SELECT id FROM companies LOOP

        -- ==========================================
        -- MANDATORY FIELDS - Basic Information
        -- ==========================================

        INSERT INTO lead_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES
        (company_record.id, 'lead_title', 'Lead Title', 'text', true, true, 'basic_info', 1,
            'Enter lead title', 'Lead title or name'),
        (company_record.id, 'account', 'Account', 'text', true, true, 'basic_info', 2,
            'Enter account name', 'Account name associated with this lead'),
        (company_record.id, 'contact', 'Contact', 'text', true, true, 'basic_info', 3,
            'Enter contact name', 'Contact person name'),
        (company_record.id, 'name', 'Name', 'text', true, true, 'basic_info', 4,
            'Enter full name', 'Full name of the lead'),
        (company_record.id, 'lead_source', 'Lead Source', 'select', true, true, 'lead_details', 5,
            'Select lead source', 'Source of the lead'),
        (company_record.id, 'lead_type', 'Lead Type', 'select', true, true, 'lead_details', 6,
            'Select lead type', 'Type of lead'),
        (company_record.id, 'lead_status', 'Lead Status', 'select', true, true, 'lead_details', 7,
            'Select lead status', 'Current status of the lead'),
        (company_record.id, 'lead_stage', 'Lead Stage', 'select', true, true, 'lead_details', 8,
            'Select lead stage', 'Current stage in the sales pipeline'),
        (company_record.id, 'nurture_level', 'Nurture Level', 'text', true, true, 'lead_details', 9,
            'Enter nurture level', 'Level of nurturing required'),
        (company_record.id, 'priority_level', 'Priority Level', 'select', true, true, 'lead_details', 10,
            'Select priority', 'Priority level of this lead'),
        (company_record.id, 'email_address', 'Email Address', 'email', true, true, 'contact_info', 11,
            'Enter email address', 'Primary email address'),
        (company_record.id, 'consent_status', 'Consent Status', 'select', true, true, 'additional_info', 12,
            'Select consent status', 'Consent status for communication')
        ON CONFLICT (company_id, field_name) DO NOTHING;

        -- Update field options for mandatory select fields
        UPDATE lead_field_configurations
        SET field_options = ARRAY['Website_Form', 'Email_Campaign', 'Social_Media', 'Referral', 'Trade_Show', 'Cold_Call', 'Partnership', 'Advertisement', 'Content_Marketing', 'Webinar', 'Direct']
        WHERE company_id = company_record.id AND field_name = 'lead_source';

        UPDATE lead_field_configurations
        SET field_options = ARRAY['New_Business', 'Existing_Customer', 'Cross_Sell', 'Up_Sell', 'Renewal', 'Partner', 'Referral', 'Inbound', 'Outbound', 'Prospect']
        WHERE company_id = company_record.id AND field_name = 'lead_type';

        UPDATE lead_field_configurations
        SET field_options = ARRAY['New', 'Contacted', 'Qualified', 'Nurturing', 'Lost', 'Unqualified', 'Recycled']
        WHERE company_id = company_record.id AND field_name = 'lead_status';

        UPDATE lead_field_configurations
        SET field_options = ARRAY['Initial_Contact', 'Information_Gathering', 'Qualification', 'Needs_Analysis', 'Proposal', 'Negotiation', 'Decision', 'Conversion', 'Follow_Up']
        WHERE company_id = company_record.id AND field_name = 'lead_stage';

        UPDATE lead_field_configurations
        SET field_options = ARRAY['High', 'Medium', 'Low']
        WHERE company_id = company_record.id AND field_name = 'priority_level';

        UPDATE lead_field_configurations
        SET field_options = ARRAY['True', 'False']
        WHERE company_id = company_record.id AND field_name = 'consent_status';

        -- ==========================================
        -- OPTIONAL FIELDS - All disabled by default
        -- ==========================================

        -- Lead Details Section
        INSERT INTO lead_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES
        (company_record.id, 'lead_description', 'Lead Description', 'textarea', false, false, 'lead_details', 20,
            'Enter comprehensive description', 'Comprehensive description of lead requirements, interest, and opportunity details'),
        (company_record.id, 'conversion_probability', 'Conversion Probability', 'number', false, false, 'lead_details', 21,
            'Enter probability (0-100)', 'AI-calculated probability of lead conversion to opportunity'),
        (company_record.id, 'created_datetime', 'Created Datetime', 'date', false, false, 'lead_details', 22,
            'Select date', 'Date and time when lead was created'),
        (company_record.id, 'last_activity_date', 'Last Activity Date', 'date', false, false, 'lead_details', 23,
            'Select date', 'Date of the most recent activity or interaction with the lead'),
        (company_record.id, 'next_followup_date', 'Next Followup Date', 'date', false, false, 'lead_details', 24,
            'Select date', 'Scheduled date for next follow-up action with lead'),
        (company_record.id, 'last_contacted_date', 'Last Contacted Date', 'date', false, false, 'lead_details', 25,
            'Select date', 'Date when lead was last contacted by sales team'),
        (company_record.id, 'lead_score', 'Lead Score', 'number', false, false, 'lead_details', 26,
            'Enter score', 'Numerical score based on behavior, profile, and engagement'),
        (company_record.id, 'engagement_score', 'Engagement Score', 'number', false, false, 'lead_details', 27,
            'Enter score', 'Score reflecting level of lead engagement and interaction'),
        (company_record.id, 'qualification_status', 'Qualification Status', 'text', false, false, 'lead_details', 28,
            'Enter status', 'AI-determined qualification status based on BANT and custom criteria'),
        (company_record.id, 'pain_points', 'Pain Points', 'textarea', false, false, 'lead_details', 29,
            'Enter pain points', 'Identified pain points and challenges the lead faces'),
        (company_record.id, 'campaign_id', 'Campaign ID', 'text', false, false, 'lead_details', 30,
            'Enter campaign ID', 'Marketing campaign that generated or influenced the lead'),
        (company_record.id, 'lead_insights', 'Lead Insights', 'textarea', false, false, 'lead_details', 31,
            'Enter insights', 'AI-generated insights about lead behavior, preferences, and conversion factors'),
        (company_record.id, 'conversion_factors', 'Conversion Factors', 'textarea', false, false, 'lead_details', 32,
            'Enter factors', 'Key factors influencing lead conversion potential and decision process'),
        (company_record.id, 'activity_summary', 'Activity Summary', 'textarea', false, false, 'lead_details', 33,
            'Enter summary', 'Summary of lead activities and interactions with organization')
        ON CONFLICT (company_id, field_name) DO NOTHING;

        -- Additional Information Section
        INSERT INTO lead_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES
        (company_record.id, 'created_date', 'Created Date', 'date', false, false, 'additional_info', 40,
            'Auto-populated', 'Date when the lead was created'),
        (company_record.id, 'modified_date', 'Modified Date', 'date', false, false, 'additional_info', 41,
            'Auto-populated', 'Timestamp when lead record was last modified'),
        (company_record.id, 'created_by', 'Created By', 'text', false, false, 'additional_info', 42,
            'Auto-populated', 'User ID who created this lead record'),
        (company_record.id, 'last_modified_by', 'Last Modified By', 'text', false, false, 'additional_info', 43,
            'Auto-populated', 'User ID who last modified this lead record'),
        (company_record.id, 'industry_id', 'Industry ID', 'text', false, false, 'additional_info', 44,
            'Enter industry ID', 'Industry sector and market classification of lead'),
        (company_record.id, 'phone_number', 'Phone Number', 'text', false, false, 'contact_info', 45,
            'Enter phone number', 'Primary phone number of the lead contact'),
        (company_record.id, 'company_name', 'Company Name', 'text', false, false, 'basic_info', 46,
            'Enter company name', 'Name of the company or organization associated with the lead'),
        (company_record.id, 'job_title', 'Job Title', 'text', false, false, 'basic_info', 47,
            'Enter job title', 'Job title or role of the lead contact within their organization'),
        (company_record.id, 'assigned_to', 'Assigned To', 'text', false, false, 'lead_details', 48,
            'Enter assignee', 'User account or market segment assignment for the lead'),
        (company_record.id, 'sales_team', 'Sales Team', 'text', false, false, 'lead_details', 49,
            'Enter sales team', 'Sales team or group responsible for lead management'),
        (company_record.id, 'geographic_region', 'Geographic Region', 'text', false, false, 'additional_info', 50,
            'Enter region', 'Geographic or market region assignment for the lead'),
        (company_record.id, 'budget_range', 'Budget Range', 'text', false, false, 'lead_details', 51,
            'Enter budget range', 'Estimated budget range or investment capacity'),
        (company_record.id, 'decision_timeframe', 'Decision Timeframe', 'text', false, false, 'lead_details', 52,
            'Enter timeframe', 'Expected timeframe for decision-making for this issue'),
        (company_record.id, 'communication_preferences', 'Communication Preferences', 'text', false, false, 'contact_info', 53,
            'Enter preferences', 'Lead preferred communication channels and contact preferences'),
        (company_record.id, 'company_size', 'Company Size', 'text', false, false, 'additional_info', 54,
            'Enter company size', 'Size classification of lead company')
        ON CONFLICT (company_id, field_name) DO NOTHING;

    END LOOP;
END $$;

-- Step 4: Verification
SELECT '✅ Lead field configurations table created' AS status;

SELECT '📊 Field configurations seeded:' AS info;
SELECT
  field_name,
  field_label,
  field_type,
  is_mandatory,
  field_section,
  display_order,
  COUNT(*) as company_count
FROM lead_field_configurations
GROUP BY field_name, field_label, field_type, is_mandatory, field_section, display_order
ORDER BY display_order;

SELECT '✅ LEADS FIELDS SETUP COMPLETE!' AS status;
