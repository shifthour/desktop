-- =====================================================
-- Seed Default Account Field Configurations
-- This should be run for each company OR as a trigger when a company is created
-- =====================================================

-- Function to seed field configurations for a company
CREATE OR REPLACE FUNCTION seed_account_field_configurations(target_company_id UUID)
RETURNS void AS $$
BEGIN
    -- Delete existing configurations for this company (if reseeding)
    DELETE FROM account_field_configurations WHERE company_id = target_company_id;

    -- ==========================================
    -- MANDATORY FIELDS (9 fields - always enabled)
    -- ==========================================

    -- 1. Account ID (auto-generated)
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, placeholder, help_text
    ) VALUES (
        target_company_id, 'account_id', 'Account ID', 'text', true, true,
        'basic_info', 1, 'Auto-generated', 'Unique system identifier with global uniqueness'
    );

    -- 2. Account Name
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, placeholder, validation_rules
    ) VALUES (
        target_company_id, 'account_name', 'Account Name', 'text', true, true,
        'basic_info', 2, 'Enter company/account name', '{"required": true, "minLength": 2}'::jsonb
    );

    -- 3. Headquarters Address
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, placeholder
    ) VALUES (
        target_company_id, 'headquarters_address', 'Headquarters Address', 'textarea', true, true,
        'addresses', 3, 'Enter complete headquarters address'
    );

    -- 4. Main Phone
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, placeholder, validation_rules
    ) VALUES (
        target_company_id, 'main_phone', 'Main Phone', 'tel', true, true,
        'contact', 4, '+91 9876543210', '{"required": true, "pattern": "^[+]?[0-9\\s-()]+$"}'::jsonb
    );

    -- 5. Customer Segment
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, field_options, help_text
    ) VALUES (
        target_company_id, 'customer_segment', 'Customer Segment', 'select', true, true,
        'basic_info', 5,
        '["Government", "Academic", "Healthcare_Public", "Healthcare_Private", "Pharma", "Biotech", "Manufacturing", "Research_Private"]'::jsonb,
        'Primary customer segment for targeted strategies'
    );

    -- 6. Account Type
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, field_options, help_text
    ) VALUES (
        target_company_id, 'account_type', 'Account Type', 'select', true, true,
        'basic_info', 6,
        '["Govt Industry", "Govt Institution", "Private Industry", "Private Institution"]'::jsonb,
        'Primary business role classification'
    );

    -- 7. Account Industry (cascading dropdown)
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, field_options, help_text
    ) VALUES (
        target_company_id, 'acct_industry', 'Account Industry', 'select', true, true,
        'basic_info', 7,
        '["Research", "Pharma Biopharma", "Chemicals Petrochemicals", "Healthcare Diagnostics", "Instrument Manufacturing", "Environmental Testing", "Education", "Forensics", "Testing Labs"]'::jsonb,
        'Primary industry classification'
    );

    -- 8. Account Sub-Industry (dependent on industry)
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, help_text, validation_rules
    ) VALUES (
        target_company_id, 'acct_sub_industry', 'Account Sub-Industry', 'select_dependent', true, true,
        'basic_info', 8,
        'More specific industry sub-category',
        '{"depends_on": "acct_industry", "source": "industry_subindustry_mapping"}'::jsonb
    );

    -- 9. Account Status
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, field_options, help_text
    ) VALUES (
        target_company_id, 'account_status', 'Account Status', 'select', true, true,
        'basic_info', 9,
        '["Active", "Inactive", "Prospect", "Customer", "Suspended"]'::jsonb,
        'Current lifecycle status'
    );

    -- ==========================================
    -- OPTIONAL FIELDS (can be toggled by admin)
    -- ==========================================

    -- BASIC INFO SECTION (Additional)
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, placeholder
    ) VALUES
    (target_company_id, 'preferred_language', 'Preferred Language', 'select', false, false, 'basic_info', 10,
        'Select language'),
    (target_company_id, 'created_by_id', 'Created By', 'text', false, false, 'basic_info', 11,
        'User ID who created account'),
    (target_company_id, 'created_date', 'Created Date', 'date', false, false, 'basic_info', 12,
        'Auto-populated'),
    (target_company_id, 'last_modified_by_id', 'Last Modified By', 'text', false, false, 'basic_info', 13,
        'User ID who modified'),
    (target_company_id, 'last_modified_date', 'Last Modified Date', 'date', false, false, 'basic_info', 14,
        'Auto-populated');

    -- Update field options for preferred_language
    UPDATE account_field_configurations
    SET field_options = '["English", "Hindi", "Kannada", "Tamil", "Telugu", "Marathi"]'::jsonb
    WHERE company_id = target_company_id AND field_name = 'preferred_language';

    -- ADDRESSES SECTION
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, placeholder, help_text
    ) VALUES
    (target_company_id, 'shipping_address', 'Shipping Address', 'textarea', false, false, 'addresses', 20,
        'Shipping delivery address for large organizations', 'Shipping address for large organizations'),
    (target_company_id, 'shipping_street', 'Shipping Street', 'text', false, false, 'addresses', 21,
        'Shipping address street line 1', 'Shipping address street line 1'),
    (target_company_id, 'shipping_street2', 'Shipping Street 2', 'text', false, false, 'addresses', 22,
        'Shipping address street line 2', 'Shipping address street line 2'),
    (target_company_id, 'shipping_city', 'Shipping City', 'text', false, false, 'addresses', 23,
        'Shipping address city', 'Shipping address city'),
    (target_company_id, 'shipping_state', 'Shipping State', 'text', false, false, 'addresses', 24,
        'Shipping address state/province', 'Shipping address state/province'),
    (target_company_id, 'shipping_country', 'Shipping Country', 'select', false, false, 'addresses', 25,
        'Select country', 'Shipping address country'),
    (target_company_id, 'shipping_postal_code', 'Shipping Postal Code', 'text', false, false, 'addresses', 26,
        'Shipping address postal/zip code', 'Shipping address postal/zip code'),

    (target_company_id, 'billing_address', 'Billing Address', 'textarea', false, false, 'addresses', 30,
        'Billing address street line 1', 'Billing address street line 1'),
    (target_company_id, 'billing_street', 'Billing Street', 'text', false, false, 'addresses', 31,
        'Primary business address with geocoding', 'Primary business address with geocoding'),
    (target_company_id, 'billing_street2', 'Billing Street 2', 'text', false, false, 'addresses', 32,
        'Billing address street line 2', 'Billing address street line 2'),
    (target_company_id, 'billing_city', 'Billing City', 'text', false, false, 'addresses', 33,
        'Billing address city', 'Billing address city'),
    (target_company_id, 'billing_state', 'Billing State', 'text', false, false, 'addresses', 34,
        'Billing address state/province', 'Billing address state/province'),
    (target_company_id, 'billing_country', 'Billing Country', 'select', false, false, 'addresses', 35,
        'Select country', 'Billing address country'),
    (target_company_id, 'billing_postal_code', 'Billing Postal Code', 'text', false, false, 'addresses', 36,
        'Billing address postal/zip code', 'Billing address postal/zip code');

    -- Update country options
    UPDATE account_field_configurations
    SET field_options = '["India", "USA", "UK", "Singapore", "UAE", "Germany", "Australia"]'::jsonb
    WHERE company_id = target_company_id AND field_name IN ('shipping_country', 'billing_country');

    -- CONTACT SECTION
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, placeholder, help_text
    ) VALUES
    (target_company_id, 'primary_email', 'Primary Email', 'email', false, false, 'contact', 40,
        'Primary email address for the organization', 'Primary email address for the organization'),
    (target_company_id, 'preferred_contact_method', 'Preferred Contact Method', 'select', false, false, 'contact', 41,
        'How to contact', 'Preferred method for communication'),
    (target_company_id, 'EmailOptIn', 'Email Opt-In Status', 'select', false, false, 'contact', 42,
        'Email marketing opt-in status', 'Email marketing opt-in status'),
    (target_company_id, 'alternate_phone', 'Alternate Phone', 'tel', false, false, 'contact', 43,
        'Alternate phone number', 'Alternate phone number'),
    (target_company_id, 'website', 'Website', 'url', false, false, 'contact', 44,
        'https://company.com', 'Primary website URL');

    -- Update options for contact fields
    UPDATE account_field_configurations
    SET field_options = '["Email", "Phone", "WhatsApp", "In-Person"]'::jsonb
    WHERE company_id = target_company_id AND field_name = 'preferred_contact_method';

    UPDATE account_field_configurations
    SET field_options = '["Opted In", "Opted Out", "Not Set"]'::jsonb
    WHERE company_id = target_company_id AND field_name = 'EmailOptIn';

    -- BUSINESS DETAILS SECTION
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, placeholder, help_text
    ) VALUES
    (target_company_id, 'Territory', 'Territory', 'select', false, false, 'business_details', 50,
        'Sales region based on geography and segment', 'Sales region based on geography and segment'),
    (target_company_id, 'communication_frequency', 'Communication Frequency', 'select', false, false, 'business_details', 51,
        'Preferred frequency of communications', 'Preferred frequency of communications'),
    (target_company_id, 'communication_channel', 'Communication Channel', 'select', false, false, 'business_details', 52,
        'Preferred channel and medium for communication', 'Preferred channel and medium for communication'),
    (target_company_id, 'organization_size', 'Organization Size', 'select', false, false, 'business_details', 53,
        'Employee count classification for sizing', 'Employee count classification for sizing'),
    (target_company_id, 'vendor_qualification', 'Vendor Qualification Status', 'select', false, false, 'business_details', 54,
        'Vendor qualification and approval status', 'Vendor qualification and approval status');

    -- Update options for business fields
    UPDATE account_field_configurations
    SET field_options = '["North", "South", "East", "West", "Central", "International"]'::jsonb
    WHERE company_id = target_company_id AND field_name = 'Territory';

    UPDATE account_field_configurations
    SET field_options = '["Weekly", "Bi-weekly", "Monthly", "Quarterly", "As Needed"]'::jsonb
    WHERE company_id = target_company_id AND field_name = 'communication_frequency';

    UPDATE account_field_configurations
    SET field_options = '["Email", "Phone", "SMS", "WhatsApp", "In-Person", "Video Call"]'::jsonb
    WHERE company_id = target_company_id AND field_name = 'communication_channel';

    UPDATE account_field_configurations
    SET field_options = '["1-50", "51-200", "201-500", "501-1000", "1000+"]'::jsonb
    WHERE company_id = target_company_id AND field_name = 'organization_size';

    UPDATE account_field_configurations
    SET field_options = '["Qualified", "Not Qualified", "In Progress", "Not Required"]'::jsonb
    WHERE company_id = target_company_id AND field_name = 'vendor_qualification';

    -- FINANCIAL SECTION
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, placeholder, help_text
    ) VALUES
    (target_company_id, 'tax_id_number', 'Tax ID Number', 'text', false, false, 'financial', 60,
        'Tax ID identification number', 'Tax ID identification number'),
    (target_company_id, 'gstin', 'GSTIN', 'text', false, false, 'financial', 61,
        'Goods & Services Tax identification number', 'Goods & Services Tax identification number'),
    (target_company_id, 'pan', 'PAN', 'text', false, false, 'financial', 62,
        'Permanent Account Number - Universal ID as to #', 'Permanent Account Number - Universal ID as to #'),
    (target_company_id, 'cin', 'CIN', 'text', false, false, 'financial', 63,
        'Corporate Identification Number for companies', 'Corporate Identification Number for companies'),
    (target_company_id, 'udyam_registration', 'Udyam Registration Number', 'text', false, false, 'financial', 64,
        'MSME udyam registration number', 'MSME udyam registration number'),
    (target_company_id, 'gem_seller_id', 'GeM Seller ID', 'text', false, false, 'financial', 65,
        'Government e-Marketplace seller registration ID', 'Government e-Marketplace seller registration ID'),
    (target_company_id, 'tax_classification', 'Tax Classification', 'select', false, false, 'financial', 66,
        'Tax status and exemptions across jurisdictions', 'Tax status and exemptions across jurisdictions'),
    (target_company_id, 'preferred_currency', 'Preferred Currency', 'select', false, false, 'financial', 67,
        'Primary transactional currency', 'Primary transactional currency'),
    (target_company_id, 'revenue_currency', 'Revenue Currency', 'select', false, false, 'financial', 68,
        'Currency code for financial figures', 'Currency code for financial figures'),
    (target_company_id, 'annual_revenue', 'Annual Revenue', 'number', false, false, 'financial', 69,
        'Annual revenue in base currency', 'Annual revenue in base currency'),
    (target_company_id, 'acc_budget_capacity', 'Account Budget Capacity', 'number', false, false, 'financial', 70,
        'Estimated budget capacity', 'Estimated budget capacity'),
    (target_company_id, 'payment_terms', 'Payment Terms', 'select', false, false, 'financial', 71,
        'Standard payment terms for transactions', 'Standard payment terms for transactions'),
    (target_company_id, 'credit_limit', 'Credit Limit', 'number', false, false, 'financial', 72,
        'Maximum credit exposure in base currency', 'Maximum credit exposure in base currency'),
    (target_company_id, 'credit_status', 'Credit Status', 'select', false, false, 'financial', 73,
        'Current credit status', 'Current credit status');

    -- Update options for financial fields
    UPDATE account_field_configurations
    SET field_options = '["Taxable", "Tax Exempt", "GST Registered", "Composition Scheme"]'::jsonb
    WHERE company_id = target_company_id AND field_name = 'tax_classification';

    UPDATE account_field_configurations
    SET field_options = '["INR", "USD", "EUR", "GBP", "SGD", "AED"]'::jsonb
    WHERE company_id = target_company_id AND field_name IN ('preferred_currency', 'revenue_currency');

    UPDATE account_field_configurations
    SET field_options = '["Net 30", "Net 45", "Net 60", "Advance", "COD", "Custom"]'::jsonb
    WHERE company_id = target_company_id AND field_name = 'payment_terms';

    UPDATE account_field_configurations
    SET field_options = '["Good", "Under Review", "Suspended", "Blocked"]'::jsonb
    WHERE company_id = target_company_id AND field_name = 'credit_status';

    -- ADVANCED SECTION
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, placeholder, help_text
    ) VALUES
    (target_company_id, 'academic_affiliation', 'Academic Affiliation', 'text', false, false, 'advanced', 80,
        'Academic institution affiliation if applicable', 'Academic institution affiliation if applicable'),
    (target_company_id, 'grant_funding_source', 'Grant Funding Source', 'text', false, false, 'advanced', 81,
        'Sources of research grant funding', 'Sources of research grant funding'),
    (target_company_id, 'regulatory_authority', 'Regulatory Authority', 'text', false, false, 'advanced', 82,
        'Regulatory body governed and compliance maintained', 'Regulatory body governed and compliance maintained'),
    (target_company_id, 'procurement_cycle', 'Procurement Cycle', 'select', false, false, 'advanced', 83,
        'Typical procurement and purchase cycle', 'Typical procurement and purchase cycle');

    -- Update options for advanced fields
    UPDATE account_field_configurations
    SET field_options = '["Monthly", "Quarterly", "Bi-Annually", "Annually", "As Needed"]'::jsonb
    WHERE company_id = target_company_id AND field_name = 'procurement_cycle';

END;
$$ LANGUAGE plpgsql;

-- Grant execute permission
GRANT EXECUTE ON FUNCTION seed_account_field_configurations TO authenticated;

-- =====================================================
-- Create trigger to auto-seed when new company is created
-- =====================================================
CREATE OR REPLACE FUNCTION auto_seed_field_configs_on_company_create()
RETURNS TRIGGER AS $$
BEGIN
    -- Seed field configurations for the new company
    PERFORM seed_account_field_configurations(NEW.id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger on companies table
CREATE TRIGGER trigger_seed_field_configs_on_company_create
AFTER INSERT ON companies
FOR EACH ROW
EXECUTE FUNCTION auto_seed_field_configs_on_company_create();

-- =====================================================
-- Seed for existing companies (Run this once)
-- =====================================================
-- This will seed configurations for all existing companies
DO $$
DECLARE
    company_record RECORD;
BEGIN
    FOR company_record IN SELECT id FROM companies LOOP
        PERFORM seed_account_field_configurations(company_record.id);
    END LOOP;
END $$;
