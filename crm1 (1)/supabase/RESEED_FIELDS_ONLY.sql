-- =====================================================
-- RESEED ACCOUNT FIELD CONFIGURATIONS
-- Run this to populate optional fields for existing companies
-- =====================================================

-- Step 1: Drop the old function if it exists
DROP FUNCTION IF EXISTS seed_account_field_configurations(UUID);

-- Step 2: Create the complete seed function with ALL fields
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
        'basic_info', 1, 'Auto-generated', 'Unique system identifier'
    );

    -- 2. Account Name
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, placeholder
    ) VALUES (
        target_company_id, 'account_name', 'Account Name', 'text', true, true,
        'basic_info', 2, 'Enter company name'
    );

    -- 3. Headquarters Address
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, placeholder
    ) VALUES (
        target_company_id, 'headquarters_address', 'Headquarters Address', 'textarea', true, true,
        'addresses', 3, 'Enter HQ address'
    );

    -- 4. Main Phone
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, placeholder
    ) VALUES (
        target_company_id, 'main_phone', 'Main Phone', 'tel', true, true,
        'contact', 4, '+91 9876543210'
    );

    -- 5. Customer Segment
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, field_options, help_text
    ) VALUES (
        target_company_id, 'customer_segment', 'Customer Segment', 'select', true, true,
        'basic_info', 5,
        '["Government", "Academic", "Healthcare_Public", "Healthcare_Private", "Pharma", "Biotech", "Manufacturing", "Research_Private"]'::jsonb,
        'Primary customer segment'
    );

    -- 6. Account Type
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, field_options, help_text
    ) VALUES (
        target_company_id, 'account_type', 'Account Type', 'select', true, true,
        'basic_info', 6,
        '["Customer", "Manufacturer", "Distributor", "End_Customer", "Research_Institution", "Hospital", "University", "Government_Department"]'::jsonb,
        'Primary business type'
    );

    -- 7. Account Industry (cascading dropdown)
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, field_options, help_text
    ) VALUES (
        target_company_id, 'acct_industry', 'Account Industry', 'select', true, true,
        'basic_info', 7,
        '["Research", "Pharma Biopharma", "Chemicals Petrochemicals", "Healthcare Diagnostics", "Instrument Manufacturing", "Environmental Testing", "Education", "Forensics", "Testing Labs"]'::jsonb,
        'Primary industry'
    );

    -- 8. Account Sub-Industry (dependent on industry)
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, help_text
    ) VALUES (
        target_company_id, 'acct_sub_industry', 'Account Sub-Industry', 'select_dependent', true, true,
        'basic_info', 8,
        'Specific sub-category'
    );

    -- 9. Account Status
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, field_options, help_text
    ) VALUES (
        target_company_id, 'account_status', 'Account Status', 'select', true, true,
        'basic_info', 9,
        '["Active", "Inactive", "Prospect", "Customer", "Suspended"]'::jsonb,
        'Current status'
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
    (target_company_id, 'description', 'Description', 'textarea', false, false, 'basic_info', 11,
        'Account description or notes');

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
        'Complete shipping address', 'Shipping address'),
    (target_company_id, 'shipping_street', 'Shipping Street', 'text', false, false, 'addresses', 21,
        'Street line 1', 'Street address line 1'),
    (target_company_id, 'shipping_street2', 'Shipping Street 2', 'text', false, false, 'addresses', 22,
        'Street line 2', 'Street address line 2'),
    (target_company_id, 'shipping_city', 'Shipping City', 'text', false, false, 'addresses', 23,
        'City', 'Shipping city'),
    (target_company_id, 'shipping_state', 'Shipping State', 'text', false, false, 'addresses', 24,
        'State/Province', 'Shipping state'),
    (target_company_id, 'shipping_country', 'Shipping Country', 'select', false, false, 'addresses', 25,
        'Select country', 'Shipping country'),
    (target_company_id, 'shipping_postal_code', 'Shipping Postal Code', 'text', false, false, 'addresses', 26,
        'Postal/Zip code', 'Postal code'),

    (target_company_id, 'billing_address', 'Billing Address', 'textarea', false, false, 'addresses', 30,
        'Complete billing address', 'Billing address'),
    (target_company_id, 'billing_street', 'Billing Street', 'text', false, false, 'addresses', 31,
        'Street line 1', 'Billing street'),
    (target_company_id, 'billing_street2', 'Billing Street 2', 'text', false, false, 'addresses', 32,
        'Street line 2', 'Billing street line 2'),
    (target_company_id, 'billing_city', 'Billing City', 'text', false, false, 'addresses', 33,
        'City', 'Billing city'),
    (target_company_id, 'billing_state', 'Billing State', 'text', false, false, 'addresses', 34,
        'State/Province', 'Billing state'),
    (target_company_id, 'billing_country', 'Billing Country', 'select', false, false, 'addresses', 35,
        'Select country', 'Billing country'),
    (target_company_id, 'billing_postal_code', 'Billing Postal Code', 'text', false, false, 'addresses', 36,
        'Postal/Zip code', 'Billing postal code');

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
        'contact@company.com', 'Primary email'),
    (target_company_id, 'preferred_contact_method', 'Preferred Contact Method', 'select', false, false, 'contact', 41,
        'How to contact', 'Preferred contact method'),
    (target_company_id, 'email_opt_in', 'Email Opt-In', 'select', false, false, 'contact', 42,
        'Email opt-in status', 'Email marketing opt-in'),
    (target_company_id, 'alternate_phone', 'Alternate Phone', 'tel', false, false, 'contact', 43,
        'Alternate phone', 'Secondary phone'),
    (target_company_id, 'website', 'Website', 'url', false, false, 'contact', 44,
        'https://company.com', 'Website URL');

    -- Update options for contact fields
    UPDATE account_field_configurations
    SET field_options = '["Email", "Phone", "WhatsApp", "In-Person"]'::jsonb
    WHERE company_id = target_company_id AND field_name = 'preferred_contact_method';

    UPDATE account_field_configurations
    SET field_options = '["Opted In", "Opted Out", "Not Set"]'::jsonb
    WHERE company_id = target_company_id AND field_name = 'email_opt_in';

    -- BUSINESS DETAILS SECTION
    INSERT INTO account_field_configurations (
        company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
        field_section, display_order, placeholder, help_text
    ) VALUES
    (target_company_id, 'territory', 'Territory', 'select', false, false, 'business_details', 50,
        'Sales territory', 'Sales region'),
    (target_company_id, 'communication_frequency', 'Communication Frequency', 'select', false, false, 'business_details', 51,
        'Frequency', 'Communication frequency'),
    (target_company_id, 'communication_channel', 'Communication Channel', 'select', false, false, 'business_details', 52,
        'Channel', 'Communication channel'),
    (target_company_id, 'organization_size', 'Organization Size', 'select', false, false, 'business_details', 53,
        'Employee count', 'Organization size'),
    (target_company_id, 'vendor_qualification', 'Vendor Qualification', 'select', false, false, 'business_details', 54,
        'Qualification status', 'Vendor qualification');

    -- Update options for business fields
    UPDATE account_field_configurations
    SET field_options = '["North", "South", "East", "West", "Central", "International"]'::jsonb
    WHERE company_id = target_company_id AND field_name = 'territory';

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
        'Tax ID', 'Tax identification number'),
    (target_company_id, 'gstin', 'GSTIN', 'text', false, false, 'financial', 61,
        'GST Number', 'GST identification'),
    (target_company_id, 'pan_number', 'PAN Number', 'text', false, false, 'financial', 62,
        'PAN', 'Permanent Account Number'),
    (target_company_id, 'cin', 'CIN', 'text', false, false, 'financial', 63,
        'Corporate ID', 'Corporate Identification Number'),
    (target_company_id, 'udyam_registration', 'Udyam Registration', 'text', false, false, 'financial', 64,
        'Udyam Number', 'MSME registration'),
    (target_company_id, 'gem_seller_id', 'GeM Seller ID', 'text', false, false, 'financial', 65,
        'GeM ID', 'Government e-Marketplace ID'),
    (target_company_id, 'tax_classification', 'Tax Classification', 'select', false, false, 'financial', 66,
        'Tax status', 'Tax classification'),
    (target_company_id, 'preferred_currency', 'Preferred Currency', 'select', false, false, 'financial', 67,
        'Currency', 'Preferred currency'),
    (target_company_id, 'revenue_currency', 'Revenue Currency', 'select', false, false, 'financial', 68,
        'Currency', 'Revenue currency'),
    (target_company_id, 'annual_revenue', 'Annual Revenue', 'number', false, false, 'financial', 69,
        '0', 'Annual revenue'),
    (target_company_id, 'acc_budget_capacity', 'Budget Capacity', 'number', false, false, 'financial', 70,
        '0', 'Budget capacity'),
    (target_company_id, 'payment_terms', 'Payment Terms', 'select', false, false, 'financial', 71,
        'Terms', 'Payment terms'),
    (target_company_id, 'credit_limit', 'Credit Limit', 'number', false, false, 'financial', 72,
        '0', 'Credit limit'),
    (target_company_id, 'credit_status', 'Credit Status', 'select', false, false, 'financial', 73,
        'Status', 'Credit status');

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
        'Academic institution', 'Academic affiliation'),
    (target_company_id, 'grant_funding_source', 'Grant Funding Source', 'text', false, false, 'advanced', 81,
        'Funding sources', 'Grant funding'),
    (target_company_id, 'regulatory_authority', 'Regulatory Authority', 'text', false, false, 'advanced', 82,
        'Regulatory body', 'Regulatory authority'),
    (target_company_id, 'procurement_cycle', 'Procurement Cycle', 'select', false, false, 'advanced', 83,
        'Cycle', 'Procurement cycle');

    -- Update options for advanced fields
    UPDATE account_field_configurations
    SET field_options = '["Monthly", "Quarterly", "Bi-Annually", "Annually", "As Needed"]'::jsonb
    WHERE company_id = target_company_id AND field_name = 'procurement_cycle';

END;
$$ LANGUAGE plpgsql;

-- Step 3: Run the seed for all existing companies
DO $$
DECLARE
    company_record RECORD;
BEGIN
    FOR company_record IN SELECT id FROM companies LOOP
        PERFORM seed_account_field_configurations(company_record.id);
    END LOOP;
END $$;

-- =====================================================
-- DONE! Field configurations reseeded successfully
-- =====================================================
SELECT 'Success! Field configurations have been reseeded for all companies.' as message;
