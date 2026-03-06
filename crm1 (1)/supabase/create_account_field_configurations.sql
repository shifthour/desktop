-- =====================================================
-- Account Field Configurations System
-- =====================================================

-- Step 1: Create the field configurations table
CREATE TABLE IF NOT EXISTS account_field_configurations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    field_name TEXT NOT NULL,
    field_label TEXT NOT NULL,
    field_type TEXT NOT NULL, -- text, email, number, select, textarea, tel, url
    is_mandatory BOOLEAN DEFAULT false,
    is_enabled BOOLEAN DEFAULT true,
    field_section TEXT NOT NULL, -- basic_info, business_details, financial, addresses, contact, advanced
    display_order INTEGER DEFAULT 0,
    field_options JSONB, -- For dropdown/select options
    placeholder TEXT,
    validation_rules JSONB, -- Validation rules
    help_text TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    CONSTRAINT unique_company_field UNIQUE(company_id, field_name)
);

-- Create indexes for performance
CREATE INDEX idx_field_configs_company_id ON account_field_configurations(company_id);
CREATE INDEX idx_field_configs_section ON account_field_configurations(field_section);
CREATE INDEX idx_field_configs_enabled ON account_field_configurations(is_enabled);
CREATE INDEX idx_field_configs_display_order ON account_field_configurations(display_order);

-- Enable RLS
ALTER TABLE account_field_configurations ENABLE ROW LEVEL SECURITY;

-- RLS Policies
CREATE POLICY "Super admins can access all field configs" ON account_field_configurations
    FOR ALL USING (
        EXISTS (
            SELECT 1 FROM users
            WHERE users.id = auth.uid()
            AND users.is_super_admin = true
        )
    );

CREATE POLICY "Company admins can access their company field configs" ON account_field_configurations
    FOR ALL USING (
        company_id IN (
            SELECT company_id FROM users
            WHERE users.id = auth.uid()
            AND users.is_admin = true
        )
    );

-- Trigger for updated_at
CREATE TRIGGER update_field_configs_updated_at
    BEFORE UPDATE ON account_field_configurations
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions
GRANT ALL ON account_field_configurations TO authenticated;

-- Step 2: Create industry-subindustry mapping table (for cascading dropdowns)
CREATE TABLE IF NOT EXISTS industry_subindustry_mapping (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    industry TEXT NOT NULL,
    subindustry TEXT NOT NULL,
    display_order INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL
);

CREATE INDEX idx_industry_mapping ON industry_subindustry_mapping(industry);

-- Enable RLS
ALTER TABLE industry_subindustry_mapping ENABLE ROW LEVEL SECURITY;

-- Allow all authenticated users to read
CREATE POLICY "All users can read industry mapping" ON industry_subindustry_mapping
    FOR SELECT USING (auth.role() = 'authenticated');

GRANT SELECT ON industry_subindustry_mapping TO authenticated;

-- Insert industry-subindustry data
INSERT INTO industry_subindustry_mapping (industry, subindustry, display_order) VALUES
-- Research
('Research', 'Research Institutions', 1),
('Research', 'BioTech R&D labs', 2),
('Research', 'Pharma R&D labs', 3),
('Research', 'Incubation Center', 4),

-- Pharma Biopharma
('Pharma Biopharma', 'Pharma', 1),
('Pharma Biopharma', 'Bio-Pharma', 2),
('Pharma Biopharma', 'API', 3),
('Pharma Biopharma', 'Neutraceuticals', 4),

-- Chemicals Petrochemicals
('Chemicals Petrochemicals', 'Chemical manufacturers', 1),
('Chemicals Petrochemicals', 'Petrochemicals', 2),
('Chemicals Petrochemicals', 'Paints', 3),
('Chemicals Petrochemicals', 'Pigments', 4),

-- Healthcare Diagnostics
('Healthcare Diagnostics', 'Hospitals', 1),
('Healthcare Diagnostics', 'Wellness Center', 2),
('Healthcare Diagnostics', 'Blood banks', 3),
('Healthcare Diagnostics', 'Diagnostic labs', 4),
('Healthcare Diagnostics', 'Clinical testing', 5),

-- Instrument Manufacturing
('Instrument Manufacturing', 'Laboratory equipment manufacturers', 1),
('Instrument Manufacturing', 'Analytical instrument companies', 2),

-- Environmental Testing
('Environmental Testing', 'Environmental testing laboratories', 1),
('Environmental Testing', 'Pollution monitoring agencies', 2),

-- Education
('Education', 'Universities', 1),
('Education', 'College', 2),
('Education', 'Vocational training institutes', 3),
('Education', 'Incubation Center', 4),
('Education', 'Others', 5),

-- Forensics
('Forensics', 'Forensic laboratories', 1),
('Forensics', 'Law enforcement agencies', 2),

-- Testing Labs
('Testing Labs', 'Regulatory', 1),
('Testing Labs', 'Preclinical', 2),
('Testing Labs', 'Biological', 3),
('Testing Labs', 'Food', 4),
('Testing Labs', 'Agricultural', 5),
('Testing Labs', 'Calibration labs', 6);
