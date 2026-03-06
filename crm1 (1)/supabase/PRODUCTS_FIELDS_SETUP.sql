-- =====================================================
-- PRODUCTS FIELD CONFIGURATIONS SETUP
-- =====================================================
-- This script creates the product_field_configurations table
-- and seeds default field configurations for all companies
-- =====================================================

-- Step 1: Create product_field_configurations table
CREATE TABLE IF NOT EXISTS product_field_configurations (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  field_name VARCHAR(100) NOT NULL,
  field_label VARCHAR(150) NOT NULL,
  field_type VARCHAR(50) NOT NULL, -- text, number, select, textarea, url, date, email
  is_mandatory BOOLEAN DEFAULT false,
  is_enabled BOOLEAN DEFAULT true,
  field_section VARCHAR(100) NOT NULL, -- basic_info, pricing, inventory, technical, media, dates, analytics
  display_order INTEGER DEFAULT 0,
  field_options TEXT[], -- For dropdown/select fields
  placeholder VARCHAR(255),
  validation_rules JSONB,
  help_text TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),

  CONSTRAINT product_field_company_unique UNIQUE (field_name, company_id)
);

CREATE INDEX IF NOT EXISTS idx_product_fields_company ON product_field_configurations(company_id);
CREATE INDEX IF NOT EXISTS idx_product_fields_section ON product_field_configurations(field_section);

-- Step 2: Enable RLS
ALTER TABLE product_field_configurations ENABLE ROW LEVEL SECURITY;

-- Drop existing policies if they exist
DROP POLICY IF EXISTS "Users can view product field configs from their company" ON product_field_configurations;
DROP POLICY IF EXISTS "Users can insert product field configs to their company" ON product_field_configurations;
DROP POLICY IF EXISTS "Users can update product field configs from their company" ON product_field_configurations;
DROP POLICY IF EXISTS "Users can delete product field configs from their company" ON product_field_configurations;

-- Create RLS policies
CREATE POLICY "Users can view product field configs from their company"
  ON product_field_configurations FOR SELECT
  USING (true);

CREATE POLICY "Users can insert product field configs to their company"
  ON product_field_configurations FOR INSERT
  WITH CHECK (true);

CREATE POLICY "Users can update product field configs from their company"
  ON product_field_configurations FOR UPDATE
  USING (true);

CREATE POLICY "Users can delete product field configs from their company"
  ON product_field_configurations FOR DELETE
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

        INSERT INTO product_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES
        (company_record.id, 'product_name', 'Product Name', 'text', true, true, 'basic_info', 1,
            'Enter product name', 'Display name of the product'),
        (company_record.id, 'product_code', 'Product Code', 'text', true, true, 'basic_info', 2,
            'Enter product code or SKU', 'Internal product code or SKU'),
        (company_record.id, 'product_description', 'Product Description', 'textarea', true, true, 'basic_info', 3,
            'Enter detailed product description', 'Detailed product description'),
        (company_record.id, 'product_category_id', 'Product Category ID', 'text', true, true, 'basic_info', 4,
            'Enter product category ID', 'Primary product category classification'),
        (company_record.id, 'base_price', 'Base Price', 'number', true, true, 'pricing', 5,
            'Enter base price', 'Base price before any discounts or classifications'),
        (company_record.id, 'currency', 'Currency', 'select', true, true, 'pricing', 6,
            'Select currency', 'Currency for base price'),
        (company_record.id, 'inventory_type', 'Inventory Type', 'select', true, true, 'inventory', 7,
            'Select inventory type', 'Type of inventory management for this product'),
        (company_record.id, 'technical_specifications', 'Technical Specifications', 'textarea', true, true, 'technical', 8,
            'Enter technical specifications', 'Detailed technical specifications and features'),
        (company_record.id, 'product_status', 'Product Status', 'select', true, true, 'basic_info', 9,
            'Select status', 'Current lifecycle status of the product'),
        (company_record.id, 'data_classification', 'Data Classification', 'select', true, true, 'technical', 10,
            'Select data classification', 'Data classification level')
        ON CONFLICT (company_id, field_name) DO NOTHING;

        -- Update field options for mandatory select fields
        UPDATE product_field_configurations
        SET field_options = ARRAY['Physical', 'Digital', 'Service', 'Subscription', 'Bundle']
        WHERE company_id = company_record.id AND field_name = 'inventory_type';

        UPDATE product_field_configurations
        SET field_options = ARRAY['INR', 'USD', 'EUR', 'GBP', 'SGD', 'AED']
        WHERE company_id = company_record.id AND field_name = 'currency';

        UPDATE product_field_configurations
        SET field_options = ARRAY['Active', 'Inactive', 'Draft', 'Discontinued', 'Out of Stock']
        WHERE company_id = company_record.id AND field_name = 'product_status';

        UPDATE product_field_configurations
        SET field_options = ARRAY['Public', 'Internal', 'Confidential', 'Restricted']
        WHERE company_id = company_record.id AND field_name = 'data_classification';

        -- ==========================================
        -- OPTIONAL FIELDS - All disabled by default
        -- ==========================================

        -- Basic Information Section
        INSERT INTO product_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES
        (company_record.id, 'short_description', 'Short Description', 'text', false, false, 'basic_info', 20,
            'Enter brief summary', 'Brief product summary for lists and mobile display'),
        (company_record.id, 'product_subcategory_id', 'Product Subcategory', 'select', false, false, 'basic_info', 21,
            'Select subcategory', 'Secondary product category classification'),
        (company_record.id, 'product_tags', 'Product Tags', 'text', false, false, 'basic_info', 22,
            'Enter tags (comma-separated)', 'Search tags and keywords for product discovery')
        ON CONFLICT (company_id, field_name) DO NOTHING;

        -- Pricing Section
        INSERT INTO product_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES
        (company_record.id, 'cost_price', 'Cost Price', 'number', false, false, 'pricing', 30,
            'Enter cost price', 'Internal cost price for margin calculations'),
        (company_record.id, 'profit_margin', 'Profit Margin %', 'number', false, false, 'pricing', 31,
            'Enter profit margin percentage', 'Calculated profit margin percentage')
        ON CONFLICT (company_id, field_name) DO NOTHING;

        -- Inventory Section
        INSERT INTO product_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES
        (company_record.id, 'current_stock_level', 'Current Stock Level', 'number', false, false, 'inventory', 40,
            'Enter current stock quantity', 'Current available stock quantity'),
        (company_record.id, 'reorder_point', 'Reorder Point', 'number', false, false, 'inventory', 41,
            'Enter reorder threshold', 'Stock level that triggers reorder process'),
        (company_record.id, 'lead_time_days', 'Lead Time (Days)', 'number', false, false, 'inventory', 42,
            'Enter lead time in days', 'Expected lead time for product delivery in days')
        ON CONFLICT (company_id, field_name) DO NOTHING;

        -- Media Section
        INSERT INTO product_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES
        (company_record.id, 'primary_image_url', 'Primary Image URL', 'url', false, false, 'media', 50,
            'Enter image URL', 'URL to the primary product image'),
        (company_record.id, 'image_gallery_urls', 'Image Gallery URLs', 'textarea', false, false, 'media', 51,
            'Enter URLs (one per line)', 'Additional product images for comprehensive view'),
        (company_record.id, 'video_demo_url', 'Video Demo URL', 'url', false, false, 'media', 52,
            'Enter video URL', 'URL to product demonstration video')
        ON CONFLICT (company_id, field_name) DO NOTHING;

        -- Technical Section
        INSERT INTO product_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES
        (company_record.id, 'integration_capabilities', 'Integration Capabilities', 'textarea', false, false, 'technical', 60,
            'Enter integration details', 'Available integrations and API capabilities')
        ON CONFLICT (company_id, field_name) DO NOTHING;

        -- Dates Section
        INSERT INTO product_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES
        (company_record.id, 'launch_date', 'Launch Date', 'date', false, false, 'dates', 70,
            'Select launch date', 'Product launch date'),
        (company_record.id, 'end_of_life_date', 'End of Life Date', 'date', false, false, 'dates', 71,
            'Select end of life date', 'Product end of life date for product')
        ON CONFLICT (company_id, field_name) DO NOTHING;

        -- Analytics Section
        INSERT INTO product_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES
        (company_record.id, 'total_revenue', 'Total Revenue', 'number', false, false, 'analytics', 80,
            'Enter total revenue', 'Total revenue generated by this product to date'),
        (company_record.id, 'total_units_sold', 'Total Units Sold', 'number', false, false, 'analytics', 81,
            'Enter total units sold', 'Total number of units sold to date'),
        (company_record.id, 'average_deal_size', 'Average Deal Size', 'number', false, false, 'analytics', 82,
            'Enter average deal size', 'Average deal size for this product')
        ON CONFLICT (company_id, field_name) DO NOTHING;

        -- Compliance Section
        INSERT INTO product_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES
        (company_record.id, 'regulatory_approvals', 'Regulatory Approvals', 'textarea', false, false, 'technical', 90,
            'Enter regulatory approvals', 'Required regulatory approvals and certifications')
        ON CONFLICT (company_id, field_name) DO NOTHING;

        -- Metadata Section
        INSERT INTO product_field_configurations (
            company_id, field_name, field_label, field_type, is_mandatory, is_enabled,
            field_section, display_order, placeholder, help_text
        ) VALUES
        (company_record.id, 'created_date', 'Created Date', 'date', false, false, 'dates', 100,
            'Auto-populated', 'Timestamp when product record was created'),
        (company_record.id, 'modified_date', 'Modified Date', 'date', false, false, 'dates', 101,
            'Auto-populated', 'Timestamp when product record was last modified'),
        (company_record.id, 'modified_by', 'Modified By', 'text', false, false, 'dates', 102,
            'Auto-populated', 'User ID who last modified the product record')
        ON CONFLICT (company_id, field_name) DO NOTHING;

    END LOOP;
END $$;

-- Step 4: Verification
SELECT '✅ Product field configurations table created' AS status;

SELECT '📊 Field configurations seeded:' AS info;
SELECT
  field_name,
  field_label,
  field_type,
  is_mandatory,
  field_section,
  display_order,
  COUNT(*) as company_count
FROM product_field_configurations
GROUP BY field_name, field_label, field_type, is_mandatory, field_section, display_order
ORDER BY display_order;

SELECT '✅ PRODUCT FIELDS SETUP COMPLETE!' AS status;
