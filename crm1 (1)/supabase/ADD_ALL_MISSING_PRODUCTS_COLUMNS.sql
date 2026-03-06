-- =====================================================
-- ADD ALL MISSING PRODUCTS COLUMNS
-- =====================================================
-- This migration adds all columns from the product field
-- configuration system to the actual products table
-- =====================================================

-- Basic Information Section (New Fields)
ALTER TABLE products ADD COLUMN IF NOT EXISTS product_code VARCHAR(255);
ALTER TABLE products ADD COLUMN IF NOT EXISTS product_description TEXT;
ALTER TABLE products ADD COLUMN IF NOT EXISTS short_description TEXT;
ALTER TABLE products ADD COLUMN IF NOT EXISTS product_category_id VARCHAR(255);
ALTER TABLE products ADD COLUMN IF NOT EXISTS product_subcategory_id VARCHAR(255);
ALTER TABLE products ADD COLUMN IF NOT EXISTS product_tags TEXT;
ALTER TABLE products ADD COLUMN IF NOT EXISTS product_status VARCHAR(100);

-- Pricing Section
ALTER TABLE products ADD COLUMN IF NOT EXISTS base_price DECIMAL(12, 2);
ALTER TABLE products ADD COLUMN IF NOT EXISTS currency VARCHAR(10);
ALTER TABLE products ADD COLUMN IF NOT EXISTS cost_price DECIMAL(12, 2);
ALTER TABLE products ADD COLUMN IF NOT EXISTS profit_margin DECIMAL(5, 2);

-- Inventory Section
ALTER TABLE products ADD COLUMN IF NOT EXISTS inventory_type VARCHAR(100);
ALTER TABLE products ADD COLUMN IF NOT EXISTS current_stock_level INTEGER;
ALTER TABLE products ADD COLUMN IF NOT EXISTS reorder_point INTEGER;
ALTER TABLE products ADD COLUMN IF NOT EXISTS lead_time_days INTEGER;

-- Technical Section
ALTER TABLE products ADD COLUMN IF NOT EXISTS technical_specifications TEXT;
ALTER TABLE products ADD COLUMN IF NOT EXISTS data_classification VARCHAR(100);
ALTER TABLE products ADD COLUMN IF NOT EXISTS integration_capabilities TEXT;
ALTER TABLE products ADD COLUMN IF NOT EXISTS regulatory_approvals TEXT;

-- Media Section
ALTER TABLE products ADD COLUMN IF NOT EXISTS primary_image_url TEXT;
ALTER TABLE products ADD COLUMN IF NOT EXISTS image_gallery_urls TEXT;
ALTER TABLE products ADD COLUMN IF NOT EXISTS video_demo_url TEXT;

-- Dates Section
ALTER TABLE products ADD COLUMN IF NOT EXISTS launch_date DATE;
ALTER TABLE products ADD COLUMN IF NOT EXISTS end_of_life_date DATE;
ALTER TABLE products ADD COLUMN IF NOT EXISTS created_date TIMESTAMPTZ;
ALTER TABLE products ADD COLUMN IF NOT EXISTS modified_date TIMESTAMPTZ;
ALTER TABLE products ADD COLUMN IF NOT EXISTS modified_by VARCHAR(255);

-- Analytics Section
ALTER TABLE products ADD COLUMN IF NOT EXISTS total_revenue DECIMAL(15, 2);
ALTER TABLE products ADD COLUMN IF NOT EXISTS total_units_sold INTEGER;
ALTER TABLE products ADD COLUMN IF NOT EXISTS average_deal_size DECIMAL(12, 2);

-- Create indexes for commonly queried fields
CREATE INDEX IF NOT EXISTS idx_products_product_code ON products(product_code);
CREATE INDEX IF NOT EXISTS idx_products_product_status ON products(product_status);
CREATE INDEX IF NOT EXISTS idx_products_product_category_id ON products(product_category_id);
CREATE INDEX IF NOT EXISTS idx_products_inventory_type ON products(inventory_type);
CREATE INDEX IF NOT EXISTS idx_products_currency ON products(currency);

-- Verification: Show all columns in products table
SELECT
    '✅ All missing products columns have been added!' AS status,
    COUNT(*) as total_columns
FROM information_schema.columns
WHERE table_name = 'products';

-- List all columns for verification
SELECT
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'products'
ORDER BY ordinal_position;
