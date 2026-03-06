-- =====================================================
-- ADD ALL LEADS COLUMNS TO DEALS TABLE
-- =====================================================
-- This script ensures the deals table has all the columns
-- from leads table so qualified leads can be converted
-- to deals without losing any data
-- =====================================================

-- Basic Info Fields (matching leads table)
ALTER TABLE deals ADD COLUMN IF NOT EXISTS lead_title VARCHAR(255);
ALTER TABLE deals ADD COLUMN IF NOT EXISTS first_name VARCHAR(100);
ALTER TABLE deals ADD COLUMN IF NOT EXISTS last_name VARCHAR(100);

-- Lead Details Fields
ALTER TABLE deals ADD COLUMN IF NOT EXISTS lead_source VARCHAR(100);
ALTER TABLE deals ADD COLUMN IF NOT EXISTS lead_type VARCHAR(100);
ALTER TABLE deals ADD COLUMN IF NOT EXISTS lead_status VARCHAR(100);
ALTER TABLE deals ADD COLUMN IF NOT EXISTS lead_stage VARCHAR(100);
ALTER TABLE deals ADD COLUMN IF NOT EXISTS nurture_level VARCHAR(100);
ALTER TABLE deals ADD COLUMN IF NOT EXISTS priority_level VARCHAR(50);

-- Contact Info Fields (some may already exist from enhance script)
ALTER TABLE deals ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS email TEXT;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS whatsapp TEXT;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS email_address VARCHAR(255);
ALTER TABLE deals ADD COLUMN IF NOT EXISTS phone_number VARCHAR(50);

-- Product and account relations (some may already exist)
ALTER TABLE deals ADD COLUMN IF NOT EXISTS product_id UUID REFERENCES products(id) ON DELETE SET NULL;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS account_id UUID REFERENCES accounts(id) ON DELETE SET NULL;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS contact_id UUID REFERENCES contacts(id) ON DELETE SET NULL;

-- Location information (some may already exist)
ALTER TABLE deals ADD COLUMN IF NOT EXISTS location TEXT;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS city TEXT;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS state TEXT;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS department TEXT;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS country TEXT;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS address TEXT;

-- Lead tracking and dates (some may already exist)
ALTER TABLE deals ADD COLUMN IF NOT EXISTS lead_date DATE;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS closing_date DATE;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS next_followup_date DATE;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS buyer_ref TEXT;

-- Financial details (some may already exist)
ALTER TABLE deals ADD COLUMN IF NOT EXISTS budget DECIMAL(15,2);
ALTER TABLE deals ADD COLUMN IF NOT EXISTS quantity INTEGER;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS price_per_unit DECIMAL(15,2);

-- Additional Info Fields
ALTER TABLE deals ADD COLUMN IF NOT EXISTS consent_status BOOLEAN DEFAULT false;

-- User tracking fields
ALTER TABLE deals ADD COLUMN IF NOT EXISTS userId UUID REFERENCES users(id) ON DELETE SET NULL;

-- Product name field (legacy support)
ALTER TABLE deals ADD COLUMN IF NOT EXISTS product_name VARCHAR(255);

-- Additional indexes for new fields (if not already created)
CREATE INDEX IF NOT EXISTS idx_deals_phone ON deals(phone);
CREATE INDEX IF NOT EXISTS idx_deals_email ON deals(email);
CREATE INDEX IF NOT EXISTS idx_deals_account_id ON deals(account_id);
CREATE INDEX IF NOT EXISTS idx_deals_contact_id ON deals(contact_id);
CREATE INDEX IF NOT EXISTS idx_deals_product_id ON deals(product_id);
CREATE INDEX IF NOT EXISTS idx_deals_city ON deals(city);
CREATE INDEX IF NOT EXISTS idx_deals_state ON deals(state);
CREATE INDEX IF NOT EXISTS idx_deals_lead_status ON deals(lead_status);
CREATE INDEX IF NOT EXISTS idx_deals_lead_stage ON deals(lead_stage);
CREATE INDEX IF NOT EXISTS idx_deals_department ON deals(department);

-- Add comments for the new fields
COMMENT ON COLUMN deals.lead_title IS 'Lead title transferred from lead';
COMMENT ON COLUMN deals.first_name IS 'Contact first name from lead';
COMMENT ON COLUMN deals.last_name IS 'Contact last name from lead';
COMMENT ON COLUMN deals.phone IS 'Contact phone number transferred from lead';
COMMENT ON COLUMN deals.email IS 'Contact email transferred from lead';
COMMENT ON COLUMN deals.whatsapp IS 'WhatsApp number transferred from lead';
COMMENT ON COLUMN deals.location IS 'Location information from lead';
COMMENT ON COLUMN deals.city IS 'City information from lead';
COMMENT ON COLUMN deals.state IS 'State information from lead';
COMMENT ON COLUMN deals.department IS 'Department information from lead';
COMMENT ON COLUMN deals.budget IS 'Budget information from lead';
COMMENT ON COLUMN deals.quantity IS 'Quantity requirement from lead';
COMMENT ON COLUMN deals.price_per_unit IS 'Price per unit from lead';
COMMENT ON COLUMN deals.lead_date IS 'Original lead creation date';
COMMENT ON COLUMN deals.closing_date IS 'Actual closing date if deal is won';
COMMENT ON COLUMN deals.next_followup_date IS 'Next scheduled followup date';
COMMENT ON COLUMN deals.buyer_ref IS 'Buyer reference from lead';
COMMENT ON COLUMN deals.lead_source IS 'Source of the original lead';
COMMENT ON COLUMN deals.lead_type IS 'Type of lead transferred to deal';
COMMENT ON COLUMN deals.lead_status IS 'Status of the original lead';
COMMENT ON COLUMN deals.lead_stage IS 'Stage of the original lead';
COMMENT ON COLUMN deals.product_name IS 'Product name for legacy support';

-- Verification query
SELECT '✅ Checking which columns were added to deals table:' AS info;

SELECT
  column_name,
  data_type,
  is_nullable
FROM information_schema.columns
WHERE table_name = 'deals'
ORDER BY ordinal_position;

SELECT '✅ All leads columns added to deals table successfully!' AS status;
