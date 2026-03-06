-- Add missing lead fields to deals table for complete data preservation
-- This ensures all lead data is preserved when converting to deals

ALTER TABLE deals ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS email TEXT;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS whatsapp TEXT;

-- Product and account relations
ALTER TABLE deals ADD COLUMN IF NOT EXISTS product_id UUID REFERENCES products(id) ON DELETE SET NULL;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS account_id UUID REFERENCES accounts(id) ON DELETE SET NULL;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS contact_id UUID REFERENCES contacts(id) ON DELETE SET NULL;

-- Location information
ALTER TABLE deals ADD COLUMN IF NOT EXISTS location TEXT;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS city TEXT;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS state TEXT;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS department TEXT;

-- Lead tracking and dates
ALTER TABLE deals ADD COLUMN IF NOT EXISTS lead_date DATE;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS closing_date DATE;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS next_followup_date DATE;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS buyer_ref TEXT;

-- Financial details
ALTER TABLE deals ADD COLUMN IF NOT EXISTS budget DECIMAL(15,2);
ALTER TABLE deals ADD COLUMN IF NOT EXISTS quantity INTEGER;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS price_per_unit DECIMAL(15,2);

-- Additional indexes for new fields
CREATE INDEX IF NOT EXISTS idx_deals_phone ON deals(phone);
CREATE INDEX IF NOT EXISTS idx_deals_email ON deals(email);
CREATE INDEX IF NOT EXISTS idx_deals_account_id ON deals(account_id);
CREATE INDEX IF NOT EXISTS idx_deals_contact_id ON deals(contact_id);
CREATE INDEX IF NOT EXISTS idx_deals_product_id ON deals(product_id);
CREATE INDEX IF NOT EXISTS idx_deals_city ON deals(city);
CREATE INDEX IF NOT EXISTS idx_deals_state ON deals(state);

-- Add comments for the new fields
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