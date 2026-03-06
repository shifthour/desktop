-- Complete schema update for quotations table
-- This adds all missing columns that the application is trying to send

-- Add missing columns to quotations table
ALTER TABLE quotations 
ADD COLUMN IF NOT EXISTS subtotal_amount DECIMAL(15,2),
ADD COLUMN IF NOT EXISTS tax_amount DECIMAL(15,2),
ADD COLUMN IF NOT EXISTS discount_amount DECIMAL(15,2),
ADD COLUMN IF NOT EXISTS currency VARCHAR(10) DEFAULT 'INR',
ADD COLUMN IF NOT EXISTS priority VARCHAR(20) DEFAULT 'Medium',
ADD COLUMN IF NOT EXISTS reference_number VARCHAR(100),
ADD COLUMN IF NOT EXISTS subject VARCHAR(255),
ADD COLUMN IF NOT EXISTS notes TEXT,
ADD COLUMN IF NOT EXISTS terms_conditions TEXT,
ADD COLUMN IF NOT EXISTS payment_terms VARCHAR(255),
ADD COLUMN IF NOT EXISTS delivery_terms VARCHAR(255),
ADD COLUMN IF NOT EXISTS tax_type VARCHAR(20) DEFAULT 'GST',
ADD COLUMN IF NOT EXISTS line_items JSONB;

-- Add comments for clarity
COMMENT ON COLUMN quotations.subtotal_amount IS 'Subtotal before taxes and discounts';
COMMENT ON COLUMN quotations.tax_amount IS 'Total tax amount';
COMMENT ON COLUMN quotations.discount_amount IS 'Total discount amount';
COMMENT ON COLUMN quotations.currency IS 'Currency code (INR, USD, EUR, etc.)';
COMMENT ON COLUMN quotations.priority IS 'Priority level (Low, Medium, High, Urgent)';
COMMENT ON COLUMN quotations.reference_number IS 'Reference or PO number';
COMMENT ON COLUMN quotations.subject IS 'Quotation subject line';
COMMENT ON COLUMN quotations.notes IS 'Additional notes and comments';
COMMENT ON COLUMN quotations.terms_conditions IS 'Terms and conditions text';
COMMENT ON COLUMN quotations.payment_terms IS 'Payment terms and conditions';
COMMENT ON COLUMN quotations.delivery_terms IS 'Delivery terms and timeline';
COMMENT ON COLUMN quotations.tax_type IS 'Tax type (GST, VAT, etc.)';
COMMENT ON COLUMN quotations.line_items IS 'Detailed quotation items as JSON array';

-- Update any existing quotations to have default values for new columns
UPDATE quotations 
SET 
    currency = COALESCE(currency, 'INR'),
    priority = COALESCE(priority, 'Medium'),
    tax_type = COALESCE(tax_type, 'GST')
WHERE currency IS NULL OR priority IS NULL OR tax_type IS NULL;

-- Optional: Add indexes for better performance
CREATE INDEX IF NOT EXISTS idx_quotations_currency ON quotations (currency);
CREATE INDEX IF NOT EXISTS idx_quotations_priority ON quotations (priority);
CREATE INDEX IF NOT EXISTS idx_quotations_status ON quotations (status);
CREATE INDEX IF NOT EXISTS idx_quotations_company_date ON quotations (company_id, quote_date);

-- Optional: Add GIN index for line_items JSON queries
-- CREATE INDEX IF NOT EXISTS idx_quotations_line_items ON quotations USING GIN (line_items);

SELECT 'Quotations table schema updated successfully!' AS result;