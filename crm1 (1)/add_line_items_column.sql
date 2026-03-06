-- Add line_items column to quotations table to store detailed item information
-- This will store the items array as JSON for better data structure

ALTER TABLE quotations 
ADD COLUMN line_items JSONB;

-- Add a comment to explain the column purpose
COMMENT ON COLUMN quotations.line_items IS 'Stores detailed quotation items as JSON array with product details, quantities, prices, etc.';

-- Optional: Add an index on line_items for better query performance if needed
-- CREATE INDEX idx_quotations_line_items ON quotations USING GIN (line_items);