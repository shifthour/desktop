-- Add bin_location column to products table
ALTER TABLE products
ADD COLUMN IF NOT EXISTS bin_location VARCHAR(100);

-- Add comment to explain the column
COMMENT ON COLUMN products.bin_location IS 'Physical storage location in warehouse (e.g., A-1-5, Shelf B-3)';
