-- Add missing fields to leads table for enhanced lead management

-- Add price_per_unit column
ALTER TABLE leads ADD COLUMN IF NOT EXISTS price_per_unit DECIMAL(15,2);

-- Add address column (if not exists)
ALTER TABLE leads ADD COLUMN IF NOT EXISTS address TEXT;

-- Add country column (if not exists) 
ALTER TABLE leads ADD COLUMN IF NOT EXISTS country TEXT;

-- Update any existing records to ensure consistency
UPDATE leads SET price_per_unit = 0 WHERE price_per_unit IS NULL;