-- Make property_id nullable in flatrix_deals table
-- This allows deals to be created without a specific property initially
ALTER TABLE flatrix_deals 
ALTER COLUMN property_id DROP NOT NULL;

-- Also make user_id nullable if it isn't already
ALTER TABLE flatrix_deals 
ALTER COLUMN user_id DROP NOT NULL;