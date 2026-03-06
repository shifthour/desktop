-- Add just the city column first to test
ALTER TABLE installations ADD COLUMN IF NOT EXISTS city VARCHAR(100);

-- Verify the column was added
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'installations' 
AND column_name = 'city';