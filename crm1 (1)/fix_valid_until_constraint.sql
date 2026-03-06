-- Fix NOT NULL constraint on valid_until column in quotations table
-- This allows quotations to be created without specifying a valid until date

-- Remove the NOT NULL constraint from valid_until column
ALTER TABLE quotations 
ALTER COLUMN valid_until DROP NOT NULL;

-- Optional: Set a reasonable default value (30 days from quote_date)
-- Uncomment the line below if you want to set a default value
-- ALTER TABLE quotations 
-- ALTER COLUMN valid_until SET DEFAULT (CURRENT_DATE + INTERVAL '30 days');

-- Add a comment to explain the column behavior
COMMENT ON COLUMN quotations.valid_until IS 'Optional: Date until which the quotation is valid. Can be NULL if not specified.';

SELECT 'Fixed NOT NULL constraint on valid_until column - quotations can now be created without valid until date' AS result;