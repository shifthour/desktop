-- Add email column to accounts table
ALTER TABLE accounts ADD COLUMN email TEXT;

-- Add comment to explain the column
COMMENT ON COLUMN accounts.email IS 'Account email address for communication';