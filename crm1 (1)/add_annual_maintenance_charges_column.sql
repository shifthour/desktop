-- Add annual_maintenance_charges column to installations table
-- This column is needed for AMC auto-creation functionality

ALTER TABLE installations 
ADD COLUMN IF NOT EXISTS annual_maintenance_charges DECIMAL(10,2);

-- Add comment for documentation
COMMENT ON COLUMN installations.annual_maintenance_charges IS 'Annual maintenance charges amount for this installation';

-- Create index for better performance if needed
CREATE INDEX IF NOT EXISTS idx_installations_annual_maintenance_charges ON installations(annual_maintenance_charges);