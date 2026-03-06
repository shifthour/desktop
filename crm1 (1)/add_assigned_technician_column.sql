-- Add assigned_technician column to installations table
-- This will fix the "Failed to schedule installation from sales order" error

ALTER TABLE installations 
ADD COLUMN IF NOT EXISTS assigned_technician VARCHAR(255);

-- Create index for better performance on this column
CREATE INDEX IF NOT EXISTS idx_installations_assigned_technician 
ON installations(assigned_technician);

-- Optional: Add a comment to document this column
COMMENT ON COLUMN installations.assigned_technician IS 'Name of the technician assigned to this installation';

-- Update existing records to copy technician_name to assigned_technician if they exist
UPDATE installations 
SET assigned_technician = technician_name 
WHERE technician_name IS NOT NULL AND assigned_technician IS NULL;