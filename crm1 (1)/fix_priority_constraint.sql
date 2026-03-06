-- Fix priority check constraint in quotations table
-- The application sends "Medium" but the constraint doesn't allow it

-- First, let's see what priority values exist in the table
SELECT DISTINCT priority, COUNT(*) as count
FROM quotations 
GROUP BY priority;

-- Update any existing rows that might not match the new constraint FIRST
UPDATE quotations 
SET priority = 'Medium' 
WHERE priority IS NULL OR priority NOT IN ('Low', 'Medium', 'High', 'Urgent');

-- Drop the existing priority check constraint
ALTER TABLE quotations 
DROP CONSTRAINT IF EXISTS quotations_priority_check;

-- Add a new constraint that matches the application's expected values
ALTER TABLE quotations 
ADD CONSTRAINT quotations_priority_check 
CHECK (priority IN ('Low', 'Medium', 'High', 'Urgent'));

-- Add a comment
COMMENT ON COLUMN quotations.priority IS 'Priority level: Low, Medium, High, or Urgent';

SELECT 'Fixed priority constraint - now allows Low, Medium, High, Urgent values' AS result;