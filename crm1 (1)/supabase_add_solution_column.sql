-- Add solution column to cases table
-- This script adds the solution field to support unified case and solution creation

ALTER TABLE cases 
ADD COLUMN IF NOT EXISTS solution TEXT;

-- Add comment for documentation
COMMENT ON COLUMN cases.solution IS 'Solution description for the case - allows unified case and solution management';

-- Optional: Create an index for better search performance on solution text
CREATE INDEX IF NOT EXISTS idx_cases_solution_search ON cases USING GIN (to_tsvector('english', solution));

-- Verify the column was added
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'cases' AND column_name = 'solution';