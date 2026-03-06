-- Add preferred_type column to flatrix_leads table
-- This column will store property type preferences like 1BHK, 2BHK, 3BHK, etc.

ALTER TABLE flatrix_leads 
ADD COLUMN IF NOT EXISTS preferred_type VARCHAR(50);

-- Add an index for better query performance
CREATE INDEX IF NOT EXISTS idx_flatrix_leads_preferred_type ON flatrix_leads(preferred_type);

-- Optional: Update existing records with a default value if needed
-- UPDATE flatrix_leads SET preferred_type = '2BHK' WHERE preferred_type IS NULL;