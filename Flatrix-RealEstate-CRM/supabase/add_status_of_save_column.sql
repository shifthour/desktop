-- Add status_of_save column to flatrix_leads table
-- This column tracks how the lead was created (CRM create, MagicBricks, Housing.com, etc.)

-- Check if column exists and add it if not
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'flatrix_leads'
        AND column_name = 'status_of_save'
    ) THEN
        ALTER TABLE flatrix_leads
        ADD COLUMN status_of_save TEXT DEFAULT NULL;

        RAISE NOTICE 'Column status_of_save added successfully';
    ELSE
        RAISE NOTICE 'Column status_of_save already exists';
    END IF;
END $$;

-- Create index for faster filtering by status_of_save
CREATE INDEX IF NOT EXISTS idx_leads_status_of_save ON flatrix_leads(status_of_save);

-- Display sample data to verify
SELECT id, name, phone, source, status_of_save, created_at
FROM flatrix_leads
ORDER BY created_at DESC
LIMIT 10;
