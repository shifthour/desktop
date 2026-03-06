-- =====================================================
-- Add Remarks Column to Tenants Table
-- Run this in Supabase SQL Editor
-- =====================================================

-- Add remarks column to serene_tenants table
ALTER TABLE serene_tenants
ADD COLUMN IF NOT EXISTS remarks TEXT;

-- Verify the column was added
SELECT column_name, data_type, character_maximum_length, is_nullable
FROM information_schema.columns
WHERE table_name = 'serene_tenants'
AND column_name = 'remarks';

-- View sample data to confirm
SELECT id, name, phone, remarks
FROM serene_tenants
LIMIT 5;

-- =====================================================
-- Column Details:
-- - Name: remarks
-- - Type: TEXT (unlimited length)
-- - Nullable: Yes
-- - Purpose: Store additional notes/remarks about tenant
-- =====================================================
