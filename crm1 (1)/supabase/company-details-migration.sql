-- Add company details fields for company admin management
-- Run this in Supabase SQL Editor

-- Step 1: Add new columns to companies table
ALTER TABLE companies ADD COLUMN IF NOT EXISTS logo_url TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS website TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS address TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS city TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS state TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS country TEXT DEFAULT 'India';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS postal_code TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS email TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS industry TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS company_size TEXT CHECK (company_size IN ('1-10', '11-50', '51-200', '201-500', '500+'));
ALTER TABLE companies ADD COLUMN IF NOT EXISTS founded_year INTEGER;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS tax_id TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS business_hours TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS primary_color TEXT DEFAULT '#3B82F6';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS secondary_color TEXT DEFAULT '#10B981';

-- Step 2: Add admin_email column if it doesn't exist
ALTER TABLE companies ADD COLUMN IF NOT EXISTS admin_email TEXT;

-- Step 3: Create storage bucket for company logos
-- This needs to be done in Supabase Storage UI or via API
-- INSERT INTO storage.buckets (id, name, public) 
-- VALUES ('company-logos', 'company-logos', true)
-- ON CONFLICT DO NOTHING;

-- Step 4: Verify the new columns
SELECT 
    column_name, 
    data_type, 
    character_maximum_length,
    is_nullable
FROM 
    information_schema.columns
WHERE 
    table_name = 'companies'
ORDER BY 
    ordinal_position;

-- Step 5: Update existing companies with sample data (optional)
UPDATE companies 
SET 
    website = 'https://example.com',
    industry = 'Technology',
    company_size = '11-50',
    description = 'Leading provider of innovative solutions'
WHERE logo_url IS NULL;