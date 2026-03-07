-- ============================================
-- YSRCP Bangalore Supporters - Supabase Setup
-- ============================================
-- Run this SQL in your Supabase project:
-- Dashboard > SQL Editor > New Query > Paste & Run

-- 1. Create the supporters table
CREATE TABLE IF NOT EXISTS supporters (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    full_name TEXT NOT NULL,
    phone_number TEXT NOT NULL,
    district TEXT,
    constituency TEXT,
    bangalore_address TEXT,
    profession TEXT,
    volunteer TEXT DEFAULT 'Maybe',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Enable Row Level Security
ALTER TABLE supporters ENABLE ROW LEVEL SECURITY;

-- 3. Policy: Allow anyone to INSERT (public form submissions)
CREATE POLICY "Allow public inserts"
    ON supporters
    FOR INSERT
    TO anon
    WITH CHECK (true);

-- 4. Policy: Allow anyone to SELECT (for viewing submissions)
CREATE POLICY "Allow public reads"
    ON supporters
    FOR SELECT
    TO anon
    USING (true);

-- 5. Add indexes for performance
CREATE INDEX idx_supporters_phone ON supporters (phone_number);
CREATE INDEX idx_supporters_constituency ON supporters (constituency);
CREATE INDEX idx_supporters_district ON supporters (district);
CREATE INDEX idx_supporters_bangalore_area ON supporters (bangalore_address);

-- ============================================
-- MIGRATION: If you already have the old table,
-- run these ALTER statements instead:
-- ============================================
-- ALTER TABLE supporters ADD COLUMN IF NOT EXISTS district TEXT;
-- ALTER TABLE supporters ADD COLUMN IF NOT EXISTS profession TEXT;
-- ALTER TABLE supporters ADD COLUMN IF NOT EXISTS volunteer TEXT DEFAULT 'Maybe';
-- CREATE INDEX IF NOT EXISTS idx_supporters_district ON supporters (district);
-- CREATE INDEX IF NOT EXISTS idx_supporters_bangalore_area ON supporters (bangalore_address);
