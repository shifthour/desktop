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
    constituency TEXT NOT NULL,
    bangalore_address TEXT NOT NULL,
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

-- 5. Add index on phone_number to prevent duplicate lookups
CREATE INDEX idx_supporters_phone ON supporters (phone_number);

-- 6. Add index on constituency for filtering
CREATE INDEX idx_supporters_constituency ON supporters (constituency);
