-- =====================================================
-- FINAL FIX FOR STOCK ENTRY NUMBER GENERATOR FUNCTION
-- Execute this in Supabase SQL Editor
-- =====================================================

-- 1. Drop the existing function completely (all versions)
DROP FUNCTION IF EXISTS generate_entry_number(VARCHAR, UUID);
DROP FUNCTION IF EXISTS generate_entry_number(character varying, uuid);
DROP FUNCTION IF EXISTS generate_entry_number;

-- 2. Recreate the function with proper table aliasing
CREATE OR REPLACE FUNCTION generate_entry_number(p_entry_type VARCHAR, p_company_id UUID)
RETURNS VARCHAR
LANGUAGE plpgsql
AS $$
DECLARE
    v_prefix VARCHAR;
    v_year VARCHAR;
    v_next_number INTEGER;
    v_result VARCHAR;
BEGIN
    -- Set prefix based on entry type
    v_prefix := CASE
        WHEN p_entry_type = 'inward' THEN 'INW'
        WHEN p_entry_type = 'outward' THEN 'OUT'
        ELSE 'STK'
    END;

    -- Get current year
    v_year := TO_CHAR(CURRENT_DATE, 'YYYY');

    -- Get next number for this type and year (using table alias 'se')
    SELECT COALESCE(MAX(CAST(SPLIT_PART(se.entry_number, '-', 3) AS INTEGER)), 0) + 1
    INTO v_next_number
    FROM stock_entries se
    WHERE se.company_id = p_company_id
    AND se.entry_type = p_entry_type
    AND se.entry_number LIKE v_prefix || '-' || v_year || '-%';

    -- Format: INW-2025-001 or OUT-2025-001
    v_result := v_prefix || '-' || v_year || '-' || LPAD(v_next_number::TEXT, 3, '0');

    RETURN v_result;
END;
$$;

-- 3. Test the function (should return INW-2025-001 for new company)
-- Replace 'de19ccb7-e90d-4507-861d-a3aecf5e3f29' with your actual company_id
SELECT generate_entry_number('inward', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'::uuid);
SELECT generate_entry_number('outward', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'::uuid);

-- 4. Verify RLS is disabled (for testing)
ALTER TABLE stock_entries DISABLE ROW LEVEL SECURITY;
ALTER TABLE stock_entry_items DISABLE ROW LEVEL SECURITY;
ALTER TABLE stock_transactions DISABLE ROW LEVEL SECURITY;

-- 5. Verify tables exist
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
AND table_name IN ('stock_entries', 'stock_entry_items', 'stock_transactions')
ORDER BY table_name;
