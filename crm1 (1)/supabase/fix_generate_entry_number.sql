-- Fix the generate_entry_number function to avoid ambiguous column reference

DROP FUNCTION IF EXISTS generate_entry_number(VARCHAR, UUID);

CREATE OR REPLACE FUNCTION generate_entry_number(p_entry_type VARCHAR, p_company_id UUID)
RETURNS VARCHAR AS $$
DECLARE
    prefix VARCHAR;
    year VARCHAR;
    next_number INTEGER;
    result_entry_number VARCHAR;
BEGIN
    -- Set prefix based on entry type
    prefix := CASE
        WHEN p_entry_type = 'inward' THEN 'INW'
        WHEN p_entry_type = 'outward' THEN 'OUT'
        ELSE 'STK'
    END;

    -- Get current year
    year := TO_CHAR(CURRENT_DATE, 'YYYY');

    -- Get next number for this type and year
    SELECT COALESCE(MAX(CAST(SPLIT_PART(stock_entries.entry_number, '-', 3) AS INTEGER)), 0) + 1
    INTO next_number
    FROM stock_entries
    WHERE stock_entries.company_id = p_company_id
    AND stock_entries.entry_type = p_entry_type
    AND stock_entries.entry_number LIKE prefix || '-' || year || '-%';

    -- Format: INW-2025-001
    result_entry_number := prefix || '-' || year || '-' || LPAD(next_number::TEXT, 3, '0');

    RETURN result_entry_number;
END;
$$ LANGUAGE plpgsql;
