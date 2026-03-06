-- Check the registered column constraints in flatrix_leads table

-- 1. Check column definition and constraints
SELECT
    column_name,
    data_type,
    character_maximum_length,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'flatrix_leads'
  AND column_name = 'registered';

-- 2. Check for CHECK constraints on registered column
SELECT
    con.conname AS constraint_name,
    con.contype AS constraint_type,
    pg_get_constraintdef(con.oid) AS constraint_definition
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
WHERE rel.relname = 'flatrix_leads'
  AND con.contype = 'c';  -- 'c' for CHECK constraints

-- 3. Check if registered is an ENUM type
SELECT
    t.typname AS enum_name,
    e.enumlabel AS enum_value,
    e.enumsortorder
FROM pg_type t
JOIN pg_enum e ON t.oid = e.enumtypid
WHERE t.typname = 'registered_status'
ORDER BY e.enumsortorder;

-- 4. Check current distinct values in the registered column
SELECT
    registered,
    COUNT(*) as count
FROM flatrix_leads
GROUP BY registered
ORDER BY count DESC;

-- 5. Try to see if there's a foreign key or other constraint
SELECT
    tc.constraint_name,
    tc.constraint_type,
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
LEFT JOIN information_schema.constraint_column_usage AS ccu
  ON ccu.constraint_name = tc.constraint_name
  AND ccu.table_schema = tc.table_schema
WHERE tc.table_name = 'flatrix_leads'
  AND kcu.column_name = 'registered';
