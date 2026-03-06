-- Check all columns in amc_contracts table
SELECT
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'amc_contracts'
ORDER BY ordinal_position;
