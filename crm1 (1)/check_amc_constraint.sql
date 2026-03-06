-- Check the actual constraint definition
SELECT 
    conname AS constraint_name,
    pg_get_constraintdef(oid) AS constraint_definition
FROM pg_constraint
WHERE conname LIKE '%frequency%' 
   OR conname = 'amc_service_frequency_check';

-- Also check all constraints on the amc_contracts table
SELECT 
    conname AS constraint_name,
    pg_get_constraintdef(oid) AS constraint_definition
FROM pg_constraint
WHERE conrelid = 'amc_contracts'::regclass;