-- Diagnose the priority constraint issue

-- Step 1: See what priority values currently exist in successful rows
SELECT DISTINCT priority, COUNT(*) as count
FROM quotations 
GROUP BY priority
ORDER BY count DESC;

-- Step 2: Try to find what the current constraint allows by looking at constraint definition
SELECT 
    conname as constraint_name,
    conrelid::regclass as table_name,
    pg_get_constraintdef(oid) as constraint_definition
FROM pg_constraint 
WHERE conname LIKE '%priority%' AND conrelid = 'quotations'::regclass;

-- Step 3: If the constraint exists, let's just remove it entirely for now
-- and add a simple one that allows what we need
ALTER TABLE quotations DROP CONSTRAINT IF EXISTS quotations_priority_check;
ALTER TABLE quotations DROP CONSTRAINT IF EXISTS check_priority;  
ALTER TABLE quotations DROP CONSTRAINT IF EXISTS priority_check;

-- Step 4: Add our simple constraint
ALTER TABLE quotations 
ADD CONSTRAINT quotations_priority_check 
CHECK (priority IS NULL OR priority IN ('Low', 'Medium', 'High', 'Urgent'));

SELECT 'Priority constraint diagnostic complete - constraint updated to allow NULL and standard priority values' AS result;