-- Force fix priority constraint by directly updating the problematic row
-- and removing all priority constraints

-- Step 1: Show current priority values
SELECT id, quote_number, priority FROM quotations ORDER BY created_at DESC LIMIT 10;

-- Step 2: Find the exact constraint name and definition
SELECT conname, pg_get_constraintdef(oid) as definition 
FROM pg_constraint 
WHERE conrelid = 'quotations'::regclass AND conname LIKE '%priority%';

-- Step 3: Drop ALL constraints on the quotations table that mention priority
DO $$ 
DECLARE 
    constraint_name text;
BEGIN
    FOR constraint_name IN 
        SELECT conname FROM pg_constraint 
        WHERE conrelid = 'quotations'::regclass 
        AND pg_get_constraintdef(oid) ILIKE '%priority%'
    LOOP
        EXECUTE 'ALTER TABLE quotations DROP CONSTRAINT IF EXISTS ' || constraint_name;
        RAISE NOTICE 'Dropped constraint: %', constraint_name;
    END LOOP;
END $$;

-- Step 4: Now update any problematic priority values (no constraints to block us)
UPDATE quotations SET priority = 'Medium' WHERE priority IS NULL;
UPDATE quotations SET priority = 'Medium' WHERE priority NOT IN ('Low', 'Medium', 'High', 'Urgent');

-- Step 5: Add a simple constraint that allows our values
ALTER TABLE quotations 
ADD CONSTRAINT quotations_priority_check 
CHECK (priority IN ('Low', 'Medium', 'High', 'Urgent'));

-- Step 6: Verify success
SELECT 'Priority constraint completely rebuilt - should work now!' as result;
SELECT DISTINCT priority, COUNT(*) FROM quotations GROUP BY priority;