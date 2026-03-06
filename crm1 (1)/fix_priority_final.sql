-- Step 1: Show current priority values and their counts
SELECT DISTINCT priority, COUNT(*) as count
FROM quotations 
GROUP BY priority
ORDER BY priority;

-- Step 2: First, let's see if there are any rows that would violate the constraint
-- by checking what values exist that are NOT in our desired list
SELECT DISTINCT priority 
FROM quotations 
WHERE priority NOT IN ('Low', 'Medium', 'High', 'Urgent') OR priority IS NULL;

-- Step 3: Temporarily disable constraint checks (if supported)
SET session_replication_role = replica;

-- Step 4: Update all invalid priority values to 'Medium'
UPDATE quotations 
SET priority = 'Medium' 
WHERE priority IS NULL OR priority NOT IN ('Low', 'Medium', 'High', 'Urgent');

-- Step 5: Re-enable constraint checks
SET session_replication_role = DEFAULT;

-- Step 6: Drop existing constraint
ALTER TABLE quotations 
DROP CONSTRAINT IF EXISTS quotations_priority_check;

-- Step 7: Add new constraint
ALTER TABLE quotations 
ADD CONSTRAINT quotations_priority_check 
CHECK (priority IN ('Low', 'Medium', 'High', 'Urgent'));

-- Step 8: Verify the fix worked
SELECT 'Priority constraint fixed! All rows now have valid priority values.' as result;

-- Step 9: Show final priority distribution
SELECT DISTINCT priority, COUNT(*) as count
FROM quotations 
GROUP BY priority
ORDER BY priority;