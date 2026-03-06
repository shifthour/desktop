-- Step 1: See what priority values currently exist
SELECT DISTINCT priority, COUNT(*) as count
FROM quotations 
GROUP BY priority
ORDER BY priority;

-- Step 2: Drop the constraint first (this allows us to update data)
ALTER TABLE quotations 
DROP CONSTRAINT IF EXISTS quotations_priority_check;

-- Step 3: Now update any problematic rows (constraint is gone, so this will work)
UPDATE quotations 
SET priority = 'Medium' 
WHERE priority IS NULL;

-- Step 4: Show updated values
SELECT DISTINCT priority, COUNT(*) as count
FROM quotations 
GROUP BY priority
ORDER BY priority;

-- Step 5: Add the new constraint (now all data should comply)
ALTER TABLE quotations 
ADD CONSTRAINT quotations_priority_check 
CHECK (priority IN ('Low', 'Medium', 'High', 'Urgent'));

-- Step 6: Add documentation
COMMENT ON COLUMN quotations.priority IS 'Priority level: Low, Medium, High, or Urgent';

SELECT 'Priority constraint fixed successfully!' AS result;