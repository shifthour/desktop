-- Simple cleanup: Remove all Housing.com duplicate notes and keep only manual notes
-- Run this in Supabase SQL Editor

-- OPTION 1: Preview which leads will be affected
SELECT
  id,
  name,
  phone,
  source,
  LEFT(notes, 150) as notes_preview,
  (LENGTH(notes) - LENGTH(REPLACE(notes, '--- Updated from Housing.com ---', ''))) / 39 as duplicate_count
FROM flatrix_leads
WHERE source = 'Housing.com'
  AND notes LIKE '%--- Updated from Housing.com ---%'
ORDER BY duplicate_count DESC;

-- OPTION 2A: Remove ALL Housing.com notes sections (keeps only manual notes like timestamps)
-- This removes everything from the first "Project: " to the end if it's Housing.com data
UPDATE flatrix_leads
SET notes = REGEXP_REPLACE(
  notes,
  'Project:.*?Housing\.com at:.*?(\n\n|\n$|$)',
  '',
  'g'
),
updated_at = NOW()
WHERE source = 'Housing.com'
  AND notes LIKE '%Project:%'
  AND notes LIKE '%Housing.com at:%';

-- OPTION 2B: Keep ONLY the first Housing.com note and remove all duplicates
-- Split by the marker, keep text before first occurrence + first occurrence only
UPDATE flatrix_leads
SET notes = (
  CASE
    -- If there are manual notes before Housing.com data
    WHEN POSITION('Project:' IN notes) > 10 THEN
      -- Keep manual notes + first Housing.com section
      SUBSTRING(notes FROM 1 FOR POSITION('Project:' IN notes) - 1) ||
      SUBSTRING(
        notes
        FROM POSITION('Project:' IN notes)
        FOR COALESCE(
          NULLIF(POSITION(E'\n\n--- Updated from Housing.com ---' IN notes), 0) - POSITION('Project:' IN notes),
          LENGTH(notes)
        )
      )
    ELSE
      -- Just keep first Housing.com section
      SUBSTRING(
        notes
        FROM 1
        FOR COALESCE(
          NULLIF(POSITION(E'\n\n--- Updated from Housing.com ---' IN notes), 0),
          LENGTH(notes)
        )
      )
  END
),
updated_at = NOW()
WHERE source = 'Housing.com'
  AND notes LIKE '%--- Updated from Housing.com ---%';

-- OPTION 3: Nuclear option - Remove ALL Housing.com generated notes completely
-- This leaves only manually added notes (timestamps, call notes, etc.)
UPDATE flatrix_leads
SET notes = REGEXP_REPLACE(
  notes,
  '(Project:.*?Housing\.com at: .*?)(\n\n--- Updated from Housing\.com ---.*)*',
  '',
  'g'
),
updated_at = NOW()
WHERE source = 'Housing.com'
  AND notes LIKE '%Housing.com at:%';

-- Verify cleanup
SELECT
  COUNT(*) as total_leads,
  COUNT(CASE WHEN notes LIKE '%--- Updated from Housing.com ---%' THEN 1 END) as still_has_duplicates,
  COUNT(CASE WHEN notes IS NULL OR notes = '' THEN 1 END) as empty_notes
FROM flatrix_leads
WHERE source = 'Housing.com';
