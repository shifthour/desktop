-- Clean up duplicate Housing.com notes from leads
-- This removes repeated "--- Updated from Housing.com ---" sections
-- Run this in Supabase SQL Editor

-- Step 1: Preview leads that have duplicate notes
SELECT
  id,
  name,
  phone,
  LENGTH(notes) as notes_length,
  (LENGTH(notes) - LENGTH(REPLACE(notes, '--- Updated from Housing.com ---', ''))) / LENGTH('--- Updated from Housing.com ---') as duplicate_count
FROM flatrix_leads
WHERE source = 'Housing.com'
  AND notes LIKE '%--- Updated from Housing.com ---%'
ORDER BY duplicate_count DESC
LIMIT 20;

-- Step 2: Clean up notes by keeping only the FIRST occurrence
-- This preserves the original notes and removes all duplicates

DO $$
DECLARE
  lead_record RECORD;
  cleaned_notes TEXT;
  first_section TEXT;
BEGIN
  FOR lead_record IN
    SELECT id, notes
    FROM flatrix_leads
    WHERE source = 'Housing.com'
      AND notes LIKE '%--- Updated from Housing.com ---%'
  LOOP
    -- Extract everything up to and including the first "--- Updated from Housing.com ---" section
    -- Split by the marker and take only the first section
    first_section := SPLIT_PART(lead_record.notes, '--- Updated from Housing.com ---', 1);

    -- Get the first duplicate section content (between first and second marker)
    IF POSITION('--- Updated from Housing.com ---' IN lead_record.notes) > 0 THEN
      cleaned_notes := first_section || SUBSTRING(
        lead_record.notes
        FROM POSITION('--- Updated from Housing.com ---' IN lead_record.notes)
        FOR POSITION('--- Updated from Housing.com ---' IN SUBSTRING(lead_record.notes FROM POSITION('--- Updated from Housing.com ---' IN lead_record.notes) + 37)) + 36
      );

      -- If there's content before the first marker, keep it
      IF LENGTH(TRIM(first_section)) > 0 THEN
        cleaned_notes := first_section;
      ELSE
        -- Just keep the first Housing.com section
        cleaned_notes := REGEXP_REPLACE(
          lead_record.notes,
          '(--- Updated from Housing\.com ---.*?)(\n\n--- Updated from Housing\.com ---.*$)',
          '\1',
          'g'
        );

        -- Remove all but the first occurrence
        cleaned_notes := REGEXP_REPLACE(
          cleaned_notes,
          '(\n\n--- Updated from Housing\.com ---.*)',
          '',
          'g'
        );
      END IF;

      -- Update the lead with cleaned notes
      UPDATE flatrix_leads
      SET
        notes = cleaned_notes,
        updated_at = NOW()
      WHERE id = lead_record.id;

      RAISE NOTICE 'Cleaned notes for lead ID: %, Phone: %', lead_record.id, (SELECT phone FROM flatrix_leads WHERE id = lead_record.id);
    END IF;
  END LOOP;
END $$;

-- Step 3: Verify the cleanup
SELECT
  id,
  name,
  phone,
  source,
  LEFT(notes, 200) as notes_preview,
  LENGTH(notes) as notes_length
FROM flatrix_leads
WHERE source = 'Housing.com'
  AND notes IS NOT NULL
ORDER BY updated_at DESC
LIMIT 10;

-- Step 4: Count how many leads were cleaned
SELECT
  COUNT(*) as total_housing_leads,
  COUNT(CASE WHEN notes NOT LIKE '%--- Updated from Housing.com ---%' THEN 1 END) as clean_notes,
  COUNT(CASE WHEN notes LIKE '%--- Updated from Housing.com ---%' THEN 1 END) as still_has_marker
FROM flatrix_leads
WHERE source = 'Housing.com';
