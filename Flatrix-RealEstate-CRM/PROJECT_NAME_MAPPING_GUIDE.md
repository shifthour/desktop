# Project Name Mapping Guide

## Overview

The CRM now automatically handles project name variations to prevent duplicate entries in the filter dropdown. This ensures that similar project names from different sources are treated as one project.

## How It Works

### Automatic Normalization

The system automatically normalizes project names by:
1. **Removing extra spaces** - "Project  Name" becomes "Project Name"
2. **Standardizing capitalization** - "project name" becomes "Project Name"
3. **Trimming whitespace** - " Project Name " becomes "Project Name"

### Examples

**Before normalization:**
- "ryth of rains"
- "Ryth Of Rains"
- "RYTH  OF  RAINS"
- "  ryth of rains  "

**After normalization (all become):**
- "Ryth Of Rains"

These will now show as **one entry** in the project filter dropdown, and selecting it will show **all leads** with any of these variations.

---

## Adding Custom Mappings for Known Variations

If you have known typos or variations (like "ryth" vs "rythm"), you can add them to the mapping.

### Where to Add Mappings

**File:** `src/components/LeadsComponent.tsx`

**Location:** Around line 76-81

### Example Mappings

```typescript
const projectNameMapping: { [key: string]: string } = {
  // Map variations (lowercase) to the correct canonical name
  'ryth of rains': 'Rhythm Of Rain',
  'rythm of rain': 'Rhythm Of Rain',
  'rythem of rain': 'Rhythm Of Rain',
  'pavani mirabilia': 'Pavani Mirabilia',
  'pavani mirabelia': 'Pavani Mirabilia',
  'pavni mirabilia': 'Pavani Mirabilia',
}
```

### How to Add a New Mapping

1. Open `src/components/LeadsComponent.tsx`
2. Find the `projectNameMapping` object (around line 76)
3. Add your mapping in the format:
   ```typescript
   'variation in lowercase': 'Correct Name With Proper Caps',
   ```
4. Save and deploy

---

## What This Solves

### ✅ Before (Problem)
- Project Filter shows:
  - "ryth of rains" (3 leads)
  - "Ryth Of Rains" (5 leads)
  - "rythm of rain" (2 leads)
  - "RYTH OF RAINS" (1 lead)
- User has to select each variation separately
- Confusing and time-consuming

### ✅ After (Solution)
- Project Filter shows:
  - "Ryth Of Rains" (11 leads)
- OR if you add to mapping:
  - "Rhythm Of Rain" (11 leads) ← correct canonical name
- Selecting it shows **all 11 leads** regardless of how the name was spelled
- Clean and organized

---

## Important Notes

### ✅ No Data Loss
- **All original data is preserved** in the database
- Project names in leads are NOT modified
- Only the display and filtering logic is affected

### ✅ Works Automatically
- For spacing and capitalization issues, no manual mapping needed
- The system automatically normalizes these

### ✅ Manual Mapping for Typos
- For actual spelling variations (like "ryth" vs "rythm"), add to `projectNameMapping`
- This gives you full control over the canonical name

---

## Common Use Cases

### Case 1: Extra Spaces
**No action needed** - automatically handled

- "Project  Name" ✓
- "Project Name" ✓
- Both become "Project Name"

### Case 2: Different Capitalization
**No action needed** - automatically handled

- "project name" ✓
- "PROJECT NAME" ✓
- "Project Name" ✓
- All become "Project Name"

### Case 3: Spelling Variations (Typos)
**Add to mapping**

```typescript
const projectNameMapping: { [key: string]: string } = {
  'rythem of rain': 'Rhythm Of Rain',
  'rhytham of rain': 'Rhythm Of Rain',
  'rythm of rain': 'Rhythm Of Rain',
}
```

### Case 4: Abbreviations vs Full Names
**Add to mapping**

```typescript
const projectNameMapping: { [key: string]: string } = {
  'pav mirabilia': 'Pavani Mirabilia',
  'pavani mir': 'Pavani Mirabilia',
}
```

---

## Testing

After adding mappings:

1. **Build locally**: `npm run build`
2. **Deploy**: `npx vercel --prod`
3. **Test in CRM**:
   - Go to Leads page
   - Check Project filter dropdown
   - Verify variations are merged
   - Select the project and verify all leads appear

---

## Maintenance

### Finding Variations

To find project name variations in your database:

```sql
-- Run in Supabase SQL Editor
SELECT
  project_name,
  COUNT(*) as lead_count
FROM flatrix_leads
WHERE project_name IS NOT NULL
GROUP BY project_name
ORDER BY LOWER(project_name), lead_count DESC;
```

This will show you all unique project names grouped by their lowercase version, making it easy to spot variations.

### Regular Cleanup

1. Run the SQL query above monthly
2. Identify new variations
3. Add to `projectNameMapping`
4. Deploy the update

---

## Example Workflow

**Scenario:** You notice "Pavani Mirabilia" has 3 variations

1. **Check the database:**
   - "Pavani Mirabilia" - 10 leads
   - "pavani mirabilia" - 5 leads
   - "Pavni Mirabilia" - 2 leads (typo)

2. **Add mapping** (only for the typo):
   ```typescript
   const projectNameMapping: { [key: string]: string } = {
     'pavni mirabilia': 'Pavani Mirabilia',
   }
   ```

3. **Result:**
   - Dropdown shows: "Pavani Mirabilia" (17 leads)
   - Selecting it shows all 17 leads
   - All case/spacing variations handled automatically
   - Typo mapped to correct name

---

## Support

If you need help:
1. Check this guide
2. Review the code in `src/components/LeadsComponent.tsx` around line 76-114
3. Test locally before deploying

---

**Last Updated:** October 28, 2025
**Feature Added:** Project Name Normalization & Mapping
