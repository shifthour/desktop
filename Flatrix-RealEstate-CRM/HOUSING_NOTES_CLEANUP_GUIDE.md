# Housing.com Duplicate Notes - Cleanup Guide

## ✅ Issue Status

**FIXED**: As of Oct 25, 2025, the code has been updated and redeployed.
- ✅ New cron runs will NO LONGER duplicate notes
- ✅ Existing leads will NOT have notes updated anymore
- ⚠️ Old duplicate notes need manual cleanup (one-time)

## 🎯 What Happened

Before the fix (Oct 24-25), every 10-minute cron job run was appending duplicate Housing.com data to notes:
```
--- Updated from Housing.com ---
Project: Pavani Mirabilia
Service Type: new-projects
...
--- Updated from Housing.com ---
Project: Pavani Mirabilia  [DUPLICATE]
Service Type: new-projects
...
[Repeated 20+ times]
```

## 🧹 Cleanup Options

You have 3 options to clean up existing duplicate notes:

---

### **Option 1: Simple - Clear ALL Housing.com Auto-Generated Notes (RECOMMENDED)**

This removes all auto-generated Housing.com data from notes, but keeps your manual notes like call records.

**SQL to Run in Supabase:**
```sql
-- Preview what will be affected
SELECT
  id,
  name,
  phone,
  LEFT(notes, 100) as current_notes
FROM flatrix_leads
WHERE source = 'Housing.com'
  AND notes LIKE '%Housing.com at:%'
LIMIT 20;

-- Clear all Housing.com auto-generated notes
UPDATE flatrix_leads
SET notes = REGEXP_REPLACE(
    notes,
    'Project:.*?Received from Housing\.com at: [0-9T:\.\-Z]+',
    '',
    'g'
  ),
  notes = REGEXP_REPLACE(notes, E'\n\n--- Updated from Housing\.com ---\n\n', '', 'g'),
  notes = TRIM(REGEXP_REPLACE(notes, E'\n{3,}', E'\n\n', 'g')),
  updated_at = NOW()
WHERE source = 'Housing.com'
  AND notes LIKE '%Housing.com at:%';

-- Verify cleanup
SELECT
  COUNT(*) as cleaned_leads,
  COUNT(CASE WHEN notes LIKE '%Housing.com at:%' THEN 1 END) as still_has_housing_notes
FROM flatrix_leads
WHERE source = 'Housing.com';
```

**What This Does:**
- ✅ Removes all auto-generated Housing.com project data
- ✅ Keeps your manual notes (timestamps, call records)
- ✅ Cleans up extra newlines
- ✅ One-time operation

**Why This is Best:**
- Housing.com data is already visible in CRM fields (project_name, etc.)
- Notes should be for YOUR team's comments, not duplicate system data
- Prevents notes section from being cluttered

---

### **Option 2: Keep First, Remove Duplicates**

Keeps the first Housing.com note entry, removes all duplicates.

**SQL to Run:**
```sql
-- This is more complex, keeps first occurrence only
UPDATE flatrix_leads
SET notes = SUBSTRING(
    notes
    FROM 1
    FOR POSITION(E'\n\n--- Updated from Housing.com ---' IN notes) - 1
  ),
  updated_at = NOW()
WHERE source = 'Housing.com'
  AND POSITION(E'\n\n--- Updated from Housing.com ---' IN notes) > 0;
```

---

### **Option 3: Manual Cleanup**

1. Go to CRM → Leads
2. Filter: Source = "Housing.com"
3. Open each lead
4. Manually edit notes to remove duplicates
5. Save

**Not recommended** - too time consuming if you have many leads.

---

## 📋 How to Run Cleanup (Option 1 - Recommended)

### Step 1: Backup First (Optional but Recommended)
```sql
-- Create a backup of notes
CREATE TABLE IF NOT EXISTS flatrix_leads_notes_backup AS
SELECT id, phone, name, notes, updated_at
FROM flatrix_leads
WHERE source = 'Housing.com'
  AND notes IS NOT NULL;
```

### Step 2: Run Cleanup
1. Go to https://eflvzsfgoelonfclzrjy.supabase.co
2. Click **"SQL Editor"**
3. Copy the SQL from **Option 1** above
4. Click **"Run"**
5. Wait for completion

### Step 3: Verify in CRM
1. Login to CRM
2. Go to Leads → Filter by Source: "Housing.com"
3. Open any lead
4. Check notes - should be clean now

### Step 4: Check a Few Leads
Make sure your manual notes (like call records) are still there:
```sql
SELECT id, name, phone, notes
FROM flatrix_leads
WHERE source = 'Housing.com'
  AND notes IS NOT NULL
  AND LENGTH(notes) > 0
LIMIT 10;
```

---

## ✅ What to Expect After Cleanup

**Before Cleanup:**
```
Project: Pavani Mirabilia
Service Type: new-projects
...
--- Updated from Housing.com ---
Project: Pavani Mirabilia  [DUPLICATE 1]
--- Updated from Housing.com ---
Project: Pavani Mirabilia  [DUPLICATE 2]
...
[10/25/25, 12:01 PM]:
called the cx, said not looking for the property till 1 year
```

**After Cleanup (Option 1):**
```
[10/25/25, 12:01 PM]:
called the cx, said not looking for the property till 1 year
```

**Your Manual Notes Preserved:**
- Timestamps like `[10/25/25, 12:01 PM]:`
- Call records
- Follow-up notes
- Any manual entries

**Removed:**
- All auto-generated Housing.com project data
- All duplicate markers
- Repeated system information

---

## 🔄 Going Forward

**After cleanup and fix:**
- ✅ Cron runs every 10 minutes
- ✅ Fetches new leads OR updates existing leads
- ✅ **Does NOT touch notes** for existing leads
- ✅ Clean, organized lead records
- ✅ Notes section is ONLY for your team's manual entries

**For New Leads:**
- First time a lead comes from Housing.com → Notes include project info
- All subsequent cron runs → Notes stay unchanged
- You can still add manual notes anytime

---

## 🆘 Rollback (If Something Goes Wrong)

If you backed up (Step 1), you can restore:
```sql
-- Restore from backup
UPDATE flatrix_leads l
SET notes = b.notes,
    updated_at = NOW()
FROM flatrix_leads_notes_backup b
WHERE l.id = b.id
  AND l.source = 'Housing.com';
```

---

## 📞 Need Help?

If you're unsure, you can:
1. Test on 1-2 leads first by adding `LIMIT 2` to the UPDATE query
2. Check those leads in CRM before running full cleanup
3. Keep the backup table for safety

---

## ✅ Summary

| Action | Status | Next Step |
|--------|--------|-----------|
| Fix deployed | ✅ Done | No action needed |
| Notes duplication stopped | ✅ Done | No action needed |
| Cleanup old duplicates | ⏳ Pending | Run Option 1 SQL |

**Recommended Action:** Run Option 1 cleanup SQL to remove all old duplicate notes.

---

**Last Updated:** October 25, 2025
**Status:** Fix deployed and tested ✅
