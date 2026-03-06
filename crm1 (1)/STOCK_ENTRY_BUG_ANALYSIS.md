# Stock Entry Critical Bug Analysis

## Problem Identified

You were **100% CORRECT** - stock quantities are NOT being updated when inward/outward entries are approved!

## Root Cause

### Original Flawed Design:
```
1. User creates stock entry (status: "draft")
   ↓
2. Stock entry items get inserted
   ↓
3. Trigger "trigger_update_stock_on_approval" fires on item INSERT
   ↓
4. BUT: Entry status is still "draft", so function returns early
   ↓
5. NO STOCK UPDATE happens ❌
```

### When User Approves:
```
1. Entry status changes from "draft" → "approved"
   ↓
2. NO new items are inserted
   ↓
3. Trigger does NOT fire (it only fires on INSERT)
   ↓
4. Stock quantities remain unchanged ❌
```

## The Bug in Detail

### Original Trigger (WRONG):
```sql
-- This trigger fires when items are INSERTED
CREATE TRIGGER trigger_update_stock_on_approval
    AFTER INSERT ON stock_entry_items  -- ❌ Wrong table!
    FOR EACH ROW
    EXECUTE FUNCTION update_product_stock();

-- But the function checks if entry is approved
CREATE OR REPLACE FUNCTION update_product_stock()
RETURNS TRIGGER AS $$
BEGIN
    -- Get stock entry details
    SELECT * INTO v_stock_entry FROM stock_entries WHERE id = NEW.stock_entry_id;

    -- ❌ This check fails because items are inserted BEFORE approval
    IF v_stock_entry.status NOT IN ('approved', 'completed') THEN
        RETURN NEW;  -- Does nothing!
    END IF;
    -- ... stock update code never runs
END;
$$
```

## The Fix

### New Trigger (CORRECT):
```sql
-- Fire trigger when ENTRY STATUS CHANGES (not when items are inserted)
CREATE TRIGGER trigger_process_stock_on_approval
    AFTER UPDATE ON stock_entries  -- ✅ Correct table!
    FOR EACH ROW
    WHEN (NEW.status IN ('approved', 'completed') AND OLD.status NOT IN ('approved', 'completed'))
    EXECUTE FUNCTION process_stock_entry_approval();
```

### New Function Logic:
```sql
CREATE OR REPLACE FUNCTION process_stock_entry_approval()
RETURNS TRIGGER AS $$
BEGIN
    -- ✅ This only runs when status actually changes to approved
    IF NEW.status IN ('approved', 'completed') AND OLD.status NOT IN ('approved', 'completed') THEN

        -- Loop through ALL items in the approved entry
        FOR v_item IN
            SELECT * FROM stock_entry_items WHERE stock_entry_id = NEW.id
        LOOP
            -- Calculate change based on entry type
            IF NEW.entry_type = 'inward' THEN
                v_quantity_change := v_item.quantity;  -- ✅ Add stock
            ELSIF NEW.entry_type = 'outward' THEN
                v_quantity_change := -v_item.quantity; -- ✅ Remove stock
            END IF;

            -- Update product stock
            UPDATE products
            SET stock_quantity = stock_quantity + v_quantity_change
            WHERE id = v_item.product_id;

            -- Create transaction record for audit trail
            INSERT INTO stock_transactions (...) VALUES (...);
        END LOOP;

    END IF;
    RETURN NEW;
END;
$$
```

## Expected Behavior After Fix

### Inward Entry:
1. Create inward entry with 10 units of Product A (draft)
2. Current stock: 100 units
3. **Approve the entry**
4. ✅ Stock should increase: 100 → 110 units

### Outward Entry:
1. Create outward entry with 5 units of Product A (draft)
2. Current stock: 110 units
3. **Approve the entry**
4. ✅ Stock should decrease: 110 → 105 units

### Stock Summary:
- Should reflect real-time stock levels after approvals
- Stock transactions table should have complete audit trail
- Critical/Low stock alerts should work based on actual quantities

## Files to Execute in Supabase

### Priority 1: Fix Stock Update Logic
Execute: `supabase/fix_stock_update_trigger.sql`

This will:
- Drop the broken trigger
- Create correct trigger on stock_entries table
- Fix the approval workflow
- Add proper reverse logic for deletions

### Priority 2: Fix Entry Number Generator
Execute: `supabase/final_stock_function_fix.sql`

This will:
- Fix the ambiguous column error
- Allow stock entries to be created

## Testing Steps

1. **Execute both SQL scripts in Supabase SQL Editor**

2. **Test Inward Entry:**
   - Note current stock of a product (e.g., Product A: 50 units)
   - Create inward entry with 10 units
   - Save as "Draft" - stock should NOT change yet
   - Approve the entry - stock should become 60 units ✅

3. **Test Outward Entry:**
   - Current stock: 60 units
   - Create outward entry with 15 units
   - Approve the entry - stock should become 45 units ✅

4. **Verify Stock Summary:**
   - Check stock summary table shows correct quantities
   - Verify stock status (critical/low/adequate) is accurate

5. **Check Transaction History:**
   - Each approval should create transaction records
   - Inward transactions should have positive quantity_change
   - Outward transactions should have negative quantity_change

## Why This Bug Existed

The original schema was designed with a common but flawed pattern:
- Trigger on item INSERT assumes items are only inserted when approved
- But in reality, items are inserted in draft state first
- Then status is updated later
- The trigger never fires again on status update

This is a **timing mismatch** between when the trigger fires and when the condition is met.

## The Solution

Move the trigger to fire on the **status change event** instead of the **item insert event**. This ensures stock updates happen at the exact moment the entry is approved, regardless of when items were inserted.
