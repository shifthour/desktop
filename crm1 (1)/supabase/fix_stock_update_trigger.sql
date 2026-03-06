-- =====================================================
-- FIX STOCK UPDATE TRIGGER
-- This fixes the critical bug where stock wasn't updating on approval
-- =====================================================

-- 1. Drop the old incorrect trigger
DROP TRIGGER IF EXISTS trigger_update_stock_on_approval ON stock_entry_items;

-- 2. Create new function to update stock when entry is approved
CREATE OR REPLACE FUNCTION process_stock_entry_approval()
RETURNS TRIGGER AS $$
DECLARE
    v_item RECORD;
    v_quantity_change INTEGER;
    v_quantity_before INTEGER;
    v_quantity_after INTEGER;
BEGIN
    -- Only process when status changes to 'approved' or 'completed'
    IF NEW.status IN ('approved', 'completed') AND OLD.status NOT IN ('approved', 'completed') THEN

        -- Loop through all items in this stock entry
        FOR v_item IN
            SELECT * FROM stock_entry_items WHERE stock_entry_id = NEW.id
        LOOP
            -- Get current stock quantity
            SELECT stock_quantity INTO v_quantity_before
            FROM products
            WHERE id = v_item.product_id;

            -- Calculate quantity change based on entry type
            IF NEW.entry_type = 'inward' THEN
                v_quantity_change := v_item.quantity;
            ELSIF NEW.entry_type = 'outward' THEN
                v_quantity_change := -v_item.quantity;
            ELSE
                v_quantity_change := 0;
            END IF;

            -- Update product stock
            UPDATE products
            SET stock_quantity = COALESCE(stock_quantity, 0) + v_quantity_change,
                updated_at = NOW()
            WHERE id = v_item.product_id
            RETURNING stock_quantity INTO v_quantity_after;

            -- Create transaction record
            INSERT INTO stock_transactions (
                company_id,
                product_id,
                stock_entry_id,
                stock_entry_item_id,
                transaction_type,
                transaction_date,
                quantity_change,
                quantity_before,
                quantity_after,
                unit_cost,
                value_change,
                reference_type,
                reference_number,
                performed_by,
                remarks
            ) VALUES (
                NEW.company_id,
                v_item.product_id,
                NEW.id,
                v_item.id,
                NEW.entry_type,
                NEW.entry_date,
                v_quantity_change,
                COALESCE(v_quantity_before, 0),
                COALESCE(v_quantity_after, 0),
                v_item.unit_price,
                v_item.total_price * (CASE WHEN NEW.entry_type = 'inward' THEN 1 ELSE -1 END),
                NEW.reference_type,
                NEW.reference_number,
                NEW.approved_by,
                NEW.remarks
            );

        END LOOP;

    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 3. Create trigger on stock_entries table (not stock_entry_items!)
DROP TRIGGER IF EXISTS trigger_process_stock_on_approval ON stock_entries;

CREATE TRIGGER trigger_process_stock_on_approval
    AFTER UPDATE ON stock_entries
    FOR EACH ROW
    WHEN (NEW.status IN ('approved', 'completed') AND OLD.status NOT IN ('approved', 'completed'))
    EXECUTE FUNCTION process_stock_entry_approval();

-- 4. Also update the reverse function for consistency
DROP TRIGGER IF EXISTS trigger_reverse_stock_on_delete ON stock_entry_items;

CREATE OR REPLACE FUNCTION reverse_stock_entry()
RETURNS TRIGGER AS $$
DECLARE
    v_item RECORD;
    v_quantity_change INTEGER;
BEGIN
    -- Only reverse if entry was approved/completed
    IF OLD.status IN ('approved', 'completed') THEN

        -- Loop through all items and reverse the stock changes
        FOR v_item IN
            SELECT * FROM stock_entry_items WHERE stock_entry_id = OLD.id
        LOOP
            -- Calculate reverse quantity change
            IF OLD.entry_type = 'inward' THEN
                v_quantity_change := -v_item.quantity;
            ELSIF OLD.entry_type = 'outward' THEN
                v_quantity_change := v_item.quantity;
            ELSE
                v_quantity_change := 0;
            END IF;

            -- Update product stock (reverse the change)
            UPDATE products
            SET stock_quantity = COALESCE(stock_quantity, 0) + v_quantity_change,
                updated_at = NOW()
            WHERE id = v_item.product_id;

        END LOOP;

    END IF;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- 5. Create trigger to reverse stock on entry deletion
CREATE TRIGGER trigger_reverse_stock_on_entry_delete
    BEFORE DELETE ON stock_entries
    FOR EACH ROW
    EXECUTE FUNCTION reverse_stock_entry();

-- 6. Test the setup
-- You can verify the triggers exist with this query:
SELECT
    trigger_name,
    event_manipulation,
    event_object_table,
    action_statement
FROM information_schema.triggers
WHERE trigger_schema = 'public'
AND event_object_table IN ('stock_entries', 'stock_entry_items')
ORDER BY event_object_table, trigger_name;
