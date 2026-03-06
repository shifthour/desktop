-- =====================================================
-- UPDATE STOCK TRIGGER TO INCLUDE BIN LOCATION
-- This updates both stock_quantity AND bin_location when entry is approved
-- =====================================================

-- Update the function to also update bin_location
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

            -- Update product stock AND bin_location (for inward entries)
            IF NEW.entry_type = 'inward' AND v_item.bin_location IS NOT NULL THEN
                UPDATE products
                SET stock_quantity = COALESCE(stock_quantity, 0) + v_quantity_change,
                    bin_location = v_item.bin_location,
                    updated_at = NOW()
                WHERE id = v_item.product_id
                RETURNING stock_quantity INTO v_quantity_after;
            ELSE
                UPDATE products
                SET stock_quantity = COALESCE(stock_quantity, 0) + v_quantity_change,
                    updated_at = NOW()
                WHERE id = v_item.product_id
                RETURNING stock_quantity INTO v_quantity_after;
            END IF;

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

-- The trigger already exists, no need to recreate it
-- It will automatically use the updated function
