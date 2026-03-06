-- =====================================================
-- Stock Entry Management System
-- This script creates tables for managing stock inward/outward entries
-- =====================================================

-- First, add stock quantity field to products table if not exists
ALTER TABLE products
ADD COLUMN IF NOT EXISTS stock_quantity INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS min_stock_level INTEGER DEFAULT 10,
ADD COLUMN IF NOT EXISTS max_stock_level INTEGER DEFAULT 1000,
ADD COLUMN IF NOT EXISTS reorder_level INTEGER DEFAULT 20,
ADD COLUMN IF NOT EXISTS unit_of_measure VARCHAR(50) DEFAULT 'pcs';

-- Create stock_entries table for tracking all stock movements
CREATE TABLE IF NOT EXISTS stock_entries (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,

    -- Entry Details
    entry_type VARCHAR(50) NOT NULL CHECK (entry_type IN ('inward', 'outward')),
    entry_number VARCHAR(100) UNIQUE NOT NULL, -- Auto-generated: INW-2025-001, OUT-2025-001
    entry_date DATE NOT NULL DEFAULT CURRENT_DATE,

    -- Reference Information
    reference_type VARCHAR(50), -- 'purchase_order', 'sales_order', 'adjustment', 'return', 'transfer'
    reference_number VARCHAR(100),

    -- Party Information (Supplier for inward, Customer for outward)
    party_type VARCHAR(50), -- 'supplier', 'customer', 'warehouse'
    party_name VARCHAR(255),
    party_id UUID, -- Reference to accounts or suppliers table

    -- Warehouse/Location
    warehouse_location VARCHAR(255),

    -- Financial
    total_quantity INTEGER NOT NULL DEFAULT 0,
    total_value DECIMAL(12, 2) DEFAULT 0,

    -- Document & Notes
    document_url TEXT, -- Upload invoice, delivery note, etc.
    remarks TEXT,
    notes TEXT,

    -- Status & Approval
    status VARCHAR(50) DEFAULT 'draft' CHECK (status IN ('draft', 'submitted', 'approved', 'rejected', 'completed')),
    approved_by UUID REFERENCES users(id),
    approved_at TIMESTAMPTZ,

    -- User Tracking
    created_by UUID REFERENCES users(id),
    updated_by UUID REFERENCES users(id),

    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create stock_entry_items table for line items
CREATE TABLE IF NOT EXISTS stock_entry_items (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    stock_entry_id UUID NOT NULL REFERENCES stock_entries(id) ON DELETE CASCADE,
    product_id UUID NOT NULL REFERENCES products(id) ON DELETE RESTRICT,

    -- Item Details
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) DEFAULT 0,
    total_price DECIMAL(12, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED,

    -- Additional Info
    batch_number VARCHAR(100),
    serial_number VARCHAR(100),
    expiry_date DATE,
    manufacture_date DATE,

    -- Quality Check
    quality_status VARCHAR(50) DEFAULT 'pending' CHECK (quality_status IN ('pending', 'passed', 'failed', 'on_hold')),
    quality_remarks TEXT,

    -- Warehouse Bin Location
    bin_location VARCHAR(100),
    rack_number VARCHAR(50),

    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create stock_transactions table for complete audit trail
CREATE TABLE IF NOT EXISTS stock_transactions (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    product_id UUID NOT NULL REFERENCES products(id) ON DELETE RESTRICT,
    stock_entry_id UUID REFERENCES stock_entries(id) ON DELETE SET NULL,
    stock_entry_item_id UUID REFERENCES stock_entry_items(id) ON DELETE SET NULL,

    -- Transaction Details
    transaction_type VARCHAR(50) NOT NULL CHECK (transaction_type IN ('inward', 'outward', 'adjustment')),
    transaction_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Quantity Changes
    quantity_change INTEGER NOT NULL, -- Positive for inward, Negative for outward
    quantity_before INTEGER NOT NULL,
    quantity_after INTEGER NOT NULL,

    -- Value Changes
    value_change DECIMAL(12, 2),
    unit_cost DECIMAL(10, 2),

    -- Reference
    reference_type VARCHAR(50),
    reference_number VARCHAR(100),

    -- User & Notes
    performed_by UUID REFERENCES users(id),
    remarks TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for better performance
CREATE INDEX idx_stock_entries_company_id ON stock_entries(company_id);
CREATE INDEX idx_stock_entries_entry_type ON stock_entries(entry_type);
CREATE INDEX idx_stock_entries_status ON stock_entries(status);
CREATE INDEX idx_stock_entries_entry_date ON stock_entries(entry_date DESC);
CREATE INDEX idx_stock_entries_entry_number ON stock_entries(entry_number);

CREATE INDEX idx_stock_entry_items_entry_id ON stock_entry_items(stock_entry_id);
CREATE INDEX idx_stock_entry_items_product_id ON stock_entry_items(product_id);

CREATE INDEX idx_stock_transactions_company_id ON stock_transactions(company_id);
CREATE INDEX idx_stock_transactions_product_id ON stock_transactions(product_id);
CREATE INDEX idx_stock_transactions_date ON stock_transactions(transaction_date DESC);
CREATE INDEX idx_stock_transactions_entry_id ON stock_transactions(stock_entry_id);

-- Enable Row Level Security
ALTER TABLE stock_entries ENABLE ROW LEVEL SECURITY;
ALTER TABLE stock_entry_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE stock_transactions ENABLE ROW LEVEL SECURITY;

-- RLS Policies for stock_entries
CREATE POLICY stock_entries_company_policy ON stock_entries
    FOR ALL
    USING (
        company_id IN (
            SELECT company_id FROM users WHERE id = auth.uid()
        )
    );

-- RLS Policies for stock_entry_items
CREATE POLICY stock_entry_items_company_policy ON stock_entry_items
    FOR ALL
    USING (
        stock_entry_id IN (
            SELECT id FROM stock_entries
            WHERE company_id IN (
                SELECT company_id FROM users WHERE id = auth.uid()
            )
        )
    );

-- RLS Policies for stock_transactions
CREATE POLICY stock_transactions_company_policy ON stock_transactions
    FOR ALL
    USING (
        company_id IN (
            SELECT company_id FROM users WHERE id = auth.uid()
        )
    );

-- Function to auto-generate entry numbers
CREATE OR REPLACE FUNCTION generate_entry_number(p_entry_type VARCHAR, p_company_id UUID)
RETURNS VARCHAR AS $$
DECLARE
    prefix VARCHAR;
    year VARCHAR;
    next_number INTEGER;
    entry_number VARCHAR;
BEGIN
    -- Set prefix based on entry type
    prefix := CASE
        WHEN p_entry_type = 'inward' THEN 'INW'
        WHEN p_entry_type = 'outward' THEN 'OUT'
        ELSE 'STK'
    END;

    -- Get current year
    year := TO_CHAR(CURRENT_DATE, 'YYYY');

    -- Get next number for this type and year
    SELECT COALESCE(MAX(CAST(SPLIT_PART(entry_number, '-', 3) AS INTEGER)), 0) + 1
    INTO next_number
    FROM stock_entries
    WHERE company_id = p_company_id
    AND entry_type = p_entry_type
    AND entry_number LIKE prefix || '-' || year || '-%';

    -- Format: INW-2025-001
    entry_number := prefix || '-' || year || '-' || LPAD(next_number::TEXT, 3, '0');

    RETURN entry_number;
END;
$$ LANGUAGE plpgsql;

-- Function to update product stock quantity
CREATE OR REPLACE FUNCTION update_product_stock()
RETURNS TRIGGER AS $$
DECLARE
    v_quantity_change INTEGER;
    v_stock_entry RECORD;
    v_quantity_before INTEGER;
    v_quantity_after INTEGER;
BEGIN
    -- Get stock entry details
    SELECT * INTO v_stock_entry FROM stock_entries WHERE id = NEW.stock_entry_id;

    -- Only process if stock entry is approved/completed
    IF v_stock_entry.status NOT IN ('approved', 'completed') THEN
        RETURN NEW;
    END IF;

    -- Get current stock quantity
    SELECT stock_quantity INTO v_quantity_before FROM products WHERE id = NEW.product_id;

    -- Calculate quantity change based on entry type
    IF v_stock_entry.entry_type = 'inward' THEN
        v_quantity_change := NEW.quantity;
    ELSIF v_stock_entry.entry_type = 'outward' THEN
        v_quantity_change := -NEW.quantity;
    END IF;

    -- Update product stock
    UPDATE products
    SET stock_quantity = stock_quantity + v_quantity_change
    WHERE id = NEW.product_id
    RETURNING stock_quantity INTO v_quantity_after;

    -- Create transaction record
    INSERT INTO stock_transactions (
        company_id,
        product_id,
        stock_entry_id,
        stock_entry_item_id,
        transaction_type,
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
        v_stock_entry.company_id,
        NEW.product_id,
        NEW.stock_entry_id,
        NEW.id,
        v_stock_entry.entry_type,
        v_quantity_change,
        v_quantity_before,
        v_quantity_after,
        NEW.unit_price,
        NEW.total_price * (CASE WHEN v_stock_entry.entry_type = 'inward' THEN 1 ELSE -1 END),
        v_stock_entry.reference_type,
        v_stock_entry.reference_number,
        v_stock_entry.created_by,
        v_stock_entry.remarks
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Function to reverse stock on entry deletion/rejection
CREATE OR REPLACE FUNCTION reverse_product_stock()
RETURNS TRIGGER AS $$
DECLARE
    v_quantity_change INTEGER;
    v_stock_entry RECORD;
BEGIN
    -- Get stock entry details
    SELECT * INTO v_stock_entry FROM stock_entries WHERE id = OLD.stock_entry_id;

    -- Only reverse if stock entry was approved/completed
    IF v_stock_entry.status NOT IN ('approved', 'completed') THEN
        RETURN OLD;
    END IF;

    -- Calculate reverse quantity change
    IF v_stock_entry.entry_type = 'inward' THEN
        v_quantity_change := -OLD.quantity;
    ELSIF v_stock_entry.entry_type = 'outward' THEN
        v_quantity_change := OLD.quantity;
    END IF;

    -- Update product stock
    UPDATE products
    SET stock_quantity = stock_quantity + v_quantity_change
    WHERE id = OLD.product_id;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- Trigger to update stock on item insert (when entry is approved)
CREATE TRIGGER trigger_update_stock_on_approval
    AFTER INSERT ON stock_entry_items
    FOR EACH ROW
    EXECUTE FUNCTION update_product_stock();

-- Trigger to reverse stock on item delete
CREATE TRIGGER trigger_reverse_stock_on_delete
    BEFORE DELETE ON stock_entry_items
    FOR EACH ROW
    EXECUTE FUNCTION reverse_product_stock();

-- Trigger to update total quantity and value in stock_entries
CREATE OR REPLACE FUNCTION update_stock_entry_totals()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE stock_entries
    SET
        total_quantity = (
            SELECT COALESCE(SUM(quantity), 0)
            FROM stock_entry_items
            WHERE stock_entry_id = COALESCE(NEW.stock_entry_id, OLD.stock_entry_id)
        ),
        total_value = (
            SELECT COALESCE(SUM(total_price), 0)
            FROM stock_entry_items
            WHERE stock_entry_id = COALESCE(NEW.stock_entry_id, OLD.stock_entry_id)
        )
    WHERE id = COALESCE(NEW.stock_entry_id, OLD.stock_entry_id);

    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_entry_totals
    AFTER INSERT OR UPDATE OR DELETE ON stock_entry_items
    FOR EACH ROW
    EXECUTE FUNCTION update_stock_entry_totals();

-- Trigger to update updated_at timestamps
CREATE TRIGGER update_stock_entries_updated_at
    BEFORE UPDATE ON stock_entries
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_stock_entry_items_updated_at
    BEFORE UPDATE ON stock_entry_items
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create view for stock summary by product
CREATE OR REPLACE VIEW v_stock_summary AS
SELECT
    p.id AS product_id,
    p.company_id,
    p.product_name,
    p.product_reference_no,
    p.category,
    p.stock_quantity,
    p.min_stock_level,
    p.reorder_level,
    p.unit_of_measure,
    COALESCE(SUM(CASE WHEN st.transaction_type = 'inward' THEN st.quantity_change ELSE 0 END), 0) AS total_inward,
    COALESCE(SUM(CASE WHEN st.transaction_type = 'outward' THEN ABS(st.quantity_change) ELSE 0 END), 0) AS total_outward,
    CASE
        WHEN p.stock_quantity <= p.min_stock_level THEN 'critical'
        WHEN p.stock_quantity <= p.reorder_level THEN 'low'
        ELSE 'adequate'
    END AS stock_status,
    p.created_at,
    p.updated_at
FROM products p
LEFT JOIN stock_transactions st ON p.id = st.product_id
GROUP BY p.id, p.company_id, p.product_name, p.product_reference_no, p.category,
         p.stock_quantity, p.min_stock_level, p.reorder_level, p.unit_of_measure,
         p.created_at, p.updated_at;

-- Create view for recent stock movements
CREATE OR REPLACE VIEW v_recent_stock_movements AS
SELECT
    st.id,
    st.company_id,
    st.transaction_date,
    st.transaction_type,
    p.product_name,
    p.product_reference_no,
    st.quantity_change,
    st.quantity_before,
    st.quantity_after,
    st.unit_cost,
    st.value_change,
    st.reference_type,
    st.reference_number,
    u.full_name AS performed_by_name,
    st.remarks
FROM stock_transactions st
JOIN products p ON st.product_id = p.id
LEFT JOIN users u ON st.performed_by = u.id
ORDER BY st.transaction_date DESC;

-- Grant permissions
GRANT SELECT ON v_stock_summary TO authenticated;
GRANT SELECT ON v_recent_stock_movements TO authenticated;

-- Insert sample data (optional - remove in production)
COMMENT ON TABLE stock_entries IS 'Main table for stock inward and outward entries';
COMMENT ON TABLE stock_entry_items IS 'Line items for each stock entry';
COMMENT ON TABLE stock_transactions IS 'Complete audit trail of all stock movements';
COMMENT ON COLUMN products.stock_quantity IS 'Current stock quantity on hand';
COMMENT ON COLUMN products.min_stock_level IS 'Minimum stock level - alert when reached';
COMMENT ON COLUMN products.reorder_level IS 'Reorder point for replenishment';
