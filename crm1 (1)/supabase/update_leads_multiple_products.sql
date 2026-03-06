-- Update leads table to support multiple products
-- Execute this script in Supabase SQL Editor

-- Create a new table for lead products (many-to-many relationship)
CREATE TABLE IF NOT EXISTS lead_products (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lead_id UUID NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    product_id UUID REFERENCES products(id) ON DELETE SET NULL,
    product_name TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    price_per_unit DECIMAL(15,2),
    total_amount DECIMAL(15,2) GENERATED ALWAYS AS (quantity * price_per_unit) STORED,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL
);

-- Create indexes for the lead_products table
CREATE INDEX IF NOT EXISTS idx_lead_products_lead_id ON lead_products(lead_id);
CREATE INDEX IF NOT EXISTS idx_lead_products_product_id ON lead_products(product_id);

-- Add trigger for updated_at
CREATE TRIGGER update_lead_products_updated_at 
    BEFORE UPDATE ON lead_products
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions
GRANT ALL ON lead_products TO authenticated;
GRANT SELECT ON lead_products TO anon;

-- Add a computed budget field to leads table based on lead_products
-- First, add a column for manual budget override
ALTER TABLE leads ADD COLUMN IF NOT EXISTS manual_budget DECIMAL(15,2);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS budget_notes TEXT;

-- Create a view that calculates total budget from lead_products
CREATE OR REPLACE VIEW leads_with_calculated_budget AS
SELECT 
    l.*,
    COALESCE(l.manual_budget, lp_totals.calculated_budget, l.budget) as effective_budget,
    lp_totals.calculated_budget,
    lp_totals.total_products,
    lp_totals.total_quantity
FROM leads l
LEFT JOIN (
    SELECT 
        lead_id,
        SUM(total_amount) as calculated_budget,
        COUNT(*) as total_products,
        SUM(quantity) as total_quantity
    FROM lead_products 
    GROUP BY lead_id
) lp_totals ON l.id = lp_totals.lead_id;

-- Grant permissions on the view
GRANT SELECT ON leads_with_calculated_budget TO authenticated;
GRANT SELECT ON leads_with_calculated_budget TO anon;

-- Create a function to get products for a lead
CREATE OR REPLACE FUNCTION get_lead_products(lead_uuid UUID)
RETURNS TABLE (
    id UUID,
    product_id UUID,
    product_name TEXT,
    quantity INTEGER,
    price_per_unit DECIMAL(15,2),
    total_amount DECIMAL(15,2),
    notes TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        lp.id,
        lp.product_id,
        lp.product_name,
        lp.quantity,
        lp.price_per_unit,
        lp.total_amount,
        lp.notes
    FROM lead_products lp
    WHERE lp.lead_id = lead_uuid
    ORDER BY lp.created_at;
END;
$$ LANGUAGE plpgsql;

-- Grant execute permission on the function
GRANT EXECUTE ON FUNCTION get_lead_products(UUID) TO authenticated;

-- Create a function to update lead products
CREATE OR REPLACE FUNCTION update_lead_products(
    lead_uuid UUID,
    products_data JSONB
) RETURNS VOID AS $$
DECLARE
    product_item JSONB;
BEGIN
    -- Delete existing products for this lead
    DELETE FROM lead_products WHERE lead_id = lead_uuid;
    
    -- Insert new products
    FOR product_item IN SELECT * FROM jsonb_array_elements(products_data)
    LOOP
        INSERT INTO lead_products (
            lead_id,
            product_id,
            product_name,
            quantity,
            price_per_unit,
            notes
        ) VALUES (
            lead_uuid,
            (product_item->>'product_id')::UUID,
            product_item->>'product_name',
            (product_item->>'quantity')::INTEGER,
            (product_item->>'price_per_unit')::DECIMAL(15,2),
            product_item->>'notes'
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Grant execute permission on the function
GRANT EXECUTE ON FUNCTION update_lead_products(UUID, JSONB) TO authenticated;

-- Add comments for documentation
COMMENT ON TABLE lead_products IS 'Junction table for leads and products with quantities and pricing';
COMMENT ON COLUMN lead_products.total_amount IS 'Computed field: quantity * price_per_unit';
COMMENT ON VIEW leads_with_calculated_budget IS 'Leads with calculated budget from associated products';
COMMENT ON FUNCTION get_lead_products(UUID) IS 'Returns all products associated with a lead';
COMMENT ON FUNCTION update_lead_products(UUID, JSONB) IS 'Updates all products for a lead (replaces existing)';

-- Insert sample data to test (optional - remove in production)
-- This assumes you have existing leads and products
/*
-- Example: Add multiple products to an existing lead
INSERT INTO lead_products (lead_id, product_id, product_name, quantity, price_per_unit, notes) 
SELECT 
    l.id as lead_id,
    p.id as product_id,
    p.product_name,
    CASE 
        WHEN p.product_name ILIKE '%spectrophotometer%' THEN 2
        WHEN p.product_name ILIKE '%microscope%' THEN 1
        ELSE 3
    END as quantity,
    p.price as price_per_unit,
    'Sample product for testing multiple products feature'
FROM leads l
CROSS JOIN products p
WHERE l.company_id = 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
AND p.company_id = 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
LIMIT 10; -- Limit to avoid too much test data
*/