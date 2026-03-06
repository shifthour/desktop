-- Update deals table to support multiple products
-- Execute this script in Supabase SQL Editor

-- Create a new table for deal products (many-to-many relationship)
CREATE TABLE IF NOT EXISTS deal_products (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    deal_id UUID NOT NULL REFERENCES deals(id) ON DELETE CASCADE,
    product_id UUID REFERENCES products(id) ON DELETE SET NULL,
    product_name TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    price_per_unit DECIMAL(15,2),
    total_amount DECIMAL(15,2) GENERATED ALWAYS AS (quantity * price_per_unit) STORED,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL
);

-- Create indexes for the deal_products table
CREATE INDEX IF NOT EXISTS idx_deal_products_deal_id ON deal_products(deal_id);
CREATE INDEX IF NOT EXISTS idx_deal_products_product_id ON deal_products(product_id);

-- Add trigger for updated_at
CREATE TRIGGER update_deal_products_updated_at 
    BEFORE UPDATE ON deal_products
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions
GRANT ALL ON deal_products TO authenticated;
GRANT SELECT ON deal_products TO anon;

-- Add a computed value field to deals table based on deal_products
-- First, add a column for manual value override
ALTER TABLE deals ADD COLUMN IF NOT EXISTS manual_value DECIMAL(15,2);
ALTER TABLE deals ADD COLUMN IF NOT EXISTS value_notes TEXT;

-- Create a view that calculates total value from deal_products
CREATE OR REPLACE VIEW deals_with_calculated_value AS
SELECT 
    d.*,
    COALESCE(d.manual_value, dp_totals.calculated_value, d.value) as effective_value,
    dp_totals.calculated_value,
    dp_totals.total_products,
    dp_totals.total_quantity
FROM deals d
LEFT JOIN (
    SELECT 
        deal_id,
        SUM(total_amount) as calculated_value,
        COUNT(*) as total_products,
        SUM(quantity) as total_quantity
    FROM deal_products 
    GROUP BY deal_id
) dp_totals ON d.id = dp_totals.deal_id;

-- Grant permissions on the view
GRANT SELECT ON deals_with_calculated_value TO authenticated;
GRANT SELECT ON deals_with_calculated_value TO anon;

-- Create a function to get products for a deal
CREATE OR REPLACE FUNCTION get_deal_products(deal_uuid UUID)
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
        dp.id,
        dp.product_id,
        dp.product_name,
        dp.quantity,
        dp.price_per_unit,
        dp.total_amount,
        dp.notes
    FROM deal_products dp
    WHERE dp.deal_id = deal_uuid
    ORDER BY dp.created_at;
END;
$$ LANGUAGE plpgsql;

-- Grant execute permission on the function
GRANT EXECUTE ON FUNCTION get_deal_products(UUID) TO authenticated;

-- Create a function to update deal products
CREATE OR REPLACE FUNCTION update_deal_products(
    deal_uuid UUID,
    products_data JSONB
) RETURNS VOID AS $$
DECLARE
    product_item JSONB;
BEGIN
    -- Delete existing products for this deal
    DELETE FROM deal_products WHERE deal_id = deal_uuid;
    
    -- Insert new products
    FOR product_item IN SELECT * FROM jsonb_array_elements(products_data)
    LOOP
        INSERT INTO deal_products (
            deal_id,
            product_id,
            product_name,
            quantity,
            price_per_unit,
            notes
        ) VALUES (
            deal_uuid,
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
GRANT EXECUTE ON FUNCTION update_deal_products(UUID, JSONB) TO authenticated;

-- Add comments for documentation
COMMENT ON TABLE deal_products IS 'Junction table for deals and products with quantities and pricing';
COMMENT ON COLUMN deal_products.total_amount IS 'Computed field: quantity * price_per_unit';
COMMENT ON VIEW deals_with_calculated_value IS 'Deals with calculated value from associated products';
COMMENT ON FUNCTION get_deal_products(UUID) IS 'Returns all products associated with a deal';
COMMENT ON FUNCTION update_deal_products(UUID, JSONB) IS 'Updates all products for a deal (replaces existing)';