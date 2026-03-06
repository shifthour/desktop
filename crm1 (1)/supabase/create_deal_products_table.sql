-- Create deal_products table for multiple products support in deals
-- This table stores the junction between deals and products with quantities

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

-- Add indexes for better performance
CREATE INDEX IF NOT EXISTS idx_deal_products_deal_id ON deal_products(deal_id);
CREATE INDEX IF NOT EXISTS idx_deal_products_product_id ON deal_products(product_id);

-- Add updated_at trigger
CREATE OR REPLACE FUNCTION update_deal_products_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = TIMEZONE('utc'::text, NOW());
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_deal_products_updated_at_trigger
    BEFORE UPDATE ON deal_products
    FOR EACH ROW
    EXECUTE FUNCTION update_deal_products_updated_at();

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

-- Create a function to calculate total deal budget from products
CREATE OR REPLACE FUNCTION calculate_deal_budget(deal_uuid UUID)
RETURNS DECIMAL(15,2) AS $$
DECLARE
    total_budget DECIMAL(15,2);
BEGIN
    SELECT COALESCE(SUM(total_amount), 0)
    INTO total_budget
    FROM deal_products
    WHERE deal_id = deal_uuid;
    
    RETURN total_budget;
END;
$$ LANGUAGE plpgsql;