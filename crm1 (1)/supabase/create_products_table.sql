-- Create products table
CREATE TABLE IF NOT EXISTS products (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    
    -- Product fields as per form
    product_name VARCHAR(255) NOT NULL,
    product_reference_no VARCHAR(255),
    description TEXT,
    principal VARCHAR(255),
    category VARCHAR(255),
    sub_category VARCHAR(255),
    price DECIMAL(10, 2),
    product_picture TEXT, -- Store as base64 or URL
    
    -- Additional fields from existing system
    branch VARCHAR(255),
    status VARCHAR(50) DEFAULT 'Active',
    assigned_to TEXT,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for better query performance
CREATE INDEX idx_products_company_id ON products(company_id);
CREATE INDEX idx_products_status ON products(status);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_principal ON products(principal);

-- Enable Row Level Security
ALTER TABLE products ENABLE ROW LEVEL SECURITY;

-- Create policy for company-based access
CREATE POLICY products_company_policy ON products
    FOR ALL
    USING (company_id IN (
        SELECT id FROM companies WHERE id = current_setting('app.current_company_id')::UUID
    ));

-- Create a trigger to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();