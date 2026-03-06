-- Add all missing columns to installations table (FIXED VERSION)
-- This will fix the "Failed to schedule installation from sales order" error completely

-- Add source information columns
ALTER TABLE installations 
ADD COLUMN IF NOT EXISTS source_type VARCHAR(50),
ADD COLUMN IF NOT EXISTS source_reference VARCHAR(100);

-- Add customer location columns  
ALTER TABLE installations 
ADD COLUMN IF NOT EXISTS customer_name VARCHAR(255),
ADD COLUMN IF NOT EXISTS customer_phone VARCHAR(20),
ADD COLUMN IF NOT EXISTS customer_email VARCHAR(255),
ADD COLUMN IF NOT EXISTS city VARCHAR(100),
ADD COLUMN IF NOT EXISTS state VARCHAR(100),
ADD COLUMN IF NOT EXISTS pincode VARCHAR(10);

-- Add product details columns
ALTER TABLE installations 
ADD COLUMN IF NOT EXISTS product_model VARCHAR(255),
ADD COLUMN IF NOT EXISTS quantity INTEGER DEFAULT 1,
ADD COLUMN IF NOT EXISTS equipment_value DECIMAL(10,2);

-- Add installation details columns
ALTER TABLE installations 
ADD COLUMN IF NOT EXISTS installation_type VARCHAR(50) DEFAULT 'new_installation',
ADD COLUMN IF NOT EXISTS scheduled_date DATE,
ADD COLUMN IF NOT EXISTS scheduled_time TIME,
ADD COLUMN IF NOT EXISTS estimated_duration INTEGER DEFAULT 2;

-- Add assignment columns
ALTER TABLE installations 
ADD COLUMN IF NOT EXISTS assigned_technician VARCHAR(255),
ADD COLUMN IF NOT EXISTS technician_phone VARCHAR(20);

-- Add notes columns
ALTER TABLE installations 
ADD COLUMN IF NOT EXISTS pre_installation_notes TEXT,
ADD COLUMN IF NOT EXISTS special_instructions TEXT,
ADD COLUMN IF NOT EXISTS required_tools TEXT,
ADD COLUMN IF NOT EXISTS safety_requirements TEXT;

-- Add warranty columns
ALTER TABLE installations 
ADD COLUMN IF NOT EXISTS warranty_period INTEGER DEFAULT 24,
ADD COLUMN IF NOT EXISTS warranty_terms TEXT;

-- Add financial columns
ALTER TABLE installations 
ADD COLUMN IF NOT EXISTS installation_cost DECIMAL(10,2),
ADD COLUMN IF NOT EXISTS additional_charges DECIMAL(10,2),
ADD COLUMN IF NOT EXISTS total_cost DECIMAL(10,2);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_installations_city ON installations(city);
CREATE INDEX IF NOT EXISTS idx_installations_state ON installations(state);
CREATE INDEX IF NOT EXISTS idx_installations_pincode ON installations(pincode);
CREATE INDEX IF NOT EXISTS idx_installations_source_type ON installations(source_type);
CREATE INDEX IF NOT EXISTS idx_installations_scheduled_date ON installations(scheduled_date);

-- Add check constraints (removed IF NOT EXISTS which is not supported)
DO $$ 
BEGIN
    -- Check if constraint doesn't exist before adding
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'installations_source_type_check') THEN
        ALTER TABLE installations ADD CONSTRAINT installations_source_type_check 
            CHECK (source_type IN ('sales_order', 'invoice', 'direct'));
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'installations_installation_type_check') THEN
        ALTER TABLE installations ADD CONSTRAINT installations_installation_type_check 
            CHECK (installation_type IN ('new_installation', 'replacement', 'upgrade', 'maintenance'));
    END IF;
END $$;

-- Add comments for documentation
COMMENT ON COLUMN installations.source_type IS 'Source of installation request: sales_order, invoice, or direct';
COMMENT ON COLUMN installations.source_reference IS 'Reference number from source (SO number, invoice number, etc.)';
COMMENT ON COLUMN installations.customer_name IS 'Customer name for the installation';
COMMENT ON COLUMN installations.city IS 'City where installation will take place';
COMMENT ON COLUMN installations.state IS 'State where installation will take place';
COMMENT ON COLUMN installations.pincode IS 'Postal code for installation location';
COMMENT ON COLUMN installations.product_model IS 'Product model number';
COMMENT ON COLUMN installations.quantity IS 'Number of units to install';
COMMENT ON COLUMN installations.equipment_value IS 'Value of equipment being installed';
COMMENT ON COLUMN installations.installation_type IS 'Type of installation';
COMMENT ON COLUMN installations.scheduled_date IS 'Date when installation is scheduled';
COMMENT ON COLUMN installations.scheduled_time IS 'Time when installation is scheduled';
COMMENT ON COLUMN installations.estimated_duration IS 'Estimated duration in hours';
COMMENT ON COLUMN installations.assigned_technician IS 'Name of assigned technician';
COMMENT ON COLUMN installations.technician_phone IS 'Phone number of assigned technician';
COMMENT ON COLUMN installations.pre_installation_notes IS 'Notes to review before installation';
COMMENT ON COLUMN installations.special_instructions IS 'Special instructions for installation';
COMMENT ON COLUMN installations.required_tools IS 'Tools required for installation';
COMMENT ON COLUMN installations.safety_requirements IS 'Safety requirements for installation';
COMMENT ON COLUMN installations.warranty_period IS 'Warranty period in months';
COMMENT ON COLUMN installations.warranty_terms IS 'Warranty terms and conditions';
COMMENT ON COLUMN installations.installation_cost IS 'Cost of installation service';
COMMENT ON COLUMN installations.additional_charges IS 'Additional charges for installation';
COMMENT ON COLUMN installations.total_cost IS 'Total cost including all charges';