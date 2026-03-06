-- Add missing columns to installations table step by step
-- Run each command separately to identify any issues

-- Step 1: Add basic location columns
ALTER TABLE installations ADD COLUMN city VARCHAR(100);
ALTER TABLE installations ADD COLUMN state VARCHAR(100);  
ALTER TABLE installations ADD COLUMN pincode VARCHAR(10);

-- Step 2: Add customer information
ALTER TABLE installations ADD COLUMN customer_name VARCHAR(255);
ALTER TABLE installations ADD COLUMN customer_phone VARCHAR(20);
ALTER TABLE installations ADD COLUMN customer_email VARCHAR(255);

-- Step 3: Add source tracking
ALTER TABLE installations ADD COLUMN source_type VARCHAR(50);
ALTER TABLE installations ADD COLUMN source_reference VARCHAR(100);

-- Step 4: Add product details
ALTER TABLE installations ADD COLUMN product_model VARCHAR(255);
ALTER TABLE installations ADD COLUMN quantity INTEGER DEFAULT 1;
ALTER TABLE installations ADD COLUMN equipment_value DECIMAL(10,2);

-- Step 5: Add scheduling
ALTER TABLE installations ADD COLUMN scheduled_date DATE;
ALTER TABLE installations ADD COLUMN scheduled_time TIME;
ALTER TABLE installations ADD COLUMN estimated_duration INTEGER DEFAULT 2;

-- Step 6: Add assignment
ALTER TABLE installations ADD COLUMN assigned_technician VARCHAR(255);
ALTER TABLE installations ADD COLUMN technician_phone VARCHAR(20);

-- Step 7: Add installation details
ALTER TABLE installations ADD COLUMN installation_type VARCHAR(50) DEFAULT 'new_installation';

-- Step 8: Add notes
ALTER TABLE installations ADD COLUMN pre_installation_notes TEXT;
ALTER TABLE installations ADD COLUMN special_instructions TEXT;
ALTER TABLE installations ADD COLUMN required_tools TEXT;
ALTER TABLE installations ADD COLUMN safety_requirements TEXT;

-- Step 9: Add warranty
ALTER TABLE installations ADD COLUMN warranty_period INTEGER DEFAULT 24;
ALTER TABLE installations ADD COLUMN warranty_terms TEXT;

-- Step 10: Add financial
ALTER TABLE installations ADD COLUMN installation_cost DECIMAL(10,2);
ALTER TABLE installations ADD COLUMN additional_charges DECIMAL(10,2);
ALTER TABLE installations ADD COLUMN total_cost DECIMAL(10,2);