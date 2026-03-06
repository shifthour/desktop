-- Complete fix for complaints table - add ALL missing columns
DO $$ 
BEGIN
    -- Basic columns that might be missing
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'category'
    ) THEN
        ALTER TABLE complaints ADD COLUMN category VARCHAR(50);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'city'
    ) THEN
        ALTER TABLE complaints ADD COLUMN city VARCHAR(100);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'state'
    ) THEN
        ALTER TABLE complaints ADD COLUMN state VARCHAR(100);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'pincode'
    ) THEN
        ALTER TABLE complaints ADD COLUMN pincode VARCHAR(10);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'customer_phone'
    ) THEN
        ALTER TABLE complaints ADD COLUMN customer_phone VARCHAR(20);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'customer_email'
    ) THEN
        ALTER TABLE complaints ADD COLUMN customer_email VARCHAR(255);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'customer_address'
    ) THEN
        ALTER TABLE complaints ADD COLUMN customer_address TEXT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'product_name'
    ) THEN
        ALTER TABLE complaints ADD COLUMN product_name VARCHAR(255);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'product_model'
    ) THEN
        ALTER TABLE complaints ADD COLUMN product_model VARCHAR(255);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'serial_number'
    ) THEN
        ALTER TABLE complaints ADD COLUMN serial_number VARCHAR(255);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'purchase_date'
    ) THEN
        ALTER TABLE complaints ADD COLUMN purchase_date DATE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'warranty_status'
    ) THEN
        ALTER TABLE complaints ADD COLUMN warranty_status VARCHAR(50);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'communication_history'
    ) THEN
        ALTER TABLE complaints ADD COLUMN communication_history JSONB DEFAULT '[]'::jsonb;
    END IF;

    -- Financial columns
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'compensation_amount'
    ) THEN
        ALTER TABLE complaints ADD COLUMN compensation_amount DECIMAL(10,2);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'refund_amount'
    ) THEN
        ALTER TABLE complaints ADD COLUMN refund_amount DECIMAL(10,2);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'cost_to_resolve'
    ) THEN
        ALTER TABLE complaints ADD COLUMN cost_to_resolve DECIMAL(10,2);
    END IF;

    -- Resolution columns
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'resolution_description'
    ) THEN
        ALTER TABLE complaints ADD COLUMN resolution_description TEXT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'resolution_date'
    ) THEN
        ALTER TABLE complaints ADD COLUMN resolution_date DATE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'resolution_time_hours'
    ) THEN
        ALTER TABLE complaints ADD COLUMN resolution_time_hours DECIMAL(8,2);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'customer_satisfaction_rating'
    ) THEN
        ALTER TABLE complaints ADD COLUMN customer_satisfaction_rating INTEGER;
    END IF;

    -- Follow-up columns
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'follow_up_required'
    ) THEN
        ALTER TABLE complaints ADD COLUMN follow_up_required BOOLEAN DEFAULT FALSE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'follow_up_date'
    ) THEN
        ALTER TABLE complaints ADD COLUMN follow_up_date DATE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'follow_up_notes'
    ) THEN
        ALTER TABLE complaints ADD COLUMN follow_up_notes TEXT;
    END IF;

    -- Escalation columns
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'escalation_level'
    ) THEN
        ALTER TABLE complaints ADD COLUMN escalation_level INTEGER DEFAULT 0;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'escalated_to'
    ) THEN
        ALTER TABLE complaints ADD COLUMN escalated_to VARCHAR(255);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'escalation_date'
    ) THEN
        ALTER TABLE complaints ADD COLUMN escalation_date DATE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'escalation_reason'
    ) THEN
        ALTER TABLE complaints ADD COLUMN escalation_reason TEXT;
    END IF;

    -- Additional analysis columns
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'root_cause'
    ) THEN
        ALTER TABLE complaints ADD COLUMN root_cause TEXT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'preventive_action'
    ) THEN
        ALTER TABLE complaints ADD COLUMN preventive_action TEXT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'lessons_learned'
    ) THEN
        ALTER TABLE complaints ADD COLUMN lessons_learned TEXT;
    END IF;

    -- Reference columns
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'installation_id'
    ) THEN
        ALTER TABLE complaints ADD COLUMN installation_id UUID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'amc_contract_id'
    ) THEN
        ALTER TABLE complaints ADD COLUMN amc_contract_id UUID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'source_reference'
    ) THEN
        ALTER TABLE complaints ADD COLUMN source_reference VARCHAR(255);
    END IF;

    -- Assignment columns
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'assigned_to'
    ) THEN
        ALTER TABLE complaints ADD COLUMN assigned_to VARCHAR(255);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'complaints' AND column_name = 'department'
    ) THEN
        ALTER TABLE complaints ADD COLUMN department VARCHAR(100);
    END IF;

END $$;

-- Show final table structure
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'complaints' 
ORDER BY ordinal_position;