-- Add payment_status column to flatrix_commissions table if it doesn't exist
ALTER TABLE flatrix_commissions 
ADD COLUMN IF NOT EXISTS payment_status VARCHAR(50) DEFAULT 'PENDING';

-- Add deal_id column to flatrix_commissions table to link with deals
ALTER TABLE flatrix_commissions 
ADD COLUMN IF NOT EXISTS deal_id UUID REFERENCES flatrix_deals(id) ON DELETE CASCADE;

-- Create function to automatically sync booked conversions to commissions
CREATE OR REPLACE FUNCTION sync_booked_conversions_to_commissions()
RETURNS TRIGGER AS $$
BEGIN
    -- When conversion status changes to BOOKED, create/update commission record
    IF (NEW.conversion_status = 'BOOKED' AND (OLD.conversion_status IS NULL OR OLD.conversion_status != 'BOOKED')) THEN
        -- Insert or update commission record
        INSERT INTO flatrix_commissions (
            deal_id,
            lead_id,
            amount,
            status,
            payment_status,
            commission_type,
            commission_percentage,
            created_at,
            updated_at
        ) VALUES (
            NEW.id,
            NEW.lead_id,
            COALESCE(NEW.deal_value * 0.05, 0), -- 5% commission rate, adjust as needed
            'PENDING',
            'PENDING',
            'SALES_COMMISSION',
            5.0,
            NOW(),
            NOW()
        )
        ON CONFLICT (deal_id) DO UPDATE SET
            amount = COALESCE(NEW.deal_value * 0.05, 0),
            updated_at = NOW();
            
    -- When conversion status changes from BOOKED to something else, remove commission record
    ELSIF (OLD.conversion_status = 'BOOKED' AND NEW.conversion_status != 'BOOKED') THEN
        DELETE FROM flatrix_commissions WHERE deal_id = NEW.id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to automatically sync booked conversions
DROP TRIGGER IF EXISTS sync_booked_conversions_trigger ON flatrix_deals;
CREATE TRIGGER sync_booked_conversions_trigger
    AFTER UPDATE ON flatrix_deals
    FOR EACH ROW
    EXECUTE FUNCTION sync_booked_conversions_to_commissions();

-- Create unique index on deal_id to prevent duplicates
CREATE UNIQUE INDEX IF NOT EXISTS idx_flatrix_commissions_deal_id_unique ON flatrix_commissions(deal_id);

-- Sync existing booked conversions to commissions (one-time migration)
INSERT INTO flatrix_commissions (
    deal_id,
    lead_id,
    amount,
    status,
    payment_status,
    commission_type,
    commission_percentage,
    created_at,
    updated_at
)
SELECT 
    d.id,
    d.lead_id,
    COALESCE(d.deal_value * 0.05, 0) as amount,
    'PENDING' as status,
    'PENDING' as payment_status,
    'SALES_COMMISSION' as commission_type,
    5.0 as commission_percentage,
    d.created_at,
    NOW() as updated_at
FROM flatrix_deals d
WHERE d.conversion_status = 'BOOKED'
AND NOT EXISTS (
    SELECT 1 FROM flatrix_commissions c WHERE c.deal_id = d.id
)
ON CONFLICT (deal_id) DO NOTHING;