-- Create table to track conversion status history
CREATE TABLE IF NOT EXISTS flatrix_conversion_history (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    deal_id UUID REFERENCES flatrix_deals(id) ON DELETE CASCADE,
    lead_id UUID REFERENCES flatrix_leads(id) ON DELETE CASCADE,
    previous_status VARCHAR(50),
    new_status VARCHAR(50) NOT NULL,
    changed_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    changed_by VARCHAR(100) DEFAULT 'Admin User', -- You can update this to reference actual users
    notes TEXT,
    conversion_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_flatrix_conversion_history_deal_id ON flatrix_conversion_history(deal_id);
CREATE INDEX IF NOT EXISTS idx_flatrix_conversion_history_lead_id ON flatrix_conversion_history(lead_id);
CREATE INDEX IF NOT EXISTS idx_flatrix_conversion_history_changed_at ON flatrix_conversion_history(changed_at DESC);

-- Create function to automatically log conversion status changes
CREATE OR REPLACE FUNCTION log_conversion_status_change()
RETURNS TRIGGER AS $$
BEGIN
    -- Only log if conversion_status actually changed
    IF (OLD.conversion_status IS DISTINCT FROM NEW.conversion_status) THEN
        INSERT INTO flatrix_conversion_history (
            deal_id,
            lead_id,
            previous_status,
            new_status,
            changed_at,
            conversion_date,
            notes
        ) VALUES (
            NEW.id,
            NEW.lead_id,
            OLD.conversion_status,
            NEW.conversion_status,
            NOW(),
            NEW.conversion_date,
            CASE 
                WHEN NEW.conversion_status = 'BOOKED' THEN 'Customer booked the flat'
                WHEN NEW.conversion_status = 'NOT_BOOKED' THEN 'Customer decided not to book'
                WHEN NEW.conversion_status = 'CANCELLED' THEN 'Booking was cancelled'
                WHEN NEW.conversion_status = 'PENDING' THEN 'Status reset to pending'
                ELSE 'Status changed'
            END
        );
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to automatically log conversion changes
DROP TRIGGER IF EXISTS log_conversion_change_trigger ON flatrix_deals;
CREATE TRIGGER log_conversion_change_trigger
    AFTER UPDATE ON flatrix_deals
    FOR EACH ROW
    EXECUTE FUNCTION log_conversion_status_change();

-- Insert initial history for existing deals (one-time migration)
INSERT INTO flatrix_conversion_history (
    deal_id,
    lead_id,
    previous_status,
    new_status,
    changed_at,
    conversion_date,
    notes
)
SELECT 
    d.id,
    d.lead_id,
    NULL as previous_status,
    d.conversion_status,
    d.created_at,
    d.conversion_date,
    'Initial status when deal was created'
FROM flatrix_deals d
WHERE d.conversion_status IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM flatrix_conversion_history ch WHERE ch.deal_id = d.id
);