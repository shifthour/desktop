-- Add site_visit_status to flatrix_deals table if not exists
ALTER TABLE flatrix_deals 
ADD COLUMN IF NOT EXISTS site_visit_status VARCHAR(50) DEFAULT 'NOT_VISITED';

-- Add site_visit_date to track when the site visit happened
ALTER TABLE flatrix_deals 
ADD COLUMN IF NOT EXISTS site_visit_date TIMESTAMPTZ;

-- Add lead_id reference if not exists (should already exist)
ALTER TABLE flatrix_deals 
ADD COLUMN IF NOT EXISTS lead_id UUID REFERENCES flatrix_leads(id);

-- Create or replace function to sync qualified leads to deals
CREATE OR REPLACE FUNCTION sync_qualified_lead_to_deal()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if status changed to QUALIFIED
    IF NEW.status = 'QUALIFIED' AND (OLD.status IS NULL OR OLD.status != 'QUALIFIED') THEN
        -- Check if deal already exists for this lead
        IF NOT EXISTS (SELECT 1 FROM flatrix_deals WHERE lead_id = NEW.id) THEN
            -- Create a new deal from the qualified lead
            INSERT INTO flatrix_deals (
                lead_id,
                user_id,
                channel_partner_id,
                property_id,
                status,
                deal_value,
                notes,
                site_visit_status,
                created_at,
                updated_at
            ) VALUES (
                NEW.id,
                NEW.assigned_to_id,
                NEW.channel_partner_id,
                NULL, -- Property can be assigned later
                'ACTIVE',
                NEW.budget_max, -- Use budget_max as initial deal value
                NEW.notes,
                'NOT_VISITED',
                NOW(),
                NOW()
            );
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop existing trigger if it exists
DROP TRIGGER IF EXISTS sync_qualified_lead_trigger ON flatrix_leads;

-- Create trigger to sync qualified leads to deals
CREATE TRIGGER sync_qualified_lead_trigger
AFTER INSERT OR UPDATE ON flatrix_leads
FOR EACH ROW
EXECUTE FUNCTION sync_qualified_lead_to_deal();

-- Create index for better performance
CREATE INDEX IF NOT EXISTS idx_flatrix_deals_site_visit_status ON flatrix_deals(site_visit_status);
CREATE INDEX IF NOT EXISTS idx_flatrix_deals_lead_id ON flatrix_deals(lead_id);

-- Update existing qualified leads to have deals (one-time sync)
INSERT INTO flatrix_deals (
    lead_id,
    user_id,
    channel_partner_id,
    property_id,
    status,
    deal_value,
    notes,
    site_visit_status,
    created_at,
    updated_at
)
SELECT 
    l.id,
    l.assigned_to_id,
    l.channel_partner_id,
    NULL,
    'ACTIVE',
    l.budget_max,
    l.notes,
    'NOT_VISITED',
    NOW(),
    NOW()
FROM flatrix_leads l
WHERE l.status = 'QUALIFIED'
AND NOT EXISTS (
    SELECT 1 FROM flatrix_deals d WHERE d.lead_id = l.id
)
ON CONFLICT DO NOTHING;