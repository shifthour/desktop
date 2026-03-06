-- Add foreign key columns to existing flatrix_leads table
-- This assumes the flatrix_leads table already exists with basic structure

-- Add columns if they don't exist
ALTER TABLE flatrix_leads 
ADD COLUMN IF NOT EXISTS assigned_to_id UUID REFERENCES flatrix_users(id),
ADD COLUMN IF NOT EXISTS created_by_id UUID REFERENCES flatrix_users(id),
ADD COLUMN IF NOT EXISTS channel_partner_id UUID REFERENCES flatrix_channel_partners(id),
ADD COLUMN IF NOT EXISTS status lead_status DEFAULT 'NEW',
ADD COLUMN IF NOT EXISTS budget_min DECIMAL(15,2),
ADD COLUMN IF NOT EXISTS budget_max DECIMAL(15,2),
ADD COLUMN IF NOT EXISTS interested_in property_type[],
ADD COLUMN IF NOT EXISTS preferred_location TEXT,
ADD COLUMN IF NOT EXISTS source VARCHAR(100),
ADD COLUMN IF NOT EXISTS notes TEXT,
ADD COLUMN IF NOT EXISTS last_contacted_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

-- Add indexes for the foreign keys if they don't exist
CREATE INDEX IF NOT EXISTS idx_flatrix_leads_assigned_to ON flatrix_leads(assigned_to_id);
CREATE INDEX IF NOT EXISTS idx_flatrix_leads_created_by ON flatrix_leads(created_by_id);
CREATE INDEX IF NOT EXISTS idx_flatrix_leads_channel_partner ON flatrix_leads(channel_partner_id);
CREATE INDEX IF NOT EXISTS idx_flatrix_leads_status ON flatrix_leads(status);

-- Add updated_at trigger for leads table
CREATE TRIGGER update_flatrix_leads_updated_at BEFORE UPDATE ON flatrix_leads
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Update foreign key references in other tables to reference flatrix_leads
ALTER TABLE flatrix_deals 
DROP CONSTRAINT IF EXISTS flatrix_deals_lead_id_fkey,
ADD CONSTRAINT flatrix_deals_lead_id_fkey 
FOREIGN KEY (lead_id) REFERENCES flatrix_leads(id);

ALTER TABLE flatrix_activities
DROP CONSTRAINT IF EXISTS flatrix_activities_lead_id_fkey,
ADD CONSTRAINT flatrix_activities_lead_id_fkey 
FOREIGN KEY (lead_id) REFERENCES flatrix_leads(id);

ALTER TABLE flatrix_tasks
DROP CONSTRAINT IF EXISTS flatrix_tasks_lead_id_fkey,
ADD CONSTRAINT flatrix_tasks_lead_id_fkey 
FOREIGN KEY (lead_id) REFERENCES flatrix_leads(id);