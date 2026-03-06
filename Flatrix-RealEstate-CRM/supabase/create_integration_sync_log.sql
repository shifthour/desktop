-- Create integration_sync_log table to track last sync times
CREATE TABLE IF NOT EXISTS flatrix_integration_sync_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  integration_name TEXT NOT NULL UNIQUE,
  last_sync_time TIMESTAMP WITH TIME ZONE NOT NULL,
  last_lead_date TIMESTAMP WITH TIME ZONE,
  leads_fetched INTEGER DEFAULT 0,
  leads_created INTEGER DEFAULT 0,
  leads_updated INTEGER DEFAULT 0,
  status TEXT DEFAULT 'success',
  error_message TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_integration_name ON flatrix_integration_sync_log(integration_name);

-- Insert initial record for Housing.com (set to 24 hours ago for first run)
INSERT INTO flatrix_integration_sync_log (
  integration_name,
  last_sync_time,
  leads_fetched,
  status
) VALUES (
  'Housing.com',
  NOW() - INTERVAL '24 hours',
  0,
  'initialized'
) ON CONFLICT (integration_name) DO NOTHING;

-- Display the table
SELECT * FROM flatrix_integration_sync_log;
