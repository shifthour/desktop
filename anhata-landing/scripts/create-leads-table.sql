-- Create flatrix_leads table for capturing user information
CREATE TABLE IF NOT EXISTS flatrix_leads (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  project_name VARCHAR(100) DEFAULT 'Anahata' NOT NULL,
  name VARCHAR(255) NOT NULL,
  phone VARCHAR(20) NOT NULL,
  email VARCHAR(255),
  source VARCHAR(100), -- e.g., 'site_visit', 'brochure_download', 'floor_plans', 'location'
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create index for better query performance
CREATE INDEX IF NOT EXISTS idx_flatrix_leads_phone ON flatrix_leads(phone);
CREATE INDEX IF NOT EXISTS idx_flatrix_leads_email ON flatrix_leads(email);
CREATE INDEX IF NOT EXISTS idx_flatrix_leads_created_at ON flatrix_leads(created_at);
CREATE INDEX IF NOT EXISTS idx_flatrix_leads_project_name ON flatrix_leads(project_name);

-- Enable Row Level Security (RLS)
ALTER TABLE flatrix_leads ENABLE ROW LEVEL SECURITY;

-- Create policy to allow inserts (for capturing leads)
CREATE POLICY "Allow insert flatrix_leads" ON flatrix_leads
  FOR INSERT WITH CHECK (true);

-- Create policy to allow select for authenticated users (if needed for admin)
CREATE POLICY "Allow select flatrix_leads for authenticated users" ON flatrix_leads
  FOR SELECT USING (auth.role() = 'authenticated');
