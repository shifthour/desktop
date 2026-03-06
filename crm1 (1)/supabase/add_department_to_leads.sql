-- Add department field to existing leads table
ALTER TABLE leads 
ADD COLUMN IF NOT EXISTS department TEXT;