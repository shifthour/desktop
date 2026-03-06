-- Create contacts table
CREATE TABLE IF NOT EXISTS contacts (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  contact_name VARCHAR(255) NOT NULL,
  department VARCHAR(255),
  position VARCHAR(255),
  phone VARCHAR(50),
  email VARCHAR(255),
  assigned_to VARCHAR(255),
  status VARCHAR(50) DEFAULT 'Active',
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  
  -- Indexes for performance
  CONSTRAINT unique_contact_email_per_account UNIQUE (account_id, email)
);

-- Create indexes for better query performance
CREATE INDEX idx_contacts_company_id ON contacts(company_id);
CREATE INDEX idx_contacts_account_id ON contacts(account_id);
CREATE INDEX idx_contacts_status ON contacts(status);
CREATE INDEX idx_contacts_contact_name ON contacts(contact_name);
CREATE INDEX idx_contacts_email ON contacts(email);

-- Enable RLS (Row Level Security)
ALTER TABLE contacts ENABLE ROW LEVEL SECURITY;

-- Create RLS policies
-- Policy for users to see contacts in their company
CREATE POLICY "Users can view contacts in their company" ON contacts
  FOR SELECT
  USING (
    company_id IN (
      SELECT company_id FROM users WHERE id = auth.uid()
    )
  );

-- Policy for users to create contacts in their company
CREATE POLICY "Users can create contacts in their company" ON contacts
  FOR INSERT
  WITH CHECK (
    company_id IN (
      SELECT company_id FROM users WHERE id = auth.uid()
    )
  );

-- Policy for users to update contacts in their company
CREATE POLICY "Users can update contacts in their company" ON contacts
  FOR UPDATE
  USING (
    company_id IN (
      SELECT company_id FROM users WHERE id = auth.uid()
    )
  );

-- Policy for users to delete contacts in their company
CREATE POLICY "Users can delete contacts in their company" ON contacts
  FOR DELETE
  USING (
    company_id IN (
      SELECT company_id FROM users WHERE id = auth.uid()
    )
  );

-- Add trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_contacts_updated_at BEFORE UPDATE
  ON contacts FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();