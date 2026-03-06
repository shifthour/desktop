-- Create complaints table
CREATE TABLE IF NOT EXISTS serene_complaints (
  id SERIAL PRIMARY KEY,
  tenant_id INTEGER NOT NULL REFERENCES serene_tenants(id) ON DELETE CASCADE,
  complaint_description TEXT NOT NULL,
  status VARCHAR(20) DEFAULT 'raised' CHECK (status IN ('raised', 'in_progress', 'resolved')),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  resolved_at TIMESTAMP,
  resolved_by INTEGER REFERENCES serene_users(id),
  resolution_description TEXT,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_complaints_tenant_id ON serene_complaints(tenant_id);
CREATE INDEX IF NOT EXISTS idx_complaints_status ON serene_complaints(status);

-- Add trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_complaints_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_complaints_updated_at
BEFORE UPDATE ON serene_complaints
FOR EACH ROW
EXECUTE FUNCTION update_complaints_updated_at();
