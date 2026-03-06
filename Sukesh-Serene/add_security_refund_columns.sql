-- Add security refund columns to serene_tenants table
ALTER TABLE serene_tenants
ADD COLUMN IF NOT EXISTS security_refunded VARCHAR(3) DEFAULT 'no' CHECK (security_refunded IN ('yes', 'no')),
ADD COLUMN IF NOT EXISTS security_refunded_amount DECIMAL(10, 2) DEFAULT 0;

-- Add comment to columns for clarity
COMMENT ON COLUMN serene_tenants.security_refunded IS 'Whether security deposit has been refunded (yes/no)';
COMMENT ON COLUMN serene_tenants.security_refunded_amount IS 'Amount of security deposit refunded';
