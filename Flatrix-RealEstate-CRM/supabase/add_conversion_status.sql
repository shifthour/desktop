-- Add conversion_status to flatrix_deals table to track flat booking after site visits
ALTER TABLE flatrix_deals 
ADD COLUMN IF NOT EXISTS conversion_status VARCHAR(50) DEFAULT 'PENDING';

-- Add conversion_date to track when the conversion happened
ALTER TABLE flatrix_deals 
ADD COLUMN IF NOT EXISTS conversion_date TIMESTAMPTZ;

-- Create index for better performance on conversion queries
CREATE INDEX IF NOT EXISTS idx_flatrix_deals_conversion_status ON flatrix_deals(conversion_status);

-- Update existing deals to have PENDING conversion status
UPDATE flatrix_deals 
SET conversion_status = 'PENDING' 
WHERE conversion_status IS NULL;

-- Comments for clarity:
-- conversion_status values:
-- 'PENDING' - Site visit done, awaiting booking decision
-- 'BOOKED' - Customer booked a flat after site visit  
-- 'NOT_BOOKED' - Customer decided not to book after site visit
-- 'CANCELLED' - Site visit was cancelled, no conversion possible