-- =====================================================
-- Add Payment Method and Paid To columns
-- Run this in Supabase SQL Editor
-- =====================================================

-- Add payment_method column to serene_payment_submissions table
ALTER TABLE serene_payment_submissions
ADD COLUMN IF NOT EXISTS payment_method VARCHAR(10) DEFAULT 'upi';

-- Add paid_to column to serene_payment_submissions table (for cash payments)
ALTER TABLE serene_payment_submissions
ADD COLUMN IF NOT EXISTS paid_to VARCHAR(255);

-- Verify the columns were added
SELECT column_name, data_type, character_maximum_length, column_default, is_nullable
FROM information_schema.columns
WHERE table_name = 'serene_payment_submissions'
AND column_name IN ('payment_method', 'paid_to');

-- View sample data to confirm
SELECT id, tenant_id, month, year, payment_method, paid_to, status
FROM serene_payment_submissions
LIMIT 5;

-- =====================================================
-- Column Details:
-- - payment_method: VARCHAR(10) - Either 'upi' or 'cash'
-- - paid_to: VARCHAR(255) - Name of person who received cash payment
-- =====================================================
