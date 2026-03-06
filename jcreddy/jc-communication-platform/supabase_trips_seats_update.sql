-- =====================================================
-- SUPABASE SCHEMA UPDATE - SEAT COLUMNS FOR JC_TRIPS
-- Run this in Supabase SQL Editor
-- =====================================================

-- Ensure all seat-related columns exist in jc_trips table
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS total_seats INTEGER DEFAULT 40;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS booked_seats INTEGER DEFAULT 0;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS available_seats INTEGER DEFAULT 40;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS blocked_seats INTEGER DEFAULT 0;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS phone_booking_count INTEGER DEFAULT 0;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS blocked_seat_numbers TEXT;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS phone_booking_seat_numbers TEXT;

-- Add coach details columns
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS coach_num VARCHAR(50);
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS coach_id INTEGER;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS coach_mobile_number VARCHAR(20);

-- Add captain and attendant columns
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS captain1_name VARCHAR(100);
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS captain1_phone VARCHAR(20);
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS captain2_name VARCHAR(100);
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS captain2_phone VARCHAR(20);
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS attendant_name VARCHAR(100);
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS attendant_phone VARCHAR(20);

-- Add route details
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS origin VARCHAR(100);
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS destination VARCHAR(100);
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS route_duration VARCHAR(10);

-- Add other details
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS chart_operated_by TEXT[];
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS pickup_van_details JSONB;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS raw_data JSONB;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS last_synced_at TIMESTAMP WITH TIME ZONE;

-- Add comments for documentation
COMMENT ON COLUMN jc_trips.total_seats IS 'Total seats in the bus (calculated: booked + available + blocked)';
COMMENT ON COLUMN jc_trips.booked_seats IS 'Number of seats booked (from passenger_details count)';
COMMENT ON COLUMN jc_trips.available_seats IS 'Number of seats still available';
COMMENT ON COLUMN jc_trips.blocked_seats IS 'Number of blocked seats';
COMMENT ON COLUMN jc_trips.phone_booking_count IS 'Number of phone bookings';
COMMENT ON COLUMN jc_trips.blocked_seat_numbers IS 'Comma-separated list of blocked seat numbers';
COMMENT ON COLUMN jc_trips.phone_booking_seat_numbers IS 'Comma-separated list of phone booked seat numbers';
COMMENT ON COLUMN jc_trips.coach_num IS 'Bus registration number e.g. NL-01-B-3463';
COMMENT ON COLUMN jc_trips.coach_id IS 'Internal coach/bus ID from Bitla';
COMMENT ON COLUMN jc_trips.captain1_name IS 'Primary driver name';
COMMENT ON COLUMN jc_trips.captain1_phone IS 'Primary driver phone';
COMMENT ON COLUMN jc_trips.captain2_name IS 'Secondary driver name';
COMMENT ON COLUMN jc_trips.captain2_phone IS 'Secondary driver phone';
COMMENT ON COLUMN jc_trips.attendant_name IS 'Bus attendant name';
COMMENT ON COLUMN jc_trips.attendant_phone IS 'Bus attendant phone';
COMMENT ON COLUMN jc_trips.route_duration IS 'Duration of trip e.g. 10:00';
COMMENT ON COLUMN jc_trips.chart_operated_by IS 'Array of who operated the chart e.g. ["Conductor"]';
COMMENT ON COLUMN jc_trips.pickup_van_details IS 'JSON array of pickup van details';
COMMENT ON COLUMN jc_trips.raw_data IS 'Complete raw JSON from Bitla API';
COMMENT ON COLUMN jc_trips.last_synced_at IS 'Last time this record was synced from Bitla';

-- =====================================================
-- VERIFY COLUMNS EXIST
-- =====================================================
-- Run this to verify all columns are present:
-- SELECT column_name, data_type, column_default
-- FROM information_schema.columns
-- WHERE table_name = 'jc_trips'
-- AND column_name IN ('total_seats', 'booked_seats', 'available_seats', 'blocked_seats', 'phone_booking_count')
-- ORDER BY column_name;
