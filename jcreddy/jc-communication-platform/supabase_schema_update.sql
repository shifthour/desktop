-- =====================================================
-- SUPABASE SCHEMA UPDATE FOR JC_TRIPS AND JC_PASSENGERS
-- Run this in Supabase SQL Editor
-- =====================================================

-- =====================================================
-- 1. UPDATE JC_TRIPS TABLE (Bus/Trip related data)
-- =====================================================

-- Add new columns to jc_trips if they don't exist
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS coach_num VARCHAR(50);
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS coach_id INTEGER;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS total_seats INTEGER DEFAULT 40;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS booked_seats INTEGER DEFAULT 0;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS blocked_seat_numbers TEXT;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS phone_booking_count INTEGER DEFAULT 0;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS phone_booking_seat_numbers TEXT;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS captain1_name VARCHAR(100);
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS captain1_phone VARCHAR(20);
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS captain2_name VARCHAR(100);
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS captain2_phone VARCHAR(20);
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS attendant_name VARCHAR(100);
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS attendant_phone VARCHAR(20);
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS coach_mobile_number VARCHAR(20);
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS route_duration VARCHAR(10);
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS trip_departure_datetime TIMESTAMP WITH TIME ZONE;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS trip_arrival_datetime TIMESTAMP WITH TIME ZONE;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS coach_updated_time TIMESTAMP WITH TIME ZONE;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS chart_operated_by TEXT[];
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS pickup_van_details JSONB;
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS origin VARCHAR(100);
ALTER TABLE jc_trips ADD COLUMN IF NOT EXISTS destination VARCHAR(100);

-- Update existing columns if needed (make sure they have proper types)
-- ALTER TABLE jc_trips ALTER COLUMN available_seats TYPE INTEGER;
-- ALTER TABLE jc_trips ALTER COLUMN blocked_seats TYPE INTEGER;

-- Add comments for documentation
COMMENT ON COLUMN jc_trips.coach_num IS 'Bus registration number e.g. NL-01-B-3463';
COMMENT ON COLUMN jc_trips.coach_id IS 'Internal coach/bus ID from Bitla';
COMMENT ON COLUMN jc_trips.total_seats IS 'Total seats in the bus (calculated: available + booked + blocked)';
COMMENT ON COLUMN jc_trips.booked_seats IS 'Number of seats booked';
COMMENT ON COLUMN jc_trips.available_seats IS 'Number of seats still available';
COMMENT ON COLUMN jc_trips.blocked_seats IS 'Number of blocked seats';
COMMENT ON COLUMN jc_trips.blocked_seat_numbers IS 'Comma-separated list of blocked seat numbers';
COMMENT ON COLUMN jc_trips.phone_booking_count IS 'Number of phone bookings';
COMMENT ON COLUMN jc_trips.phone_booking_seat_numbers IS 'Comma-separated list of phone booked seat numbers';
COMMENT ON COLUMN jc_trips.captain1_name IS 'Primary driver name';
COMMENT ON COLUMN jc_trips.captain1_phone IS 'Primary driver phone';
COMMENT ON COLUMN jc_trips.captain2_name IS 'Secondary driver name';
COMMENT ON COLUMN jc_trips.captain2_phone IS 'Secondary driver phone';
COMMENT ON COLUMN jc_trips.attendant_name IS 'Bus attendant name';
COMMENT ON COLUMN jc_trips.attendant_phone IS 'Bus attendant phone';
COMMENT ON COLUMN jc_trips.route_duration IS 'Duration of trip e.g. 10:00';
COMMENT ON COLUMN jc_trips.chart_operated_by IS 'Array of who operated the chart e.g. ["Conductor"]';
COMMENT ON COLUMN jc_trips.pickup_van_details IS 'JSON array of pickup van details';

-- =====================================================
-- 2. UPDATE JC_PASSENGERS TABLE
-- =====================================================

-- Add new columns to jc_passengers if they don't exist
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS title VARCHAR(10);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS first_name VARCHAR(100);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS last_name VARCHAR(100);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS age INTEGER;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS gender VARCHAR(20);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS origin VARCHAR(100);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS destination VARCHAR(100);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS booked_by VARCHAR(50);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS booked_by_login VARCHAR(50);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS boarding_at VARCHAR(200);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS boarding_at_id INTEGER;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS boarding_address TEXT;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS boarding_landmark TEXT;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS boarding_datetime TIMESTAMP WITH TIME ZONE;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS drop_off VARCHAR(200);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS dropoff_id INTEGER;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS drop_off_address TEXT;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS drop_off_landmark TEXT;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS base_fare DECIMAL(10,2);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS service_tax DECIMAL(10,2);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS commission_amount DECIMAL(10,2);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS seat_discount DECIMAL(10,2);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS net_amount DECIMAL(10,2);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS transaction_charges DECIMAL(10,2);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS is_primary BOOLEAN DEFAULT FALSE;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS is_confirmed BOOLEAN DEFAULT TRUE;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS is_boarded BOOLEAN DEFAULT FALSE;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS boarded_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS is_cancelled BOOLEAN DEFAULT FALSE;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS is_shifted BOOLEAN DEFAULT FALSE;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS is_phone_blocked BOOLEAN DEFAULT FALSE;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS booking_type_id INTEGER;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS booked_date DATE;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS bp_latitude DECIMAL(10,6);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS bp_longitude DECIMAL(10,6);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS dp_latitude DECIMAL(10,6);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS dp_longitude DECIMAL(10,6);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS id_card_type VARCHAR(50);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS id_card_number VARCHAR(100);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS wake_up_call_applicable BOOLEAN DEFAULT FALSE;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS pre_boarding_applicable BOOLEAN DEFAULT FALSE;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS welcome_call_applicable BOOLEAN DEFAULT FALSE;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS is_trackingo_sms_allowed BOOLEAN DEFAULT TRUE;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS is_coupon_created_ticket BOOLEAN DEFAULT FALSE;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS is_inclusive_service_tax BOOLEAN DEFAULT FALSE;
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS cs_booking_type VARCHAR(50);
ALTER TABLE jc_passengers ADD COLUMN IF NOT EXISTS cs_booked_by VARCHAR(50);

-- Add comments for documentation
COMMENT ON COLUMN jc_passengers.title IS 'Mr, Ms, Miss, etc.';
COMMENT ON COLUMN jc_passengers.booked_by IS 'Booking source: Redbus, Abhi, Paytm, etc.';
COMMENT ON COLUMN jc_passengers.booked_by_login IS 'Login used for booking e.g. myth.redbus';
COMMENT ON COLUMN jc_passengers.boarding_at IS 'Boarding point name with time e.g. Kukatpally - 08:00 PM';
COMMENT ON COLUMN jc_passengers.boarding_at_id IS 'Boarding point ID from Bitla';
COMMENT ON COLUMN jc_passengers.drop_off IS 'Drop off point name with time';
COMMENT ON COLUMN jc_passengers.dropoff_id IS 'Drop off point ID from Bitla';
COMMENT ON COLUMN jc_passengers.is_primary IS 'Is this the primary passenger in group booking';
COMMENT ON COLUMN jc_passengers.is_shifted IS 'Was passenger shifted to different seat/bus';
COMMENT ON COLUMN jc_passengers.bp_latitude IS 'Boarding point GPS latitude';
COMMENT ON COLUMN jc_passengers.bp_longitude IS 'Boarding point GPS longitude';
COMMENT ON COLUMN jc_passengers.dp_latitude IS 'Drop off point GPS latitude';
COMMENT ON COLUMN jc_passengers.dp_longitude IS 'Drop off point GPS longitude';

-- =====================================================
-- 3. CREATE INDEXES FOR BETTER PERFORMANCE
-- =====================================================

-- Trips indexes
CREATE INDEX IF NOT EXISTS idx_jc_trips_travel_date ON jc_trips(travel_date);
CREATE INDEX IF NOT EXISTS idx_jc_trips_route_id ON jc_trips(route_id);
CREATE INDEX IF NOT EXISTS idx_jc_trips_service_number ON jc_trips(service_number);
CREATE INDEX IF NOT EXISTS idx_jc_trips_reservation_id ON jc_trips(reservation_id);

-- Passengers indexes
CREATE INDEX IF NOT EXISTS idx_jc_passengers_pnr ON jc_passengers(pnr_number);
CREATE INDEX IF NOT EXISTS idx_jc_passengers_booking_id ON jc_passengers(booking_id);
CREATE INDEX IF NOT EXISTS idx_jc_passengers_trip_id ON jc_passengers(trip_id);
CREATE INDEX IF NOT EXISTS idx_jc_passengers_mobile ON jc_passengers(mobile);
CREATE INDEX IF NOT EXISTS idx_jc_passengers_seat ON jc_passengers(seat_number);
CREATE INDEX IF NOT EXISTS idx_jc_passengers_booked_by ON jc_passengers(booked_by);

-- =====================================================
-- 4. UPDATE JC_ROUTES TABLE (if needed)
-- =====================================================

ALTER TABLE jc_routes ADD COLUMN IF NOT EXISTS coach_type VARCHAR(100);
ALTER TABLE jc_routes ADD COLUMN IF NOT EXISTS via TEXT;
ALTER TABLE jc_routes ADD COLUMN IF NOT EXISTS operating_days VARCHAR(50);
ALTER TABLE jc_routes ADD COLUMN IF NOT EXISTS distance_km INTEGER;
ALTER TABLE jc_routes ADD COLUMN IF NOT EXISTS base_fare DECIMAL(10,2);

-- =====================================================
-- 5. VERIFY SCHEMA
-- =====================================================

-- Run this to verify the columns exist:
-- SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'jc_trips' ORDER BY ordinal_position;
-- SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'jc_passengers' ORDER BY ordinal_position;
