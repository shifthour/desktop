-- ═══════════════════════════════════════════════════════════════════════════════
-- JC COMMUNICATION PLATFORM - DATABASE SCHEMA
-- For Supabase PostgreSQL
-- All tables prefixed with jc_
-- ═══════════════════════════════════════════════════════════════════════════════

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 1: MASTER TABLES
-- ═══════════════════════════════════════════════════════════════════════════════

-- Operators (Bus Companies)
CREATE TABLE jc_operators (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    gds_operator_id VARCHAR(50),
    operator_name VARCHAR(255) NOT NULL,
    operator_code VARCHAR(50) UNIQUE NOT NULL,
    api_base_url VARCHAR(500),
    api_key_encrypted TEXT,
    api_username VARCHAR(255),
    logo_url TEXT,
    contact_email VARCHAR(255),
    contact_phone VARCHAR(20),
    address TEXT,
    settings JSONB DEFAULT '{}',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Routes
CREATE TABLE jc_routes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    operator_id UUID REFERENCES jc_operators(id) ON DELETE CASCADE,
    bitla_route_id INTEGER,
    service_number VARCHAR(100),
    route_name VARCHAR(255),
    origin VARCHAR(255) NOT NULL,
    destination VARCHAR(255) NOT NULL,
    origin_code VARCHAR(50),
    destination_code VARCHAR(50),
    coach_type VARCHAR(100),
    distance_km DECIMAL(10, 2),
    duration_hours DECIMAL(5, 2),
    base_fare DECIMAL(10, 2),
    departure_time TIME,
    arrival_time TIME,
    days_of_operation VARCHAR(50) DEFAULT 'daily',
    amenities JSONB DEFAULT '[]',
    multistation_fares JSONB DEFAULT '{}',
    status VARCHAR(20) DEFAULT 'active',
    last_synced_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(operator_id, bitla_route_id)
);

-- Coach Types
CREATE TABLE jc_coach_types (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    operator_id UUID REFERENCES jc_operators(id) ON DELETE CASCADE,
    bitla_coach_type_id INTEGER,
    coach_type_name VARCHAR(255) NOT NULL,
    total_seats INTEGER,
    is_ac BOOLEAN DEFAULT false,
    is_sleeper BOOLEAN DEFAULT false,
    bus_type VARCHAR(100),
    amenities JSONB DEFAULT '[]',
    seat_layout JSONB,
    image_url TEXT,
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Coaches (Vehicles)
CREATE TABLE jc_coaches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    operator_id UUID REFERENCES jc_operators(id) ON DELETE CASCADE,
    coach_type_id UUID REFERENCES jc_coach_types(id),
    vehicle_number VARCHAR(50) NOT NULL,
    gps_id VARCHAR(100),
    gps_vendor VARCHAR(100),
    chassis_number VARCHAR(100),
    engine_number VARCHAR(100),
    manufacture_year INTEGER,
    last_service_date DATE,
    insurance_expiry DATE,
    fitness_expiry DATE,
    is_operational BOOLEAN DEFAULT true,
    current_location JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(operator_id, vehicle_number)
);

-- Stages (Boarding/Drop Points)
CREATE TABLE jc_stages (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    operator_id UUID REFERENCES jc_operators(id) ON DELETE CASCADE,
    bitla_stage_id INTEGER,
    stage_name VARCHAR(255) NOT NULL,
    stage_code VARCHAR(50),
    stage_type VARCHAR(20) DEFAULT 'both', -- 'boarding', 'dropping', 'both'
    address TEXT,
    landmark TEXT,
    city VARCHAR(100),
    state VARCHAR(100),
    pincode VARCHAR(10),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    contact_name VARCHAR(255),
    contact_phone VARCHAR(20),
    google_maps_link TEXT,
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Agents (Booking Agents/OTAs)
CREATE TABLE jc_agents (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    operator_id UUID REFERENCES jc_operators(id) ON DELETE CASCADE,
    bitla_agent_id INTEGER,
    agent_name VARCHAR(255) NOT NULL,
    agent_code VARCHAR(50),
    agent_type VARCHAR(50), -- 'ota', 'branch', 'franchise', 'api'
    login_id VARCHAR(100),
    commission_percent DECIMAL(5, 2),
    commission_type VARCHAR(20),
    credit_limit DECIMAL(12, 2),
    current_balance DECIMAL(12, 2),
    contact_email VARCHAR(255),
    contact_phone VARCHAR(20),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 2: TRANSACTIONAL TABLES
-- ═══════════════════════════════════════════════════════════════════════════════

-- Trips (Service instances for a specific date)
CREATE TABLE jc_trips (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    route_id UUID REFERENCES jc_routes(id) ON DELETE CASCADE,
    coach_id UUID REFERENCES jc_coaches(id),
    travel_date DATE NOT NULL,
    reservation_id INTEGER,
    service_number VARCHAR(100),
    bus_type VARCHAR(255),
    departure_time TIME,
    arrival_time TIME,
    departure_datetime TIMESTAMP WITH TIME ZONE,
    arrival_datetime TIMESTAMP WITH TIME ZONE,
    total_seats INTEGER,
    available_seats INTEGER,
    booked_seats INTEGER DEFAULT 0,
    blocked_seats INTEGER DEFAULT 0,
    captain1_name VARCHAR(255),
    captain1_phone VARCHAR(20),
    captain2_name VARCHAR(255),
    captain2_phone VARCHAR(20),
    conductor_name VARCHAR(255),
    conductor_phone VARCHAR(20),
    helpline_number VARCHAR(20),
    trip_status VARCHAR(30) DEFAULT 'scheduled', -- 'scheduled', 'departed', 'in_transit', 'completed', 'cancelled'
    delay_minutes INTEGER DEFAULT 0,
    current_location JSONB,
    raw_data JSONB,
    last_synced_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(route_id, travel_date, reservation_id)
);

-- Bookings
CREATE TABLE jc_bookings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    trip_id UUID REFERENCES jc_trips(id) ON DELETE CASCADE,
    operator_id UUID REFERENCES jc_operators(id),
    ticket_number VARCHAR(100) NOT NULL,
    travel_operator_pnr VARCHAR(100) UNIQUE NOT NULL,
    service_number VARCHAR(100),
    travel_date DATE NOT NULL,
    origin VARCHAR(255),
    destination VARCHAR(255),
    boarding_point_id UUID REFERENCES jc_stages(id),
    dropoff_point_id UUID REFERENCES jc_stages(id),
    boarding_point_name VARCHAR(255),
    boarding_address TEXT,
    boarding_landmark TEXT,
    boarding_time TIME,
    boarding_datetime TIMESTAMP WITH TIME ZONE,
    boarding_latitude DECIMAL(10, 8),
    boarding_longitude DECIMAL(11, 8),
    dropoff_point_name VARCHAR(255),
    dropoff_address TEXT,
    dropoff_time TIME,
    dropoff_datetime TIMESTAMP WITH TIME ZONE,
    dropoff_latitude DECIMAL(10, 8),
    dropoff_longitude DECIMAL(11, 8),
    total_seats INTEGER DEFAULT 1,
    total_fare DECIMAL(10, 2),
    base_fare DECIMAL(10, 2),
    service_tax DECIMAL(10, 2),
    cgst DECIMAL(10, 2),
    sgst DECIMAL(10, 2),
    igst DECIMAL(10, 2),
    convenience_fee DECIMAL(10, 2),
    discount DECIMAL(10, 2) DEFAULT 0,
    payment_mode VARCHAR(50),
    payment_status VARCHAR(30) DEFAULT 'paid',
    gst_in VARCHAR(50),
    booked_by VARCHAR(100), -- OTA name
    booked_by_login VARCHAR(100),
    agent_id UUID REFERENCES jc_agents(id),
    booking_date TIMESTAMP WITH TIME ZONE,
    booking_source VARCHAR(50), -- 'webhook', 'api_sync', 'manual'
    booking_status VARCHAR(30) DEFAULT 'confirmed', -- 'confirmed', 'cancelled', 'partially_cancelled', 'completed'
    cancellation_date TIMESTAMP WITH TIME ZONE,
    cancellation_reason TEXT,
    cancellation_charge DECIMAL(10, 2),
    refund_amount DECIMAL(10, 2),
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Passengers
CREATE TABLE jc_passengers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    booking_id UUID REFERENCES jc_bookings(id) ON DELETE CASCADE,
    trip_id UUID REFERENCES jc_trips(id),
    pnr_number VARCHAR(50) NOT NULL,
    title VARCHAR(10),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    full_name VARCHAR(255) NOT NULL,
    age INTEGER,
    gender VARCHAR(10),
    mobile VARCHAR(20) NOT NULL,
    email VARCHAR(255),
    id_proof_type VARCHAR(50),
    id_proof_number VARCHAR(100),
    seat_number VARCHAR(20) NOT NULL,
    seat_type VARCHAR(50), -- 'sleeper', 'seater', 'semi_sleeper'
    seat_position VARCHAR(20), -- 'lower', 'upper', 'window', 'aisle'
    fare DECIMAL(10, 2),
    base_fare DECIMAL(10, 2),
    cgst DECIMAL(10, 2),
    sgst DECIMAL(10, 2),
    igst DECIMAL(10, 2),
    is_primary BOOLEAN DEFAULT false,
    is_confirmed BOOLEAN DEFAULT true,
    is_boarded BOOLEAN DEFAULT false,
    boarded_at TIMESTAMP WITH TIME ZONE,
    boarding_location JSONB,
    is_dropped BOOLEAN DEFAULT false,
    dropped_at TIMESTAMP WITH TIME ZONE,
    is_cancelled BOOLEAN DEFAULT false,
    cancelled_at TIMESTAMP WITH TIME ZONE,
    cancellation_charge DECIMAL(10, 2),
    refund_amount DECIMAL(10, 2),
    wake_up_call_applicable BOOLEAN DEFAULT false,
    pre_boarding_applicable BOOLEAN DEFAULT false,
    welcome_call_applicable BOOLEAN DEFAULT false,
    is_sms_allowed BOOLEAN DEFAULT true,
    is_whatsapp_allowed BOOLEAN DEFAULT true,
    language_preference VARCHAR(10) DEFAULT 'en',
    special_requirements TEXT,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(booking_id, seat_number)
);

-- Cancellations
CREATE TABLE jc_cancellations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    booking_id UUID REFERENCES jc_bookings(id),
    travel_operator_pnr VARCHAR(100) NOT NULL,
    ticket_number VARCHAR(100),
    cancellation_type VARCHAR(20) DEFAULT 'full', -- 'full', 'partial'
    seat_numbers TEXT[], -- Array of cancelled seat numbers
    passenger_names TEXT[], -- Array of passenger names
    original_fare DECIMAL(10, 2),
    cancellation_charge DECIMAL(10, 2),
    refund_amount DECIMAL(10, 2),
    refund_status VARCHAR(30) DEFAULT 'pending', -- 'pending', 'processed', 'completed', 'failed'
    refund_transaction_id VARCHAR(100),
    cancelled_by VARCHAR(100),
    cancellation_source VARCHAR(50), -- 'customer', 'operator', 'system'
    cancellation_reason TEXT,
    cancelled_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    refunded_at TIMESTAMP WITH TIME ZONE,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Boarding Status
CREATE TABLE jc_boarding_status (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    booking_id UUID REFERENCES jc_bookings(id),
    passenger_id UUID REFERENCES jc_passengers(id),
    ticket_number VARCHAR(100),
    service_number VARCHAR(100),
    seat_number VARCHAR(20),
    boarding_status VARCHAR(30) NOT NULL, -- 'pending', 'boarded', 'not_boarded', 'no_show'
    status_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    updated_by VARCHAR(100), -- 'conductor', 'driver', 'system'
    device_id VARCHAR(100),
    remarks TEXT,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Ticket Shifts
CREATE TABLE jc_ticket_shifts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    booking_id UUID REFERENCES jc_bookings(id),
    travel_operator_pnr VARCHAR(100) NOT NULL,
    old_trip_id UUID REFERENCES jc_trips(id),
    new_trip_id UUID REFERENCES jc_trips(id),
    old_service_number VARCHAR(100),
    new_service_number VARCHAR(100),
    old_travel_date DATE,
    new_travel_date DATE,
    old_seat_numbers TEXT[],
    new_seat_numbers TEXT[],
    shift_charge DECIMAL(10, 2) DEFAULT 0,
    fare_difference DECIMAL(10, 2) DEFAULT 0,
    shifted_by VARCHAR(100),
    shift_reason TEXT,
    shifted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Ticket Updates
CREATE TABLE jc_ticket_updates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    booking_id UUID REFERENCES jc_bookings(id),
    travel_operator_pnr VARCHAR(100) NOT NULL,
    update_type VARCHAR(50) NOT NULL, -- 'boarding_point', 'dropoff_point', 'passenger_details', 'contact'
    old_values JSONB,
    new_values JSONB,
    updated_by VARCHAR(100),
    update_reason TEXT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 3: OPERATIONAL TABLES
-- ═══════════════════════════════════════════════════════════════════════════════

-- Vehicle Assignments
CREATE TABLE jc_vehicle_assignments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    trip_id UUID REFERENCES jc_trips(id) ON DELETE CASCADE,
    coach_id UUID REFERENCES jc_coaches(id),
    service_id VARCHAR(100),
    service_number VARCHAR(100),
    travel_date DATE NOT NULL,
    coach_number VARCHAR(50),
    gps_id VARCHAR(100),
    gps_vendor VARCHAR(100),
    assignment_status VARCHAR(30) DEFAULT 'assigned', -- 'assigned', 'in_service', 'completed', 'replaced'
    assigned_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    trip_start_time TIMESTAMP WITH TIME ZONE,
    trip_end_time TIMESTAMP WITH TIME ZONE,
    multistation_fares JSONB,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Crew Members
CREATE TABLE jc_crew_members (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    operator_id UUID REFERENCES jc_operators(id) ON DELETE CASCADE,
    employee_id VARCHAR(50),
    full_name VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL, -- 'driver', 'conductor', 'helper', 'cleaner'
    mobile VARCHAR(20) NOT NULL,
    alternate_mobile VARCHAR(20),
    license_number VARCHAR(50),
    license_expiry DATE,
    address TEXT,
    emergency_contact VARCHAR(20),
    date_of_joining DATE,
    is_active BOOLEAN DEFAULT true,
    profile_photo_url TEXT,
    documents JSONB DEFAULT '[]',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Trip Crew Assignments
CREATE TABLE jc_trip_crew (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    trip_id UUID REFERENCES jc_trips(id) ON DELETE CASCADE,
    vehicle_assignment_id UUID REFERENCES jc_vehicle_assignments(id),
    crew_member_id UUID REFERENCES jc_crew_members(id),
    role VARCHAR(50) NOT NULL, -- 'driver1', 'driver2', 'conductor', 'helper'
    assigned_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    duty_start TIMESTAMP WITH TIME ZONE,
    duty_end TIMESTAMP WITH TIME ZONE,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(trip_id, crew_member_id, role)
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 4: COMMUNICATION TABLES
-- ═══════════════════════════════════════════════════════════════════════════════

-- Message Templates
CREATE TABLE jc_message_templates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    operator_id UUID REFERENCES jc_operators(id),
    template_code VARCHAR(100) NOT NULL,
    template_name VARCHAR(255) NOT NULL,
    notification_type VARCHAR(50) NOT NULL, -- 'booking_confirmation', 'reminder_24h', 'reminder_2h', etc.
    channel VARCHAR(20) NOT NULL, -- 'whatsapp', 'sms', 'email'
    language VARCHAR(10) NOT NULL DEFAULT 'en',
    subject VARCHAR(255), -- For email
    body TEXT NOT NULL,
    variables JSONB DEFAULT '[]', -- List of dynamic variables
    whatsapp_template_name VARCHAR(255),
    whatsapp_namespace VARCHAR(255),
    sms_dlt_template_id VARCHAR(100),
    is_active BOOLEAN DEFAULT true,
    version INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(operator_id, template_code, channel, language)
);

-- Notifications
CREATE TABLE jc_notifications (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    passenger_id UUID REFERENCES jc_passengers(id),
    booking_id UUID REFERENCES jc_bookings(id),
    trip_id UUID REFERENCES jc_trips(id),
    operator_id UUID REFERENCES jc_operators(id),
    notification_type VARCHAR(50) NOT NULL,
    template_id UUID REFERENCES jc_message_templates(id),
    recipient_mobile VARCHAR(20) NOT NULL,
    recipient_email VARCHAR(255),
    recipient_name VARCHAR(255),
    channel VARCHAR(20) NOT NULL, -- 'whatsapp', 'sms', 'email'
    priority INTEGER DEFAULT 5, -- 1 = highest
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL,
    sent_at TIMESTAMP WITH TIME ZONE,
    delivered_at TIMESTAMP WITH TIME ZONE,
    read_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'queued', 'sent', 'delivered', 'read', 'failed', 'cancelled'
    message_content TEXT,
    message_variables JSONB DEFAULT '{}',
    external_message_id VARCHAR(255), -- ID from WhatsApp/SMS provider
    provider VARCHAR(50), -- 'gupshup', 'wati', 'msg91', 'sendgrid'
    provider_response JSONB,
    error_message TEXT,
    error_code VARCHAR(50),
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Notification Logs (Detailed audit trail)
CREATE TABLE jc_notification_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    notification_id UUID REFERENCES jc_notifications(id) ON DELETE CASCADE,
    event_type VARCHAR(50) NOT NULL, -- 'created', 'queued', 'sent', 'delivered', 'read', 'failed', 'retry'
    event_data JSONB,
    provider_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Communication Queue (For scheduled messages)
CREATE TABLE jc_communication_queue (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    notification_id UUID REFERENCES jc_notifications(id),
    passenger_id UUID REFERENCES jc_passengers(id),
    booking_id UUID REFERENCES jc_bookings(id),
    trip_id UUID REFERENCES jc_trips(id),
    message_type VARCHAR(50) NOT NULL,
    channel VARCHAR(20) NOT NULL,
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL,
    priority INTEGER DEFAULT 5,
    status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'processing', 'completed', 'failed', 'cancelled'
    attempts INTEGER DEFAULT 0,
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    next_attempt_at TIMESTAMP WITH TIME ZONE,
    locked_until TIMESTAMP WITH TIME ZONE,
    locked_by VARCHAR(100),
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Channel Preferences (User opt-in/opt-out)
CREATE TABLE jc_channel_preferences (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    mobile VARCHAR(20) NOT NULL UNIQUE,
    email VARCHAR(255),
    whatsapp_opted_in BOOLEAN DEFAULT true,
    sms_opted_in BOOLEAN DEFAULT true,
    email_opted_in BOOLEAN DEFAULT true,
    language_preference VARCHAR(10) DEFAULT 'en',
    quiet_hours_start TIME,
    quiet_hours_end TIME,
    timezone VARCHAR(50) DEFAULT 'Asia/Kolkata',
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 5: WEBHOOK & SYNC TABLES
-- ═══════════════════════════════════════════════════════════════════════════════

-- Raw Webhook Events (Audit trail for all incoming webhooks)
CREATE TABLE jc_webhook_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    operator_id UUID REFERENCES jc_operators(id),
    event_type VARCHAR(50) NOT NULL, -- 'booking', 'cancellation', 'boarding_status', 'vehicle_assign', 'service_update'
    endpoint VARCHAR(100) NOT NULL,
    method VARCHAR(10) DEFAULT 'POST',
    headers JSONB,
    payload JSONB NOT NULL,
    source_ip VARCHAR(50),
    signature VARCHAR(255),
    signature_valid BOOLEAN,
    processed BOOLEAN DEFAULT false,
    processed_at TIMESTAMP WITH TIME ZONE,
    processing_error TEXT,
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Sync Jobs (Track data synchronization)
CREATE TABLE jc_sync_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    operator_id UUID REFERENCES jc_operators(id),
    sync_type VARCHAR(50) NOT NULL, -- 'full', 'incremental', 'routes', 'passengers', 'master_data'
    target_date DATE,
    status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'running', 'completed', 'failed'
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    records_fetched INTEGER DEFAULT 0,
    records_created INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    details JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- API Request Logs
CREATE TABLE jc_api_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    operator_id UUID REFERENCES jc_operators(id),
    direction VARCHAR(10) NOT NULL, -- 'inbound', 'outbound'
    endpoint VARCHAR(255) NOT NULL,
    method VARCHAR(10) NOT NULL,
    request_headers JSONB,
    request_body JSONB,
    response_status INTEGER,
    response_body JSONB,
    latency_ms INTEGER,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 6: ANALYTICS & REPORTING TABLES
-- ═══════════════════════════════════════════════════════════════════════════════

-- Daily Statistics
CREATE TABLE jc_daily_stats (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    operator_id UUID REFERENCES jc_operators(id),
    stat_date DATE NOT NULL,
    total_bookings INTEGER DEFAULT 0,
    total_passengers INTEGER DEFAULT 0,
    total_cancellations INTEGER DEFAULT 0,
    total_revenue DECIMAL(12, 2) DEFAULT 0,
    total_refunds DECIMAL(12, 2) DEFAULT 0,
    notifications_sent INTEGER DEFAULT 0,
    notifications_delivered INTEGER DEFAULT 0,
    notifications_failed INTEGER DEFAULT 0,
    whatsapp_sent INTEGER DEFAULT 0,
    sms_sent INTEGER DEFAULT 0,
    trips_completed INTEGER DEFAULT 0,
    avg_occupancy_percent DECIMAL(5, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(operator_id, stat_date)
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 7: INDEXES FOR PERFORMANCE
-- ═══════════════════════════════════════════════════════════════════════════════

-- Operators
CREATE INDEX idx_jc_operators_code ON jc_operators(operator_code);
CREATE INDEX idx_jc_operators_active ON jc_operators(is_active);

-- Routes
CREATE INDEX idx_jc_routes_operator ON jc_routes(operator_id);
CREATE INDEX idx_jc_routes_bitla ON jc_routes(bitla_route_id);
CREATE INDEX idx_jc_routes_origin_dest ON jc_routes(origin, destination);
CREATE INDEX idx_jc_routes_status ON jc_routes(status);

-- Trips
CREATE INDEX idx_jc_trips_route ON jc_trips(route_id);
CREATE INDEX idx_jc_trips_date ON jc_trips(travel_date);
CREATE INDEX idx_jc_trips_departure ON jc_trips(departure_datetime);
CREATE INDEX idx_jc_trips_status ON jc_trips(trip_status);
CREATE INDEX idx_jc_trips_date_status ON jc_trips(travel_date, trip_status);

-- Bookings
CREATE INDEX idx_jc_bookings_trip ON jc_bookings(trip_id);
CREATE INDEX idx_jc_bookings_operator ON jc_bookings(operator_id);
CREATE INDEX idx_jc_bookings_pnr ON jc_bookings(travel_operator_pnr);
CREATE INDEX idx_jc_bookings_ticket ON jc_bookings(ticket_number);
CREATE INDEX idx_jc_bookings_date ON jc_bookings(travel_date);
CREATE INDEX idx_jc_bookings_status ON jc_bookings(booking_status);
CREATE INDEX idx_jc_bookings_created ON jc_bookings(created_at DESC);

-- Passengers
CREATE INDEX idx_jc_passengers_booking ON jc_passengers(booking_id);
CREATE INDEX idx_jc_passengers_trip ON jc_passengers(trip_id);
CREATE INDEX idx_jc_passengers_pnr ON jc_passengers(pnr_number);
CREATE INDEX idx_jc_passengers_mobile ON jc_passengers(mobile);
CREATE INDEX idx_jc_passengers_email ON jc_passengers(email);

-- Notifications
CREATE INDEX idx_jc_notifications_passenger ON jc_notifications(passenger_id);
CREATE INDEX idx_jc_notifications_booking ON jc_notifications(booking_id);
CREATE INDEX idx_jc_notifications_trip ON jc_notifications(trip_id);
CREATE INDEX idx_jc_notifications_scheduled ON jc_notifications(scheduled_at);
CREATE INDEX idx_jc_notifications_status ON jc_notifications(status);
CREATE INDEX idx_jc_notifications_type ON jc_notifications(notification_type);
CREATE INDEX idx_jc_notifications_pending ON jc_notifications(status, scheduled_at) WHERE status = 'pending';
CREATE INDEX idx_jc_notifications_external_id ON jc_notifications(external_message_id) WHERE external_message_id IS NOT NULL;

-- Communication Queue
CREATE INDEX idx_jc_queue_scheduled ON jc_communication_queue(scheduled_at, status) WHERE status = 'pending';
CREATE INDEX idx_jc_queue_locked ON jc_communication_queue(locked_until) WHERE locked_until IS NOT NULL;

-- Webhook Events
CREATE INDEX idx_jc_webhook_events_operator ON jc_webhook_events(operator_id);
CREATE INDEX idx_jc_webhook_events_type ON jc_webhook_events(event_type);
CREATE INDEX idx_jc_webhook_events_processed ON jc_webhook_events(processed, created_at);
CREATE INDEX idx_jc_webhook_events_created ON jc_webhook_events(created_at DESC);

-- Boarding Status
CREATE INDEX idx_jc_boarding_booking ON jc_boarding_status(booking_id);
CREATE INDEX idx_jc_boarding_passenger ON jc_boarding_status(passenger_id);
CREATE INDEX idx_jc_boarding_status ON jc_boarding_status(boarding_status);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 8: FUNCTIONS & TRIGGERS
-- ═══════════════════════════════════════════════════════════════════════════════

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply updated_at trigger to all relevant tables
CREATE TRIGGER update_jc_operators_updated_at BEFORE UPDATE ON jc_operators FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_jc_routes_updated_at BEFORE UPDATE ON jc_routes FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_jc_coach_types_updated_at BEFORE UPDATE ON jc_coach_types FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_jc_coaches_updated_at BEFORE UPDATE ON jc_coaches FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_jc_stages_updated_at BEFORE UPDATE ON jc_stages FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_jc_agents_updated_at BEFORE UPDATE ON jc_agents FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_jc_trips_updated_at BEFORE UPDATE ON jc_trips FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_jc_bookings_updated_at BEFORE UPDATE ON jc_bookings FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_jc_passengers_updated_at BEFORE UPDATE ON jc_passengers FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_jc_cancellations_updated_at BEFORE UPDATE ON jc_cancellations FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_jc_vehicle_assignments_updated_at BEFORE UPDATE ON jc_vehicle_assignments FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_jc_crew_members_updated_at BEFORE UPDATE ON jc_crew_members FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_jc_message_templates_updated_at BEFORE UPDATE ON jc_message_templates FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_jc_notifications_updated_at BEFORE UPDATE ON jc_notifications FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_jc_communication_queue_updated_at BEFORE UPDATE ON jc_communication_queue FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_jc_channel_preferences_updated_at BEFORE UPDATE ON jc_channel_preferences FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_jc_daily_stats_updated_at BEFORE UPDATE ON jc_daily_stats FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Function to auto-create daily stats record
CREATE OR REPLACE FUNCTION ensure_daily_stats()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO jc_daily_stats (operator_id, stat_date)
    VALUES (NEW.operator_id, CURRENT_DATE)
    ON CONFLICT (operator_id, stat_date) DO NOTHING;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 9: ROW LEVEL SECURITY (RLS) FOR SUPABASE
-- ═══════════════════════════════════════════════════════════════════════════════

-- Enable RLS on all tables
ALTER TABLE jc_operators ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_routes ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_coach_types ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_coaches ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_stages ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_agents ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_trips ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_bookings ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_passengers ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_cancellations ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_boarding_status ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_ticket_shifts ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_ticket_updates ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_vehicle_assignments ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_crew_members ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_trip_crew ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_message_templates ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_notifications ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_notification_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_communication_queue ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_channel_preferences ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_webhook_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_sync_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_api_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_daily_stats ENABLE ROW LEVEL SECURITY;

-- Create policies for service role (full access)
CREATE POLICY "Service role has full access to jc_operators" ON jc_operators FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_routes" ON jc_routes FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_coach_types" ON jc_coach_types FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_coaches" ON jc_coaches FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_stages" ON jc_stages FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_agents" ON jc_agents FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_trips" ON jc_trips FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_bookings" ON jc_bookings FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_passengers" ON jc_passengers FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_cancellations" ON jc_cancellations FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_boarding_status" ON jc_boarding_status FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_ticket_shifts" ON jc_ticket_shifts FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_ticket_updates" ON jc_ticket_updates FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_vehicle_assignments" ON jc_vehicle_assignments FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_crew_members" ON jc_crew_members FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_trip_crew" ON jc_trip_crew FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_message_templates" ON jc_message_templates FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_notifications" ON jc_notifications FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_notification_logs" ON jc_notification_logs FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_communication_queue" ON jc_communication_queue FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_channel_preferences" ON jc_channel_preferences FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_webhook_events" ON jc_webhook_events FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_sync_jobs" ON jc_sync_jobs FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_api_logs" ON jc_api_logs FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role has full access to jc_daily_stats" ON jc_daily_stats FOR ALL USING (auth.role() = 'service_role');

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 10: SEED DATA
-- ═══════════════════════════════════════════════════════════════════════════════

-- Insert default operator (Mythri Travels for pilot)
INSERT INTO jc_operators (
    operator_name,
    operator_code,
    api_base_url,
    contact_email,
    contact_phone,
    settings,
    is_active
) VALUES (
    'Mythri Travels',
    'mythri',
    'http://myth.mythribus.com/api/',
    'support@mythritravels.com',
    '7095666619',
    '{
        "default_language": "te",
        "notification_channels": ["whatsapp", "sms"],
        "reminder_24h_enabled": true,
        "reminder_2h_enabled": true,
        "wakeup_enabled": true,
        "whatsapp_template_namespace": "mythri_travels"
    }'::jsonb,
    true
);

-- Insert default message templates
INSERT INTO jc_message_templates (template_code, template_name, notification_type, channel, language, body, variables) VALUES
('BOOKING_CONFIRM_EN', 'Booking Confirmation - English', 'booking_confirmation', 'whatsapp', 'en',
'🎫 *Booking Confirmed!*

Dear {{passenger_name}},

Your bus ticket is confirmed:
📍 Route: {{origin}} → {{destination}}
📅 Date: {{travel_date}}
🕐 Departure: {{departure_time}}
💺 Seat: {{seat_number}}
🎫 PNR: {{pnr_number}}

*Boarding Point:* {{boarding_point}}
📍 Address: {{boarding_address}}
🗺️ Location: {{google_maps_link}}

Helpline: {{helpline_number}}

Thank you for choosing Mythri Travels!',
'["passenger_name", "origin", "destination", "travel_date", "departure_time", "seat_number", "pnr_number", "boarding_point", "boarding_address", "google_maps_link", "helpline_number"]'::jsonb),

('BOOKING_CONFIRM_TE', 'Booking Confirmation - Telugu', 'booking_confirmation', 'whatsapp', 'te',
'🎫 *బుకింగ్ నిర్ధారించబడింది!*

ప్రియమైన {{passenger_name}},

మీ బస్ టిక్కెట్ నిర్ధారించబడింది:
📍 రూట్: {{origin}} → {{destination}}
📅 తేదీ: {{travel_date}}
🕐 బయలుదేరు: {{departure_time}}
💺 సీట్: {{seat_number}}
🎫 PNR: {{pnr_number}}

*ఎక్కే స్థలం:* {{boarding_point}}
📍 చిరునామా: {{boarding_address}}

సహాయం: {{helpline_number}}',
'["passenger_name", "origin", "destination", "travel_date", "departure_time", "seat_number", "pnr_number", "boarding_point", "boarding_address", "helpline_number"]'::jsonb),

('REMINDER_24H_EN', '24 Hour Reminder - English', 'reminder_24h', 'whatsapp', 'en',
'⏰ *Trip Reminder - Tomorrow!*

Dear {{passenger_name}},

Your trip is tomorrow:
📍 {{origin}} → {{destination}}
📅 {{travel_date}} at {{departure_time}}
💺 Seat: {{seat_number}}

*Boarding:* {{boarding_point}}
⏰ Time: {{boarding_time}}
📍 Location: {{google_maps_link}}

Safe travels!
Mythri Travels',
'["passenger_name", "origin", "destination", "travel_date", "departure_time", "seat_number", "boarding_point", "boarding_time", "google_maps_link"]'::jsonb),

('REMINDER_2H_EN', '2 Hour Reminder - English', 'reminder_2h', 'whatsapp', 'en',
'🚌 *Your Bus is Arriving Soon!*

Dear {{passenger_name}},

Please reach your boarding point:

📍 {{boarding_point}}
⏰ Time: {{boarding_time}}
🚌 Bus: {{coach_number}}
👨‍✈️ Driver: {{driver_name}} ({{driver_phone}})

📍 Navigate: {{google_maps_link}}

Show this message to the conductor.
PNR: {{pnr_number}} | Seat: {{seat_number}}',
'["passenger_name", "boarding_point", "boarding_time", "coach_number", "driver_name", "driver_phone", "google_maps_link", "pnr_number", "seat_number"]'::jsonb),

('CANCELLATION_EN', 'Cancellation Acknowledgment - English', 'cancellation_ack', 'whatsapp', 'en',
'❌ *Booking Cancelled*

Dear {{passenger_name}},

Your booking has been cancelled:
🎫 PNR: {{pnr_number}}
📍 Route: {{origin}} → {{destination}}
📅 Date: {{travel_date}}

Cancellation Charge: ₹{{cancellation_charge}}
Refund Amount: ₹{{refund_amount}}

Refund will be processed within 5-7 business days.

For queries: {{helpline_number}}',
'["passenger_name", "pnr_number", "origin", "destination", "travel_date", "cancellation_charge", "refund_amount", "helpline_number"]'::jsonb),

('DELAY_ALERT_EN', 'Delay Alert - English', 'delay_alert', 'whatsapp', 'en',
'⚠️ *Trip Delay Alert*

Dear {{passenger_name}},

Your bus is delayed:
📍 Route: {{origin}} → {{destination}}

Original Time: {{original_time}}
New Time: {{new_time}}
Delay: {{delay_minutes}} minutes

{{#if reason}}Reason: {{reason}}{{/if}}

We apologize for the inconvenience.
Helpline: {{helpline_number}}',
'["passenger_name", "origin", "destination", "original_time", "new_time", "delay_minutes", "reason", "helpline_number"]'::jsonb),

('WAKEUP_EN', 'Wake-up Call - English', 'wakeup_call', 'whatsapp', 'en',
'☀️ *Wake Up! Arriving Soon*

Dear {{passenger_name}},

Your destination is approaching!

📍 Arriving at: {{destination}}
⏰ Expected: {{arrival_time}}

Please prepare to alight.

Thank you for traveling with Mythri Travels!',
'["passenger_name", "destination", "arrival_time"]'::jsonb);

-- ═══════════════════════════════════════════════════════════════════════════════
-- DONE! Schema creation complete.
-- ═══════════════════════════════════════════════════════════════════════════════
