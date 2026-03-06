-- ═══════════════════════════════════════════════════════════════════════════════
-- JC COMMUNICATION PLATFORM - DYNAMIC PRICING SCHEMA
-- Extension to 001_schema.sql
-- Tables for demand tracking, pricing rules, and fare recommendations
-- ═══════════════════════════════════════════════════════════════════════════════

-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE 1: DEMAND SNAPSHOTS (Append-only time-series)
-- Periodic snapshots of trip-level demand signals captured by cron job
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE jc_demand_snapshots (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    trip_id UUID REFERENCES jc_trips(id) ON DELETE CASCADE,
    route_id UUID REFERENCES jc_routes(id) ON DELETE SET NULL,
    service_number VARCHAR(100),
    travel_date DATE NOT NULL,
    departure_time TIME,
    snapshot_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    -- Occupancy data (from jc_trips at snapshot time)
    total_seats INTEGER,
    available_seats INTEGER,
    booked_seats INTEGER,
    blocked_seats INTEGER,
    occupancy_percent DECIMAL(5, 2),

    -- Booking velocity (computed from jc_bookings at snapshot time)
    bookings_last_1h INTEGER DEFAULT 0,
    bookings_last_6h INTEGER DEFAULT 0,
    bookings_last_24h INTEGER DEFAULT 0,
    cancellations_last_24h INTEGER DEFAULT 0,

    -- Time signals
    hours_to_departure DECIMAL(8, 2),
    day_of_week INTEGER,              -- 0=Sunday, 6=Saturday
    is_weekend BOOLEAN DEFAULT false,
    is_holiday BOOLEAN DEFAULT false,

    -- Fare signals (from booked passengers)
    avg_fare_booked DECIMAL(10, 2),
    min_fare_booked DECIMAL(10, 2),
    max_fare_booked DECIMAL(10, 2),
    route_base_fare DECIMAL(10, 2),

    -- Revenue snapshot
    total_revenue_so_far DECIMAL(12, 2) DEFAULT 0,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for demand snapshots
CREATE INDEX idx_jc_demand_snapshots_trip ON jc_demand_snapshots(trip_id);
CREATE INDEX idx_jc_demand_snapshots_route ON jc_demand_snapshots(route_id);
CREATE INDEX idx_jc_demand_snapshots_date ON jc_demand_snapshots(travel_date);
CREATE INDEX idx_jc_demand_snapshots_at ON jc_demand_snapshots(snapshot_at DESC);
CREATE INDEX idx_jc_demand_snapshots_trip_at ON jc_demand_snapshots(trip_id, snapshot_at DESC);

-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE 2: PRICING CONFIG (Global key-value configuration)
-- System-wide pricing parameters and feature flags
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE jc_pricing_config (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    config_key VARCHAR(100) UNIQUE NOT NULL,
    config_value JSONB NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE 3: PRICING RULES (Operator-configurable rules engine)
-- Each rule evaluates a condition and produces a fare multiplier
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE jc_pricing_rules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rule_name VARCHAR(255) NOT NULL,
    rule_type VARCHAR(50) NOT NULL,       -- 'occupancy_threshold', 'time_to_departure', 'day_of_week', 'booking_velocity', 'last_seats', 'early_bird'
    description TEXT,
    condition JSONB NOT NULL,              -- e.g. {"field": "occupancy_percent", "operator": "gte", "value": 80}
    multiplier DECIMAL(5, 4) NOT NULL,     -- e.g. 1.1500 for +15%
    priority INTEGER DEFAULT 10,           -- Lower number = higher priority (evaluated first)
    is_active BOOLEAN DEFAULT true,
    applies_to_routes TEXT[] DEFAULT '{}', -- Empty = all routes. Otherwise array of service_numbers
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_jc_pricing_rules_type ON jc_pricing_rules(rule_type);
CREATE INDEX idx_jc_pricing_rules_active ON jc_pricing_rules(is_active) WHERE is_active = true;

-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE 4: PRICING RECOMMENDATIONS (Approval workflow)
-- Generated fare recommendations with operator review workflow
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE jc_pricing_recommendations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    trip_id UUID REFERENCES jc_trips(id) ON DELETE CASCADE,
    route_id UUID REFERENCES jc_routes(id) ON DELETE SET NULL,
    snapshot_id UUID REFERENCES jc_demand_snapshots(id) ON DELETE SET NULL,
    service_number VARCHAR(100),
    travel_date DATE NOT NULL,

    -- Current pricing
    current_base_fare DECIMAL(10, 2) NOT NULL,

    -- Recommended pricing
    recommended_fare DECIMAL(10, 2) NOT NULL,
    fare_change_percent DECIMAL(5, 2) NOT NULL,    -- e.g. +15.00 or -10.00
    fare_multiplier DECIMAL(5, 4) NOT NULL,         -- Combined multiplier from all rules

    -- Demand scoring
    demand_score DECIMAL(5, 2),                     -- 0-100 composite demand score
    occupancy_percent DECIMAL(5, 2),
    hours_to_departure DECIMAL(8, 2),
    booking_velocity_1h INTEGER,

    -- Rules that contributed to this recommendation
    applied_rules JSONB DEFAULT '[]',               -- [{rule_id, rule_name, multiplier}]

    -- Approval workflow
    status VARCHAR(20) DEFAULT 'pending',           -- 'pending', 'approved', 'rejected', 'expired', 'superseded'
    reviewed_by VARCHAR(100),
    reviewed_at TIMESTAMP WITH TIME ZONE,
    review_notes TEXT,

    -- Timing
    generated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE,            -- Auto-expire if not acted on

    -- Revenue impact estimation
    estimated_revenue_impact DECIMAL(12, 2),        -- Positive = additional revenue

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for recommendations
CREATE INDEX idx_jc_pricing_rec_trip ON jc_pricing_recommendations(trip_id);
CREATE INDEX idx_jc_pricing_rec_route ON jc_pricing_recommendations(route_id);
CREATE INDEX idx_jc_pricing_rec_date ON jc_pricing_recommendations(travel_date);
CREATE INDEX idx_jc_pricing_rec_status ON jc_pricing_recommendations(status);
CREATE INDEX idx_jc_pricing_rec_pending ON jc_pricing_recommendations(status, travel_date) WHERE status = 'pending';
CREATE INDEX idx_jc_pricing_rec_generated ON jc_pricing_recommendations(generated_at DESC);

-- ═══════════════════════════════════════════════════════════════════════════════
-- ROW LEVEL SECURITY
-- ═══════════════════════════════════════════════════════════════════════════════

ALTER TABLE jc_demand_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_pricing_config ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_pricing_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_pricing_recommendations ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Service role has full access to jc_demand_snapshots"
    ON jc_demand_snapshots FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Service role has full access to jc_pricing_config"
    ON jc_pricing_config FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Service role has full access to jc_pricing_rules"
    ON jc_pricing_rules FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Service role has full access to jc_pricing_recommendations"
    ON jc_pricing_recommendations FOR ALL USING (auth.role() = 'service_role');

-- ═══════════════════════════════════════════════════════════════════════════════
-- TRIGGERS (auto-update updated_at)
-- Note: jc_demand_snapshots is append-only, no update trigger needed
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TRIGGER update_jc_pricing_config_updated_at
    BEFORE UPDATE ON jc_pricing_config
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_jc_pricing_rules_updated_at
    BEFORE UPDATE ON jc_pricing_rules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_jc_pricing_recommendations_updated_at
    BEFORE UPDATE ON jc_pricing_recommendations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ═══════════════════════════════════════════════════════════════════════════════
-- SEED DATA: Default pricing configuration
-- ═══════════════════════════════════════════════════════════════════════════════

INSERT INTO jc_pricing_config (config_key, config_value, description) VALUES
('global_settings', '{
    "pricing_enabled": true,
    "min_fare_multiplier": 0.80,
    "max_fare_multiplier": 1.40,
    "rule_combination_strategy": "multiplicative",
    "auto_approve": false,
    "snapshot_interval_minutes": 30,
    "recommendation_lookahead_days": 3,
    "min_change_threshold_percent": 2,
    "estimated_conversion_rate": 0.6,
    "demand_score_weights": {
        "occupancy": 0.35,
        "velocity": 0.25,
        "time_to_departure": 0.20,
        "historical": 0.20
    }
}', 'Global pricing system settings - multiplier bounds, rule strategy, and scoring weights'),

('notification_settings', '{
    "notify_on_new_recommendation": true,
    "notify_channel": "dashboard",
    "high_urgency_threshold_hours": 6
}', 'Notification preferences for pricing recommendations');

-- ═══════════════════════════════════════════════════════════════════════════════
-- SEED DATA: Default pricing rules
-- ═══════════════════════════════════════════════════════════════════════════════

INSERT INTO jc_pricing_rules (rule_name, rule_type, description, condition, multiplier, priority) VALUES

-- Occupancy-based rules
('High Occupancy Surge (>80%)',
 'occupancy_threshold',
 'Increase fare by 15% when more than 80% seats are booked',
 '{"field": "occupancy_percent", "operator": "gte", "value": 80}',
 1.1500, 10),

('Medium Occupancy Bump (60-80%)',
 'occupancy_threshold',
 'Increase fare by 8% when 60-80% seats are booked',
 '{"field": "occupancy_percent", "operator": "gte", "value": 60, "max_exclusive": 80}',
 1.0800, 20),

('Low Occupancy Discount (<30%, <48h)',
 'occupancy_threshold',
 'Decrease fare by 10% when occupancy is below 30% and departure is within 48 hours',
 '{"field": "occupancy_percent", "operator": "lt", "value": 30, "additional": {"field": "hours_to_departure", "operator": "lt", "value": 48}}',
 0.9000, 30),

('Very Low Occupancy Discount (<20%, <24h)',
 'occupancy_threshold',
 'Decrease fare by 15% when occupancy is below 20% and departure is within 24 hours',
 '{"field": "occupancy_percent", "operator": "lt", "value": 20, "additional": {"field": "hours_to_departure", "operator": "lt", "value": 24}}',
 0.8500, 25),

-- Day-of-week rule
('Weekend Premium (Fri/Sat/Sun)',
 'day_of_week',
 'Add 10% premium on Friday, Saturday, and Sunday trips',
 '{"field": "day_of_week", "operator": "in", "value": [0, 5, 6]}',
 1.1000, 50),

-- Last seats rule
('Last 5 Seats Premium',
 'last_seats',
 'Add 20% premium when only 5 or fewer seats remain',
 '{"field": "available_seats", "operator": "lte", "value": 5}',
 1.2000, 5),

-- Early bird rule
('Early Bird Discount (>7 days)',
 'early_bird',
 'Offer 5% discount for trips more than 7 days away',
 '{"field": "hours_to_departure", "operator": "gt", "value": 168}',
 0.9500, 60),

-- Velocity rule
('High Booking Velocity Surge',
 'booking_velocity',
 'Add 10% when booking velocity exceeds 5 bookings per hour',
 '{"field": "bookings_last_1h", "operator": "gte", "value": 5}',
 1.1000, 15);
