-- ═══════════════════════════════════════════════════════════════════════════════
-- JC COMMUNICATION PLATFORM - COMPETITIVE PRICING SCHEMA
-- Extension to 002_pricing_schema.sql
-- Tables for competitor fare tracking and seat-level price comparisons
-- ═══════════════════════════════════════════════════════════════════════════════

-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE 1: COMPETITOR FARES (Time-series of scraped fares)
-- Stores raw fare data scraped from both Mythri and Vikram websites
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE jc_competitor_fares (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scrape_batch_id UUID NOT NULL,               -- Groups all fares from one scrape run
    operator VARCHAR(50) NOT NULL,                -- 'mythri' or 'vikram'

    -- Route info
    route_from VARCHAR(100) NOT NULL,             -- City name (normalized)
    route_to VARCHAR(100) NOT NULL,               -- City name (normalized)
    travel_date DATE NOT NULL,

    -- Trip details
    service_number VARCHAR(200),
    bus_type VARCHAR(300),
    departure_time TIMESTAMP WITH TIME ZONE,
    arrival_time TIMESTAMP WITH TIME ZONE,
    trip_identifier VARCHAR(200),                 -- External trip ID (res_id for Mythri, tripCode for Vikram)

    -- Seat counts
    total_seats INTEGER,
    available_seats INTEGER,

    -- Fare summary
    min_fare DECIMAL(10, 2),
    max_fare DECIMAL(10, 2),
    avg_fare DECIMAL(10, 2),

    -- Per-seat-type fares: [{seat_type, seat_name, fare, available_count}]
    fare_by_seat_type JSONB DEFAULT '[]',

    -- Per-seat fares (detailed): [{seat_number, fare, seat_type, is_available, is_window, is_single}]
    seat_fares JSONB DEFAULT '[]',

    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for competitor fares
CREATE INDEX idx_jc_comp_fares_batch ON jc_competitor_fares(scrape_batch_id);
CREATE INDEX idx_jc_comp_fares_operator ON jc_competitor_fares(operator);
CREATE INDEX idx_jc_comp_fares_route ON jc_competitor_fares(route_from, route_to);
CREATE INDEX idx_jc_comp_fares_date ON jc_competitor_fares(travel_date);
CREATE INDEX idx_jc_comp_fares_scraped ON jc_competitor_fares(scraped_at DESC);
CREATE INDEX idx_jc_comp_fares_lookup ON jc_competitor_fares(operator, route_from, route_to, travel_date, scraped_at DESC);

-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE 2: FARE COMPARISONS (Computed comparison results)
-- Matched trips between Mythri and Vikram with undercut recommendations
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE jc_fare_comparisons (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scrape_batch_id UUID NOT NULL,

    -- Route and date
    route_from VARCHAR(100) NOT NULL,
    route_to VARCHAR(100) NOT NULL,
    travel_date DATE NOT NULL,

    -- Mythri trip details
    mythri_fare_id UUID REFERENCES jc_competitor_fares(id) ON DELETE SET NULL,
    mythri_service VARCHAR(200),
    mythri_departure TIMESTAMP WITH TIME ZONE,
    mythri_bus_type VARCHAR(300),
    mythri_min_fare DECIMAL(10, 2),
    mythri_max_fare DECIMAL(10, 2),
    mythri_avg_fare DECIMAL(10, 2),
    mythri_available_seats INTEGER,

    -- Vikram trip details
    vikram_fare_id UUID REFERENCES jc_competitor_fares(id) ON DELETE SET NULL,
    vikram_service VARCHAR(200),
    vikram_departure TIMESTAMP WITH TIME ZONE,
    vikram_bus_type VARCHAR(300),
    vikram_min_fare DECIMAL(10, 2),
    vikram_max_fare DECIMAL(10, 2),
    vikram_avg_fare DECIMAL(10, 2),
    vikram_available_seats INTEGER,

    -- Comparison results
    fare_difference DECIMAL(10, 2),              -- Positive = Mythri is more expensive
    recommended_mythri_fare DECIMAL(10, 2),      -- Vikram fare - undercut_amount
    undercut_amount DECIMAL(10, 2) DEFAULT 30,   -- How much cheaper than Vikram (default ₹30)
    savings_percent DECIMAL(5, 2),               -- How much % cheaper the recommended fare is

    -- Per seat-type comparison: [{seat_type, mythri_fare, vikram_fare, difference, recommended}]
    seat_type_comparison JSONB DEFAULT '[]',

    -- Status
    status VARCHAR(20) DEFAULT 'pending',         -- 'pending', 'applied', 'dismissed', 'expired'
    reviewed_at TIMESTAMP WITH TIME ZONE,
    review_notes TEXT,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for comparisons
CREATE INDEX idx_jc_fare_comp_batch ON jc_fare_comparisons(scrape_batch_id);
CREATE INDEX idx_jc_fare_comp_route ON jc_fare_comparisons(route_from, route_to);
CREATE INDEX idx_jc_fare_comp_date ON jc_fare_comparisons(travel_date);
CREATE INDEX idx_jc_fare_comp_status ON jc_fare_comparisons(status);
CREATE INDEX idx_jc_fare_comp_pending ON jc_fare_comparisons(status, travel_date) WHERE status = 'pending';
CREATE INDEX idx_jc_fare_comp_created ON jc_fare_comparisons(created_at DESC);

-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE 3: COMPETITOR ROUTE MAPPING (Configuration)
-- Maps routes between Mythri and Vikram city IDs/station codes
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE jc_competitor_route_config (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    route_name VARCHAR(200) NOT NULL,             -- Display name: "Hyderabad → Bangalore"
    route_from VARCHAR(100) NOT NULL,             -- Normalized city name
    route_to VARCHAR(100) NOT NULL,               -- Normalized city name

    -- Mythri (TicketSimply) API params
    mythri_from_id VARCHAR(20) NOT NULL,          -- City ID (e.g. "1" for Hyderabad)
    mythri_to_id VARCHAR(20) NOT NULL,            -- City ID (e.g. "89" for Bangalore)

    -- Vikram (EzeeBus) API params
    vikram_from_code VARCHAR(50) NOT NULL,        -- Station code (e.g. "ST16891257D")
    vikram_to_code VARCHAR(50) NOT NULL,          -- Station code (e.g. "STF3OEX206")

    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_jc_comp_route_unique ON jc_competitor_route_config(route_from, route_to);

-- ═══════════════════════════════════════════════════════════════════════════════
-- ROW LEVEL SECURITY
-- ═══════════════════════════════════════════════════════════════════════════════

ALTER TABLE jc_competitor_fares ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_fare_comparisons ENABLE ROW LEVEL SECURITY;
ALTER TABLE jc_competitor_route_config ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Service role has full access to jc_competitor_fares"
    ON jc_competitor_fares FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Service role has full access to jc_fare_comparisons"
    ON jc_fare_comparisons FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Service role has full access to jc_competitor_route_config"
    ON jc_competitor_route_config FOR ALL USING (auth.role() = 'service_role');

-- ═══════════════════════════════════════════════════════════════════════════════
-- TRIGGERS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TRIGGER update_jc_fare_comparisons_updated_at
    BEFORE UPDATE ON jc_fare_comparisons
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_jc_competitor_route_config_updated_at
    BEFORE UPDATE ON jc_competitor_route_config
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ═══════════════════════════════════════════════════════════════════════════════
-- SEED DATA: Route mappings between Mythri and Vikram
-- Major overlapping routes where both operators run buses
-- ═══════════════════════════════════════════════════════════════════════════════

INSERT INTO jc_competitor_route_config (route_name, route_from, route_to, mythri_from_id, mythri_to_id, vikram_from_code, vikram_to_code) VALUES
-- Hyderabad routes
('Hyderabad → Bangalore', 'Hyderabad', 'Bangalore', '1', '89', 'ST16891257D', 'STF3OEX206'),
('Bangalore → Hyderabad', 'Bangalore', 'Hyderabad', '89', '1', 'STF3OEX206', 'ST16891257D'),
('Hyderabad → Vijayawada', 'Hyderabad', 'Vijayawada', '1', '16', 'ST16891257D', 'ST268B1431E'),
('Vijayawada → Hyderabad', 'Vijayawada', 'Hyderabad', '16', '1', 'ST268B1431E', 'ST16891257D'),
('Hyderabad → Pune', 'Hyderabad', 'Pune', '1', '157', 'ST16891257D', 'ST188E44G'),
('Pune → Hyderabad', 'Pune', 'Hyderabad', '157', '1', 'ST188E44G', 'ST16891257D'),
('Hyderabad → Visakhapatnam', 'Hyderabad', 'Visakhapatnam', '1', '115', 'ST16891257D', 'ST18841474F'),
('Visakhapatnam → Hyderabad', 'Visakhapatnam', 'Hyderabad', '115', '1', 'ST18841474F', 'ST16891257D'),
('Hyderabad → Tirupathi', 'Hyderabad', 'Tirupathi', '1', '235', 'ST16891257D', 'STG74D4393'),
('Tirupathi → Hyderabad', 'Tirupathi', 'Hyderabad', '235', '1', 'STG74D4393', 'ST16891257D'),
-- Bangalore routes
('Bangalore → Vijayawada', 'Bangalore', 'Vijayawada', '89', '16', 'STF3OEX206', 'ST268B1431E'),
('Vijayawada → Bangalore', 'Vijayawada', 'Bangalore', '16', '89', 'ST268B1431E', 'STF3OEX206'),
-- Kurnool routes
('Hyderabad → Kurnool', 'Hyderabad', 'Kurnool', '1', '91', 'ST16891257D', 'ST3F8C266I'),
('Kurnool → Hyderabad', 'Kurnool', 'Hyderabad', '91', '1', 'ST3F8C266I', 'ST16891257D');
