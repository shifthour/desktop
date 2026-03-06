-- Seed initial admin user
INSERT INTO flatrix_users (email, password, name, role, is_active)
VALUES 
    ('admin@flatrix.com', crypt('admin123', gen_salt('bf')), 'Admin User', 'ADMIN', true),
    ('manager@flatrix.com', crypt('manager123', gen_salt('bf')), 'Sales Manager', 'SALES_MANAGER', true),
    ('agent1@flatrix.com', crypt('agent123', gen_salt('bf')), 'Amit Singh', 'AGENT', true),
    ('agent2@flatrix.com', crypt('agent123', gen_salt('bf')), 'Priya Patel', 'AGENT', true);

-- Seed channel partners
INSERT INTO flatrix_channel_partners (company_name, registration_no, address, city, state, pincode, commission_rate, is_active)
VALUES 
    ('Elite Realty Solutions', 'REG2024001', 'MG Road, Block A', 'Bangalore', 'Karnataka', '560001', 2.5, true),
    ('Property Hub Associates', 'REG2024002', 'Koramangala, 5th Block', 'Bangalore', 'Karnataka', '560034', 3.0, true),
    ('Dream Homes Consultancy', 'REG2024003', 'Whitefield Main Road', 'Bangalore', 'Karnataka', '560066', 2.0, true);

-- Seed projects
INSERT INTO flatrix_projects (
    name, developer_name, location, city, total_units, available_units, 
    price_range, description, amenities, completion_date, rera_number, is_active
)
VALUES 
    (
        'Sunrise Apartments', 
        'Sunrise Builders', 
        'Whitefield, Near IT Park', 
        'Bangalore', 
        120, 
        45,
        '₹75L - 1.5Cr', 
        'Premium apartments with modern amenities', 
        ARRAY['Swimming Pool', 'Gym', 'Club House', 'Children Play Area', 'Power Backup'],
        '2025-06-30',
        'RERA/KA/2024/001234',
        true
    ),
    (
        'Green Valley Villas', 
        'Green Developers', 
        'Sarjapur Road', 
        'Bangalore', 
        48, 
        12,
        '₹1.5Cr - 3Cr', 
        'Luxury villas in a gated community', 
        ARRAY['Private Garden', 'Swimming Pool', 'Gym', 'Security', 'Power Backup'],
        '2024-12-31',
        'RERA/KA/2024/001235',
        true
    ),
    (
        'Tech Park Plaza', 
        'Tech Builders', 
        'Electronic City Phase 2', 
        'Bangalore', 
        200, 
        85,
        '₹50L - 2Cr', 
        'Commercial and residential complex', 
        ARRAY['Shopping Complex', 'Food Court', 'Gym', 'Conference Rooms', 'Parking'],
        '2025-12-31',
        'RERA/KA/2024/001236',
        true
    );

-- Get project IDs for reference
DO $$
DECLARE
    sunrise_id UUID;
    green_valley_id UUID;
    tech_park_id UUID;
BEGIN
    SELECT id INTO sunrise_id FROM flatrix_projects WHERE name = 'Sunrise Apartments';
    SELECT id INTO green_valley_id FROM flatrix_projects WHERE name = 'Green Valley Villas';
    SELECT id INTO tech_park_id FROM flatrix_projects WHERE name = 'Tech Park Plaza';

    -- Seed properties for Sunrise Apartments
    INSERT INTO flatrix_properties (unit_number, type, status, floor, area, price, facing, bedrooms, bathrooms, parking, furnishing, project_id)
    VALUES 
        ('A-101', 'APARTMENT', 'AVAILABLE', 1, 1250, 7500000, 'East', 2, 2, true, 'Semi-Furnished', sunrise_id),
        ('A-102', 'APARTMENT', 'BOOKED', 1, 1250, 7500000, 'West', 2, 2, true, 'Semi-Furnished', sunrise_id),
        ('A-201', 'APARTMENT', 'AVAILABLE', 2, 1450, 8500000, 'East', 3, 2, true, 'Unfurnished', sunrise_id),
        ('A-301', 'APARTMENT', 'AVAILABLE', 3, 1450, 8500000, 'North', 3, 2, true, 'Unfurnished', sunrise_id),
        ('A-401', 'APARTMENT', 'SOLD', 4, 1850, 12500000, 'East', 3, 3, true, 'Fully Furnished', sunrise_id);

    -- Seed properties for Green Valley Villas
    INSERT INTO flatrix_properties (unit_number, type, status, floor, area, price, facing, bedrooms, bathrooms, parking, furnishing, project_id)
    VALUES 
        ('V-01', 'VILLA', 'AVAILABLE', 0, 2800, 15000000, 'North', 4, 4, true, 'Unfurnished', green_valley_id),
        ('V-02', 'VILLA', 'BOOKED', 0, 2800, 15000000, 'South', 4, 4, true, 'Unfurnished', green_valley_id),
        ('V-03', 'VILLA', 'AVAILABLE', 0, 3200, 18000000, 'East', 4, 5, true, 'Semi-Furnished', green_valley_id),
        ('V-04', 'VILLA', 'AVAILABLE', 0, 3500, 22000000, 'North', 5, 5, true, 'Unfurnished', green_valley_id);

    -- Seed properties for Tech Park Plaza
    INSERT INTO flatrix_properties (unit_number, type, status, floor, area, price, facing, bedrooms, bathrooms, parking, furnishing, project_id)
    VALUES 
        ('T-101', 'APARTMENT', 'AVAILABLE', 1, 950, 5500000, 'East', 2, 2, true, 'Unfurnished', tech_park_id),
        ('T-201', 'APARTMENT', 'AVAILABLE', 2, 1150, 6500000, 'West', 2, 2, true, 'Unfurnished', tech_park_id),
        ('T-301', 'OFFICE_SPACE', 'AVAILABLE', 3, 5000, 35000000, 'North', 0, 4, true, 'Bare Shell', tech_park_id),
        ('T-401', 'OFFICE_SPACE', 'BOOKED', 4, 8000, 55000000, 'South', 0, 6, true, 'Bare Shell', tech_park_id);
END $$;

-- Link channel partners to projects
INSERT INTO flatrix_project_partners (project_id, partner_id, commission_rate)
SELECT 
    p.id as project_id,
    cp.id as partner_id,
    cp.commission_rate
FROM flatrix_projects p
CROSS JOIN flatrix_channel_partners cp;

-- Note: Sample leads should be added after the flatrix_leads table structure is confirmed
-- Since flatrix_leads already exists, we'll just add a comment here
-- Sample leads can be inserted once we know the exact structure of the existing table