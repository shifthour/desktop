-- ================================================
-- FLATRIX REAL ESTATE CRM - COMPLETE DATABASE SETUP
-- ================================================
-- IMPORTANT: flatrix_leads table already exists and is not created here
-- Run this entire script in your Supabase SQL Editor

-- ================================================
-- PART 1: ENABLE EXTENSIONS AND CREATE TYPES
-- ================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Drop existing types if they exist
DROP TYPE IF EXISTS user_role CASCADE;
DROP TYPE IF EXISTS lead_status CASCADE;
DROP TYPE IF EXISTS property_type CASCADE;
DROP TYPE IF EXISTS property_status CASCADE;
DROP TYPE IF EXISTS commission_status CASCADE;
DROP TYPE IF EXISTS task_priority CASCADE;
DROP TYPE IF EXISTS task_status CASCADE;
DROP TYPE IF EXISTS deal_status CASCADE;

-- Create ENUM types
CREATE TYPE user_role AS ENUM ('ADMIN', 'CHANNEL_PARTNER', 'SALES_MANAGER', 'AGENT');
CREATE TYPE lead_status AS ENUM ('NEW', 'CONTACTED', 'QUALIFIED', 'NEGOTIATION', 'CONVERTED', 'LOST');
CREATE TYPE property_type AS ENUM ('APARTMENT', 'VILLA', 'PLOT', 'COMMERCIAL', 'OFFICE_SPACE');
CREATE TYPE property_status AS ENUM ('AVAILABLE', 'BOOKED', 'SOLD', 'HOLD');
CREATE TYPE commission_status AS ENUM ('PENDING', 'APPROVED', 'PAID', 'CANCELLED');
CREATE TYPE task_priority AS ENUM ('LOW', 'MEDIUM', 'HIGH', 'URGENT');
CREATE TYPE task_status AS ENUM ('PENDING', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED');
CREATE TYPE deal_status AS ENUM ('ACTIVE', 'WON', 'LOST', 'ON_HOLD');

-- ================================================
-- PART 2: CREATE TABLES
-- ================================================

-- Create Users table
CREATE TABLE IF NOT EXISTS flatrix_users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    phone VARCHAR(20),
    role user_role DEFAULT 'AGENT',
    is_active BOOLEAN DEFAULT true,
    channel_partner_id UUID,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create Channel Partners table
CREATE TABLE IF NOT EXISTS flatrix_channel_partners (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_name VARCHAR(255) NOT NULL,
    registration_no VARCHAR(100) UNIQUE,
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(100),
    pincode VARCHAR(10),
    website VARCHAR(255),
    gst_number VARCHAR(50),
    pan_number VARCHAR(50),
    commission_rate DECIMAL(5,2) DEFAULT 2.5,
    total_earnings DECIMAL(15,2) DEFAULT 0,
    pending_payments DECIMAL(15,2) DEFAULT 0,
    is_active BOOLEAN DEFAULT true,
    manager_id UUID REFERENCES flatrix_users(id),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Add foreign key for channel_partner_id in users table
ALTER TABLE flatrix_users 
DROP CONSTRAINT IF EXISTS fk_user_channel_partner,
ADD CONSTRAINT fk_user_channel_partner 
FOREIGN KEY (channel_partner_id) REFERENCES flatrix_channel_partners(id);

-- Create Projects table
CREATE TABLE IF NOT EXISTS flatrix_projects (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    developer_name VARCHAR(255) NOT NULL,
    location TEXT NOT NULL,
    city VARCHAR(100) NOT NULL,
    total_units INTEGER NOT NULL,
    available_units INTEGER NOT NULL,
    price_range VARCHAR(100),
    description TEXT,
    amenities TEXT[],
    brochure_url TEXT,
    launch_date DATE,
    completion_date DATE,
    rera_number VARCHAR(100),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create Project Partners junction table
CREATE TABLE IF NOT EXISTS flatrix_project_partners (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID NOT NULL REFERENCES flatrix_projects(id) ON DELETE CASCADE,
    partner_id UUID NOT NULL REFERENCES flatrix_channel_partners(id) ON DELETE CASCADE,
    commission_rate DECIMAL(5,2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(project_id, partner_id)
);

-- Create Properties table
CREATE TABLE IF NOT EXISTS flatrix_properties (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    unit_number VARCHAR(50) NOT NULL,
    type property_type NOT NULL,
    status property_status DEFAULT 'AVAILABLE',
    floor INTEGER,
    area DECIMAL(10,2) NOT NULL,
    price DECIMAL(15,2) NOT NULL,
    facing VARCHAR(50),
    bedrooms INTEGER,
    bathrooms INTEGER,
    parking BOOLEAN DEFAULT false,
    furnishing VARCHAR(50),
    project_id UUID NOT NULL REFERENCES flatrix_projects(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(project_id, unit_number)
);

-- Create sequence for deal numbers
CREATE SEQUENCE IF NOT EXISTS deal_number_seq START 1;

-- Create Deals table
CREATE TABLE IF NOT EXISTS flatrix_deals (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    deal_number VARCHAR(50) UNIQUE DEFAULT ('DL-' || TO_CHAR(NOW(), 'YYYY') || '-' || LPAD(NEXTVAL('deal_number_seq')::TEXT, 6, '0')),
    lead_id UUID NOT NULL,
    property_id UUID NOT NULL REFERENCES flatrix_properties(id),
    user_id UUID NOT NULL REFERENCES flatrix_users(id),
    channel_partner_id UUID REFERENCES flatrix_channel_partners(id),
    deal_value DECIMAL(15,2) NOT NULL,
    status deal_status DEFAULT 'ACTIVE',
    booking_date TIMESTAMPTZ DEFAULT NOW(),
    closing_date TIMESTAMPTZ,
    notes TEXT,
    documents TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create Commissions table
CREATE TABLE IF NOT EXISTS flatrix_commissions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    deal_id UUID UNIQUE NOT NULL REFERENCES flatrix_deals(id) ON DELETE CASCADE,
    channel_partner_id UUID NOT NULL REFERENCES flatrix_channel_partners(id),
    amount DECIMAL(15,2) NOT NULL,
    percentage DECIMAL(5,2) NOT NULL,
    status commission_status DEFAULT 'PENDING',
    approved_date TIMESTAMPTZ,
    paid_date TIMESTAMPTZ,
    payment_method VARCHAR(50),
    transaction_ref VARCHAR(100),
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create Activities table
CREATE TABLE IF NOT EXISTS flatrix_activities (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    type VARCHAR(50) NOT NULL,
    description TEXT NOT NULL,
    lead_id UUID,
    user_id UUID NOT NULL REFERENCES flatrix_users(id),
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create Tasks table
CREATE TABLE IF NOT EXISTS flatrix_tasks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    title VARCHAR(255) NOT NULL,
    description TEXT,
    due_date TIMESTAMPTZ,
    priority task_priority DEFAULT 'MEDIUM',
    status task_status DEFAULT 'PENDING',
    lead_id UUID,
    assigned_to_id UUID NOT NULL REFERENCES flatrix_users(id),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

-- ================================================
-- PART 3: UPDATE EXISTING LEADS TABLE
-- ================================================

-- Add columns to existing flatrix_leads table if they don't exist
ALTER TABLE flatrix_leads 
ADD COLUMN IF NOT EXISTS assigned_to_id UUID REFERENCES flatrix_users(id),
ADD COLUMN IF NOT EXISTS created_by_id UUID REFERENCES flatrix_users(id),
ADD COLUMN IF NOT EXISTS channel_partner_id UUID REFERENCES flatrix_channel_partners(id),
ADD COLUMN IF NOT EXISTS status lead_status DEFAULT 'NEW',
ADD COLUMN IF NOT EXISTS budget_min DECIMAL(15,2),
ADD COLUMN IF NOT EXISTS budget_max DECIMAL(15,2),
ADD COLUMN IF NOT EXISTS interested_in property_type[],
ADD COLUMN IF NOT EXISTS preferred_location TEXT,
ADD COLUMN IF NOT EXISTS source VARCHAR(100),
ADD COLUMN IF NOT EXISTS notes TEXT,
ADD COLUMN IF NOT EXISTS last_contacted_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

-- Add foreign key references for flatrix_leads
ALTER TABLE flatrix_deals 
DROP CONSTRAINT IF EXISTS flatrix_deals_lead_id_fkey,
ADD CONSTRAINT flatrix_deals_lead_id_fkey 
FOREIGN KEY (lead_id) REFERENCES flatrix_leads(id);

ALTER TABLE flatrix_activities
DROP CONSTRAINT IF EXISTS flatrix_activities_lead_id_fkey,
ADD CONSTRAINT flatrix_activities_lead_id_fkey 
FOREIGN KEY (lead_id) REFERENCES flatrix_leads(id);

ALTER TABLE flatrix_tasks
DROP CONSTRAINT IF EXISTS flatrix_tasks_lead_id_fkey,
ADD CONSTRAINT flatrix_tasks_lead_id_fkey 
FOREIGN KEY (lead_id) REFERENCES flatrix_leads(id);

-- ================================================
-- PART 4: CREATE INDEXES
-- ================================================

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_flatrix_users_email ON flatrix_users(email);
CREATE INDEX IF NOT EXISTS idx_flatrix_users_channel_partner ON flatrix_users(channel_partner_id);
CREATE INDEX IF NOT EXISTS idx_flatrix_channel_partners_active ON flatrix_channel_partners(is_active);
CREATE INDEX IF NOT EXISTS idx_flatrix_properties_project ON flatrix_properties(project_id);
CREATE INDEX IF NOT EXISTS idx_flatrix_properties_status ON flatrix_properties(status);
CREATE INDEX IF NOT EXISTS idx_flatrix_deals_lead ON flatrix_deals(lead_id);
CREATE INDEX IF NOT EXISTS idx_flatrix_deals_property ON flatrix_deals(property_id);
CREATE INDEX IF NOT EXISTS idx_flatrix_deals_status ON flatrix_deals(status);
CREATE INDEX IF NOT EXISTS idx_flatrix_commissions_partner ON flatrix_commissions(channel_partner_id);
CREATE INDEX IF NOT EXISTS idx_flatrix_commissions_status ON flatrix_commissions(status);
CREATE INDEX IF NOT EXISTS idx_flatrix_activities_lead ON flatrix_activities(lead_id);
CREATE INDEX IF NOT EXISTS idx_flatrix_activities_user ON flatrix_activities(user_id);
CREATE INDEX IF NOT EXISTS idx_flatrix_tasks_assigned ON flatrix_tasks(assigned_to_id);
CREATE INDEX IF NOT EXISTS idx_flatrix_tasks_status ON flatrix_tasks(status);
CREATE INDEX IF NOT EXISTS idx_flatrix_leads_assigned_to ON flatrix_leads(assigned_to_id);
CREATE INDEX IF NOT EXISTS idx_flatrix_leads_created_by ON flatrix_leads(created_by_id);
CREATE INDEX IF NOT EXISTS idx_flatrix_leads_channel_partner ON flatrix_leads(channel_partner_id);
CREATE INDEX IF NOT EXISTS idx_flatrix_leads_status ON flatrix_leads(status);

-- ================================================
-- PART 5: CREATE TRIGGERS
-- ================================================

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Add updated_at triggers to all tables
DROP TRIGGER IF EXISTS update_flatrix_users_updated_at ON flatrix_users;
CREATE TRIGGER update_flatrix_users_updated_at BEFORE UPDATE ON flatrix_users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_flatrix_channel_partners_updated_at ON flatrix_channel_partners;
CREATE TRIGGER update_flatrix_channel_partners_updated_at BEFORE UPDATE ON flatrix_channel_partners
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_flatrix_projects_updated_at ON flatrix_projects;
CREATE TRIGGER update_flatrix_projects_updated_at BEFORE UPDATE ON flatrix_projects
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_flatrix_properties_updated_at ON flatrix_properties;
CREATE TRIGGER update_flatrix_properties_updated_at BEFORE UPDATE ON flatrix_properties
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_flatrix_deals_updated_at ON flatrix_deals;
CREATE TRIGGER update_flatrix_deals_updated_at BEFORE UPDATE ON flatrix_deals
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_flatrix_commissions_updated_at ON flatrix_commissions;
CREATE TRIGGER update_flatrix_commissions_updated_at BEFORE UPDATE ON flatrix_commissions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_flatrix_tasks_updated_at ON flatrix_tasks;
CREATE TRIGGER update_flatrix_tasks_updated_at BEFORE UPDATE ON flatrix_tasks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_flatrix_leads_updated_at ON flatrix_leads;
CREATE TRIGGER update_flatrix_leads_updated_at BEFORE UPDATE ON flatrix_leads
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ================================================
-- PART 6: ENABLE ROW LEVEL SECURITY
-- ================================================

-- Enable Row Level Security on all tables
ALTER TABLE flatrix_users ENABLE ROW LEVEL SECURITY;
ALTER TABLE flatrix_channel_partners ENABLE ROW LEVEL SECURITY;
ALTER TABLE flatrix_projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE flatrix_project_partners ENABLE ROW LEVEL SECURITY;
ALTER TABLE flatrix_properties ENABLE ROW LEVEL SECURITY;
ALTER TABLE flatrix_leads ENABLE ROW LEVEL SECURITY;
ALTER TABLE flatrix_deals ENABLE ROW LEVEL SECURITY;
ALTER TABLE flatrix_commissions ENABLE ROW LEVEL SECURITY;
ALTER TABLE flatrix_activities ENABLE ROW LEVEL SECURITY;
ALTER TABLE flatrix_tasks ENABLE ROW LEVEL SECURITY;

-- Create basic policies for development (adjust for production)
-- Allow all operations for authenticated users (for development)
CREATE POLICY "Allow all for authenticated users" ON flatrix_users FOR ALL USING (true);
CREATE POLICY "Allow all for authenticated users" ON flatrix_channel_partners FOR ALL USING (true);
CREATE POLICY "Allow all for authenticated users" ON flatrix_projects FOR ALL USING (true);
CREATE POLICY "Allow all for authenticated users" ON flatrix_project_partners FOR ALL USING (true);
CREATE POLICY "Allow all for authenticated users" ON flatrix_properties FOR ALL USING (true);
CREATE POLICY "Allow all for authenticated users" ON flatrix_leads FOR ALL USING (true);
CREATE POLICY "Allow all for authenticated users" ON flatrix_deals FOR ALL USING (true);
CREATE POLICY "Allow all for authenticated users" ON flatrix_commissions FOR ALL USING (true);
CREATE POLICY "Allow all for authenticated users" ON flatrix_activities FOR ALL USING (true);
CREATE POLICY "Allow all for authenticated users" ON flatrix_tasks FOR ALL USING (true);

-- ================================================
-- PART 7: INSERT SAMPLE DATA
-- ================================================

-- Clear existing data (optional - remove if you want to keep existing data)
-- DELETE FROM flatrix_users WHERE email IN ('admin@flatrix.com', 'manager@flatrix.com', 'agent1@flatrix.com', 'agent2@flatrix.com');

-- Insert sample users
INSERT INTO flatrix_users (email, password, name, role, is_active)
VALUES 
    ('admin@flatrix.com', crypt('admin123', gen_salt('bf')), 'Admin User', 'ADMIN', true),
    ('manager@flatrix.com', crypt('manager123', gen_salt('bf')), 'Sales Manager', 'SALES_MANAGER', true),
    ('agent1@flatrix.com', crypt('agent123', gen_salt('bf')), 'Amit Singh', 'AGENT', true),
    ('agent2@flatrix.com', crypt('agent123', gen_salt('bf')), 'Priya Patel', 'AGENT', true)
ON CONFLICT (email) DO NOTHING;

-- Insert sample channel partners
INSERT INTO flatrix_channel_partners (company_name, registration_no, address, city, state, pincode, commission_rate, is_active)
VALUES 
    ('Elite Realty Solutions', 'REG2024001', 'MG Road, Block A', 'Bangalore', 'Karnataka', '560001', 2.5, true),
    ('Property Hub Associates', 'REG2024002', 'Koramangala, 5th Block', 'Bangalore', 'Karnataka', '560034', 3.0, true),
    ('Dream Homes Consultancy', 'REG2024003', 'Whitefield Main Road', 'Bangalore', 'Karnataka', '560066', 2.0, true)
ON CONFLICT (registration_no) DO NOTHING;

-- Insert sample projects
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

-- Insert sample properties
WITH project_ids AS (
    SELECT 
        (SELECT id FROM flatrix_projects WHERE name = 'Sunrise Apartments') as sunrise_id,
        (SELECT id FROM flatrix_projects WHERE name = 'Green Valley Villas') as green_valley_id,
        (SELECT id FROM flatrix_projects WHERE name = 'Tech Park Plaza') as tech_park_id
)
INSERT INTO flatrix_properties (unit_number, type, status, floor, area, price, facing, bedrooms, bathrooms, parking, furnishing, project_id)
SELECT 
    unit_number, type::property_type, status::property_status, floor, area, price, facing, bedrooms, bathrooms, parking, furnishing, project_id
FROM (
    VALUES 
        ('A-101', 'APARTMENT', 'AVAILABLE', 1, 1250, 7500000, 'East', 2, 2, true, 'Semi-Furnished', (SELECT sunrise_id FROM project_ids)),
        ('A-102', 'APARTMENT', 'BOOKED', 1, 1250, 7500000, 'West', 2, 2, true, 'Semi-Furnished', (SELECT sunrise_id FROM project_ids)),
        ('A-201', 'APARTMENT', 'AVAILABLE', 2, 1450, 8500000, 'East', 3, 2, true, 'Unfurnished', (SELECT sunrise_id FROM project_ids)),
        ('V-01', 'VILLA', 'AVAILABLE', 0, 2800, 15000000, 'North', 4, 4, true, 'Unfurnished', (SELECT green_valley_id FROM project_ids)),
        ('V-02', 'VILLA', 'BOOKED', 0, 2800, 15000000, 'South', 4, 4, true, 'Unfurnished', (SELECT green_valley_id FROM project_ids)),
        ('T-101', 'APARTMENT', 'AVAILABLE', 1, 950, 5500000, 'East', 2, 2, true, 'Unfurnished', (SELECT tech_park_id FROM project_ids)),
        ('T-301', 'OFFICE_SPACE', 'AVAILABLE', 3, 5000, 35000000, 'North', 0, 4, true, 'Bare Shell', (SELECT tech_park_id FROM project_ids))
) AS properties(unit_number, type, status, floor, area, price, facing, bedrooms, bathrooms, parking, furnishing, project_id)
ON CONFLICT (project_id, unit_number) DO NOTHING;

-- Link channel partners to projects
INSERT INTO flatrix_project_partners (project_id, partner_id, commission_rate)
SELECT 
    p.id as project_id,
    cp.id as partner_id,
    cp.commission_rate
FROM flatrix_projects p
CROSS JOIN flatrix_channel_partners cp
ON CONFLICT (project_id, partner_id) DO NOTHING;

-- ================================================
-- COMPLETION MESSAGE
-- ================================================
-- Database setup complete!
-- Test credentials:
-- Admin: admin@flatrix.com / admin123
-- Manager: manager@flatrix.com / manager123
-- Agent 1: agent1@flatrix.com / agent123
-- Agent 2: agent2@flatrix.com / agent123