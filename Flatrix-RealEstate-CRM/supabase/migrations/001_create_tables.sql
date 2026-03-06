-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create ENUM types
CREATE TYPE user_role AS ENUM ('ADMIN', 'CHANNEL_PARTNER', 'SALES_MANAGER', 'AGENT');
CREATE TYPE lead_status AS ENUM ('NEW', 'CONTACTED', 'QUALIFIED', 'NEGOTIATION', 'CONVERTED', 'LOST');
CREATE TYPE property_type AS ENUM ('APARTMENT', 'VILLA', 'PLOT', 'COMMERCIAL', 'OFFICE_SPACE');
CREATE TYPE property_status AS ENUM ('AVAILABLE', 'BOOKED', 'SOLD', 'HOLD');
CREATE TYPE commission_status AS ENUM ('PENDING', 'APPROVED', 'PAID', 'CANCELLED');
CREATE TYPE task_priority AS ENUM ('LOW', 'MEDIUM', 'HIGH', 'URGENT');
CREATE TYPE task_status AS ENUM ('PENDING', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED');
CREATE TYPE deal_status AS ENUM ('ACTIVE', 'WON', 'LOST', 'ON_HOLD');

-- Create Users table
CREATE TABLE flatrix_users (
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
CREATE TABLE flatrix_channel_partners (
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
ADD CONSTRAINT fk_user_channel_partner 
FOREIGN KEY (channel_partner_id) REFERENCES flatrix_channel_partners(id);

-- Create Projects table
CREATE TABLE flatrix_projects (
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
CREATE TABLE flatrix_project_partners (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID NOT NULL REFERENCES flatrix_projects(id) ON DELETE CASCADE,
    partner_id UUID NOT NULL REFERENCES flatrix_channel_partners(id) ON DELETE CASCADE,
    commission_rate DECIMAL(5,2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(project_id, partner_id)
);

-- Create Properties table
CREATE TABLE flatrix_properties (
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

-- Note: flatrix_leads table already exists, so we'll just reference it in other tables

-- Create Deals table
CREATE TABLE flatrix_deals (
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

-- Create sequence for deal numbers
CREATE SEQUENCE deal_number_seq START 1;

-- Create Commissions table
CREATE TABLE flatrix_commissions (
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
CREATE TABLE flatrix_activities (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    type VARCHAR(50) NOT NULL,
    description TEXT NOT NULL,
    lead_id UUID,
    user_id UUID NOT NULL REFERENCES flatrix_users(id),
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create Tasks table
CREATE TABLE flatrix_tasks (
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

-- Create indexes for better query performance
CREATE INDEX idx_flatrix_users_email ON flatrix_users(email);
CREATE INDEX idx_flatrix_users_channel_partner ON flatrix_users(channel_partner_id);
CREATE INDEX idx_flatrix_channel_partners_active ON flatrix_channel_partners(is_active);
CREATE INDEX idx_flatrix_properties_project ON flatrix_properties(project_id);
CREATE INDEX idx_flatrix_properties_status ON flatrix_properties(status);
CREATE INDEX idx_flatrix_deals_lead ON flatrix_deals(lead_id);
CREATE INDEX idx_flatrix_deals_property ON flatrix_deals(property_id);
CREATE INDEX idx_flatrix_deals_status ON flatrix_deals(status);
CREATE INDEX idx_flatrix_commissions_partner ON flatrix_commissions(channel_partner_id);
CREATE INDEX idx_flatrix_commissions_status ON flatrix_commissions(status);
CREATE INDEX idx_flatrix_activities_lead ON flatrix_activities(lead_id);
CREATE INDEX idx_flatrix_activities_user ON flatrix_activities(user_id);
CREATE INDEX idx_flatrix_tasks_assigned ON flatrix_tasks(assigned_to_id);
CREATE INDEX idx_flatrix_tasks_status ON flatrix_tasks(status);

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Add updated_at triggers to all tables
CREATE TRIGGER update_flatrix_users_updated_at BEFORE UPDATE ON flatrix_users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_flatrix_channel_partners_updated_at BEFORE UPDATE ON flatrix_channel_partners
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_flatrix_projects_updated_at BEFORE UPDATE ON flatrix_projects
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_flatrix_properties_updated_at BEFORE UPDATE ON flatrix_properties
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_flatrix_deals_updated_at BEFORE UPDATE ON flatrix_deals
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_flatrix_commissions_updated_at BEFORE UPDATE ON flatrix_commissions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_flatrix_tasks_updated_at BEFORE UPDATE ON flatrix_tasks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();