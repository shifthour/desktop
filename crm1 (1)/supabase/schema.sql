-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Enable Row Level Security
ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

-- Companies table (for client companies)
CREATE TABLE companies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    domain TEXT UNIQUE NOT NULL,
    max_users INTEGER NOT NULL DEFAULT 5,
    current_users INTEGER NOT NULL DEFAULT 0,
    subscription_status TEXT NOT NULL DEFAULT 'active' CHECK (subscription_status IN ('active', 'inactive', 'suspended')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL
);

-- User roles table (predefined roles for instrumental CRM)
CREATE TABLE user_roles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL,
    permissions JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL
);

-- Users table
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
    email TEXT NOT NULL UNIQUE,
    full_name TEXT NOT NULL,
    password TEXT NOT NULL, -- Initial password for admin users
    role_id UUID REFERENCES user_roles(id) ON DELETE RESTRICT,
    is_admin BOOLEAN NOT NULL DEFAULT false,
    is_active BOOLEAN NOT NULL DEFAULT true,
    is_super_admin BOOLEAN NOT NULL DEFAULT false, -- For product owners
    password_changed BOOLEAN NOT NULL DEFAULT false, -- Track if user changed initial password
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    last_login TIMESTAMP WITH TIME ZONE NULL
);

-- Create indexes for better performance
CREATE INDEX idx_users_company_id ON users(company_id);
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_role_id ON users(role_id);

-- Insert predefined roles for instrumental CRM
INSERT INTO user_roles (name, description, permissions) VALUES
('super_admin', 'Product Owner - Full System Access', '["*"]'::jsonb),
('company_admin', 'Company Administrator - Manage Company Users and Settings', '[
    "users.create", "users.read", "users.update", "users.delete",
    "company.read", "company.update",
    "dashboard.read", "reports.read"
]'::jsonb),
('sales_manager', 'Sales Manager - Full Sales Operations', '[
    "leads.create", "leads.read", "leads.update", "leads.delete",
    "accounts.create", "accounts.read", "accounts.update", "accounts.delete",
    "opportunities.create", "opportunities.read", "opportunities.update", "opportunities.delete",
    "quotations.create", "quotations.read", "quotations.update", "quotations.delete",
    "invoices.create", "invoices.read", "invoices.update", "invoices.delete",
    "dashboard.read", "reports.read", "analytics.read"
]'::jsonb),
('sales_representative', 'Sales Representative - Assigned Sales Activities', '[
    "leads.read", "leads.update", "leads.assigned",
    "accounts.read", "accounts.assigned",
    "opportunities.read", "opportunities.update", "opportunities.assigned",
    "quotations.create", "quotations.read", "quotations.update", "quotations.assigned",
    "dashboard.read"
]'::jsonb),
('marketing_manager', 'Marketing Manager - Campaigns and Lead Generation', '[
    "leads.create", "leads.read", "leads.update",
    "campaigns.create", "campaigns.read", "campaigns.update", "campaigns.delete",
    "analytics.read", "reports.read", "dashboard.read"
]'::jsonb),
('field_service_manager', 'Field Service Manager - Installation and Maintenance', '[
    "service_tickets.create", "service_tickets.read", "service_tickets.update", "service_tickets.delete",
    "installations.create", "installations.read", "installations.update", "installations.delete",
    "equipment.read", "equipment.update",
    "technicians.read", "technicians.update",
    "dashboard.read", "reports.read"
]'::jsonb),
('field_service_engineer', 'Field Service Engineer - Assigned Service Tasks', '[
    "service_tickets.read", "service_tickets.update", "service_tickets.assigned",
    "installations.read", "installations.update", "installations.assigned",
    "equipment.read", "dashboard.read"
]'::jsonb),
('customer_support', 'Customer Support - Support Tickets and Communication', '[
    "support_tickets.create", "support_tickets.read", "support_tickets.update",
    "complaints.create", "complaints.read", "complaints.update",
    "accounts.read", "communication.create", "communication.read",
    "dashboard.read"
]'::jsonb),
('finance_manager', 'Finance Manager - Financial Operations', '[
    "invoices.create", "invoices.read", "invoices.update", "invoices.delete",
    "payments.create", "payments.read", "payments.update",
    "financial_reports.read", "financial_reports.create",
    "accounts.read", "dashboard.read"
]'::jsonb),
('inventory_manager', 'Inventory Manager - Product and Stock Management', '[
    "products.create", "products.read", "products.update", "products.delete",
    "inventory.create", "inventory.read", "inventory.update",
    "suppliers.create", "suppliers.read", "suppliers.update",
    "purchase_orders.create", "purchase_orders.read", "purchase_orders.update",
    "dashboard.read", "reports.read"
]'::jsonb),
('viewer', 'Viewer - Read-Only Access to Basic Data', '[
    "dashboard.read", "accounts.read", "leads.read", "opportunities.read"
]'::jsonb);

-- Enable Row Level Security
ALTER TABLE companies ENABLE ROW LEVEL SECURITY;
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_roles ENABLE ROW LEVEL SECURITY;

-- RLS Policies for companies
CREATE POLICY "Super admins can access all companies" ON companies
    FOR ALL USING (
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_super_admin = true
        )
    );

CREATE POLICY "Company admins can access their company" ON companies
    FOR SELECT USING (
        id IN (
            SELECT company_id FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_admin = true
        )
    );

-- RLS Policies for users
CREATE POLICY "Super admins can access all users" ON users
    FOR ALL USING (
        EXISTS (
            SELECT 1 FROM users u
            WHERE u.id = auth.uid() 
            AND u.is_super_admin = true
        )
    );

CREATE POLICY "Company admins can access users in their company" ON users
    FOR ALL USING (
        company_id IN (
            SELECT company_id FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_admin = true
        )
    );

CREATE POLICY "Users can access their own record" ON users
    FOR SELECT USING (id = auth.uid());

-- RLS Policies for user_roles
CREATE POLICY "All authenticated users can read roles" ON user_roles
    FOR SELECT USING (auth.uid() IS NOT NULL);

-- Function to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = TIMEZONE('utc'::text, NOW());
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at
CREATE TRIGGER update_companies_updated_at BEFORE UPDATE ON companies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Function to update company user count
CREATE OR REPLACE FUNCTION update_company_user_count()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE companies 
        SET current_users = current_users + 1 
        WHERE id = NEW.company_id;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE companies 
        SET current_users = current_users - 1 
        WHERE id = OLD.company_id;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ language 'plpgsql';

-- Create trigger for user count
CREATE TRIGGER update_user_count AFTER INSERT OR DELETE ON users
    FOR EACH ROW EXECUTE FUNCTION update_company_user_count();

-- Insert Super Admin User for Product Owner
-- Email: superadmin@labgig.com
-- Password: LabGig@2025!
INSERT INTO users (
    email,
    full_name,
    password,
    role_id,
    is_admin,
    is_super_admin,
    is_active,
    password_changed
) VALUES (
    'superadmin@labgig.com',
    'LabGig Super Admin',
    'LabGig@2025!',
    (SELECT id FROM user_roles WHERE name = 'super_admin'),
    true,
    true,
    true,
    true
);

-- Notifications table
CREATE TABLE notifications (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE, -- NULL for system-wide notifications
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE, -- NULL for super admin notifications
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'info' CHECK (type IN ('info', 'success', 'warning', 'error')),
    entity_type TEXT, -- 'company', 'user', 'system'
    entity_id UUID, -- Reference to the related entity
    is_read BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL
);

-- Create indexes for notifications
CREATE INDEX idx_notifications_user_id ON notifications(user_id);
CREATE INDEX idx_notifications_company_id ON notifications(company_id);
CREATE INDEX idx_notifications_created_at ON notifications(created_at DESC);
CREATE INDEX idx_notifications_unread ON notifications(user_id, is_read, created_at DESC);

-- Enable RLS for notifications
ALTER TABLE notifications ENABLE ROW LEVEL SECURITY;

-- RLS Policies for notifications
CREATE POLICY "Super admins can access all notifications" ON notifications
    FOR ALL USING (
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_super_admin = true
        )
    );

CREATE POLICY "Users can access their own notifications" ON notifications
    FOR SELECT USING (user_id = auth.uid());

CREATE POLICY "Company admins can access company notifications" ON notifications
    FOR SELECT USING (
        company_id IN (
            SELECT company_id FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_admin = true
        )
    );

-- Trigger for updated_at
CREATE TRIGGER update_notifications_updated_at BEFORE UPDATE ON notifications
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();