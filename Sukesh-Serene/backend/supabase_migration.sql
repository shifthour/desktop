-- Supabase Migration Script for Serene Living PG Management Portal
-- Execute this script in Supabase SQL Editor

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create serene_users table
CREATE TABLE IF NOT EXISTS serene_users (
  id SERIAL PRIMARY KEY,
  email TEXT UNIQUE NOT NULL,
  password TEXT NOT NULL,
  name TEXT NOT NULL,
  role TEXT DEFAULT 'admin',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create serene_pg_properties table
CREATE TABLE IF NOT EXISTS serene_pg_properties (
  id SERIAL PRIMARY KEY,
  pg_name TEXT NOT NULL,
  address TEXT,
  total_floors INTEGER,
  image_url TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create serene_rooms table
CREATE TABLE IF NOT EXISTS serene_rooms (
  id SERIAL PRIMARY KEY,
  pg_id INTEGER REFERENCES serene_pg_properties(id) ON DELETE SET NULL,
  room_number TEXT NOT NULL,
  floor INTEGER,
  room_type TEXT,
  rent_amount NUMERIC(10,2),
  total_beds INTEGER DEFAULT 1,
  status TEXT DEFAULT 'available',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create serene_tenants table
CREATE TABLE IF NOT EXISTS serene_tenants (
  id SERIAL PRIMARY KEY,
  room_id INTEGER REFERENCES serene_rooms(id) ON DELETE SET NULL,
  name TEXT NOT NULL,
  email TEXT,
  phone TEXT NOT NULL,
  aadhar_number TEXT,
  aadhar_card_image TEXT,
  occupation TEXT,
  emergency_contact TEXT,
  address TEXT,
  check_in_date DATE,
  check_out_date DATE,
  monthly_rent NUMERIC(10,2),
  advance_amount NUMERIC(10,2),
  security_deposit NUMERIC(10,2),
  password TEXT,
  payment_qr_code TEXT,
  first_login INTEGER DEFAULT 1,
  status TEXT DEFAULT 'active',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create serene_payments table
CREATE TABLE IF NOT EXISTS serene_payments (
  id SERIAL PRIMARY KEY,
  tenant_id INTEGER NOT NULL REFERENCES serene_tenants(id) ON DELETE CASCADE,
  payment_type TEXT NOT NULL,
  amount NUMERIC(10,2) NOT NULL,
  payment_date DATE NOT NULL,
  payment_method TEXT,
  month TEXT,
  year INTEGER,
  description TEXT,
  status TEXT DEFAULT 'completed',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create serene_expenses table
CREATE TABLE IF NOT EXISTS serene_expenses (
  id SERIAL PRIMARY KEY,
  pg_id INTEGER REFERENCES serene_pg_properties(id) ON DELETE SET NULL,
  category TEXT NOT NULL,
  description TEXT,
  amount NUMERIC(10,2) NOT NULL,
  expense_date DATE NOT NULL,
  payment_method TEXT,
  bill_image TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create serene_maintenance_requests table
CREATE TABLE IF NOT EXISTS serene_maintenance_requests (
  id SERIAL PRIMARY KEY,
  room_id INTEGER REFERENCES serene_rooms(id) ON DELETE SET NULL,
  tenant_id INTEGER REFERENCES serene_tenants(id) ON DELETE SET NULL,
  title TEXT NOT NULL,
  description TEXT,
  priority TEXT DEFAULT 'medium',
  status TEXT DEFAULT 'pending',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  resolved_at TIMESTAMP
);

-- Create serene_payment_submissions table
CREATE TABLE IF NOT EXISTS serene_payment_submissions (
  id SERIAL PRIMARY KEY,
  tenant_id INTEGER NOT NULL REFERENCES serene_tenants(id) ON DELETE CASCADE,
  month TEXT NOT NULL,
  year INTEGER NOT NULL,
  amount NUMERIC(10,2) NOT NULL,
  payment_screenshot TEXT,
  submission_date DATE NOT NULL,
  status TEXT DEFAULT 'pending',
  reviewed_at TIMESTAMP,
  reviewed_by INTEGER REFERENCES serene_users(id) ON DELETE SET NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_rooms_pg_id ON serene_rooms(pg_id);
CREATE INDEX IF NOT EXISTS idx_tenants_room_id ON serene_tenants(room_id);
CREATE INDEX IF NOT EXISTS idx_tenants_status ON serene_tenants(status);
CREATE INDEX IF NOT EXISTS idx_tenants_email ON serene_tenants(email);
CREATE INDEX IF NOT EXISTS idx_tenants_phone ON serene_tenants(phone);
CREATE INDEX IF NOT EXISTS idx_payments_tenant_id ON serene_payments(tenant_id);
CREATE INDEX IF NOT EXISTS idx_payments_month_year ON serene_payments(month, year);
CREATE INDEX IF NOT EXISTS idx_expenses_pg_id ON serene_expenses(pg_id);
CREATE INDEX IF NOT EXISTS idx_expenses_date ON serene_expenses(expense_date);
CREATE INDEX IF NOT EXISTS idx_payment_submissions_tenant_id ON serene_payment_submissions(tenant_id);
CREATE INDEX IF NOT EXISTS idx_payment_submissions_status ON serene_payment_submissions(status);

-- Insert default admin user (password: admin123)
-- Note: This is the bcrypt hash for 'admin123'
INSERT INTO serene_users (email, password, name, role)
VALUES ('admin@sereneliving.com', '$2a$10$XM0zVm3JHI9db6tG6nVs4.DbjR0oNBVv7.1VkZ4lMmGHzmHzgYvCK', 'Admin User', 'admin')
ON CONFLICT (email) DO NOTHING;

-- Enable Row Level Security (RLS) - Optional but recommended
-- ALTER TABLE serene_users ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE serene_pg_properties ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE serene_rooms ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE serene_tenants ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE serene_payments ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE serene_expenses ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE serene_maintenance_requests ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE serene_payment_submissions ENABLE ROW LEVEL SECURITY;

-- Create RLS Policies (Uncomment if you enable RLS)
-- Example: Allow authenticated users to read all data
-- CREATE POLICY "Allow authenticated read access" ON serene_users FOR SELECT USING (auth.role() = 'authenticated');

COMMENT ON TABLE serene_users IS 'Admin users table';
COMMENT ON TABLE serene_pg_properties IS 'PG properties/locations';
COMMENT ON TABLE serene_rooms IS 'Rooms in each PG property';
COMMENT ON TABLE serene_tenants IS 'Tenant information';
COMMENT ON TABLE serene_payments IS 'Payment records';
COMMENT ON TABLE serene_expenses IS 'Expense records';
COMMENT ON TABLE serene_maintenance_requests IS 'Maintenance requests from tenants';
COMMENT ON TABLE serene_payment_submissions IS 'Tenant-submitted payment proofs for review';
