-- Setup demo company and user in production Supabase
-- Run this after creating the accounts table

-- First check if companies table exists, if not create it
CREATE TABLE IF NOT EXISTS companies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    domain TEXT,
    industry TEXT,
    size TEXT,
    website TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    postal_code TEXT,
    phone TEXT,
    email TEXT,
    logo_url TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Check if users table exists (should already exist from auth)
-- We'll just add company_id and other fields if they don't exist
DO $$ 
BEGIN
    -- Add company_id column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'users' 
                   AND column_name = 'company_id') THEN
        ALTER TABLE users ADD COLUMN company_id UUID REFERENCES companies(id);
    END IF;
    
    -- Add is_admin column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'users' 
                   AND column_name = 'is_admin') THEN
        ALTER TABLE users ADD COLUMN is_admin BOOLEAN DEFAULT false;
    END IF;
    
    -- Add full_name column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'users' 
                   AND column_name = 'full_name') THEN
        ALTER TABLE users ADD COLUMN full_name TEXT;
    END IF;
    
    -- Add password column if it doesn't exist (for demo purposes)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'users' 
                   AND column_name = 'password') THEN
        ALTER TABLE users ADD COLUMN password TEXT;
    END IF;
END $$;

-- Insert demo company (if not exists)
INSERT INTO companies (
    id,
    name,
    domain,
    industry,
    website,
    city,
    country,
    is_active
) VALUES (
    '22adbd06-8ce1-49ea-9a03-d0b46720c624',
    'LabGig Demo Company',
    'labgig-demo',
    'Technology',
    'https://labgig.com',
    'Mumbai',
    'India',
    true
) ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    updated_at = NOW();

-- Insert demo admin user (if not exists)
INSERT INTO users (
    id,
    email,
    full_name,
    company_id,
    is_admin,
    password
) VALUES (
    '45b84401-ca13-4627-ac8f-42a11374633c',
    'demo-admin@labgig.com',
    'Demo Administrator',
    '22adbd06-8ce1-49ea-9a03-d0b46720c624',
    true,
    'Admin@123'
) ON CONFLICT (id) DO UPDATE SET
    full_name = EXCLUDED.full_name,
    company_id = EXCLUDED.company_id,
    is_admin = EXCLUDED.is_admin,
    password = EXCLUDED.password;

-- Verify setup
SELECT 'Demo setup complete!' as message;
SELECT 
    u.email,
    u.full_name,
    c.name as company_name,
    u.is_admin
FROM users u
JOIN companies c ON u.company_id = c.id
WHERE u.email = 'demo-admin@labgig.com';