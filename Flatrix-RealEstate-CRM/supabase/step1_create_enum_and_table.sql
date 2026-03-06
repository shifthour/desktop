-- Step 1: Create enum and table structure
-- First, check if user_role enum exists and alter it to include super_admin
DO $$ 
BEGIN
    -- Check if the enum type exists
    IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'user_role') THEN
        -- Add super_admin to existing enum if it doesn't exist
        IF NOT EXISTS (SELECT 1 FROM pg_enum WHERE enumlabel = 'super_admin' AND enumtypid = (SELECT oid FROM pg_type WHERE typname = 'user_role')) THEN
            ALTER TYPE user_role ADD VALUE 'super_admin';
        END IF;
    ELSE
        -- Create the enum type if it doesn't exist
        CREATE TYPE user_role AS ENUM ('admin', 'super_admin');
    END IF;
END $$;

-- Create users table for authentication or add missing columns if it exists
CREATE TABLE IF NOT EXISTS flatrix_users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role user_role NOT NULL DEFAULT 'admin',
    full_name VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Add missing columns if table already exists
DO $$ 
BEGIN
    -- Add password_hash column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'flatrix_users' AND column_name = 'password_hash') THEN
        ALTER TABLE flatrix_users ADD COLUMN password_hash VARCHAR(255);
    END IF;
    
    -- Add role column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'flatrix_users' AND column_name = 'role') THEN
        ALTER TABLE flatrix_users ADD COLUMN role user_role DEFAULT 'admin';
    END IF;
    
    -- Add full_name column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'flatrix_users' AND column_name = 'full_name') THEN
        ALTER TABLE flatrix_users ADD COLUMN full_name VARCHAR(255);
    END IF;
    
    -- Add created_at column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'flatrix_users' AND column_name = 'created_at') THEN
        ALTER TABLE flatrix_users ADD COLUMN created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;
    END IF;
    
    -- Add updated_at column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'flatrix_users' AND column_name = 'updated_at') THEN
        ALTER TABLE flatrix_users ADD COLUMN updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;
    END IF;
END $$;

-- Create index on email for faster lookups
CREATE INDEX IF NOT EXISTS idx_flatrix_users_email ON flatrix_users(email);

-- Create index on role for faster filtering
CREATE INDEX IF NOT EXISTS idx_flatrix_users_role ON flatrix_users(role);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
DROP TRIGGER IF EXISTS update_flatrix_users_updated_at ON flatrix_users;
CREATE TRIGGER update_flatrix_users_updated_at 
    BEFORE UPDATE ON flatrix_users 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();