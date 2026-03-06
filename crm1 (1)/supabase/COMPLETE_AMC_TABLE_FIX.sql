-- =====================================================
-- COMPLETE AMC_CONTRACTS TABLE FIX
-- =====================================================
-- This script adds all missing columns that the AMC form uses
-- =====================================================

-- First, make installation_id nullable
ALTER TABLE amc_contracts
ALTER COLUMN installation_id DROP NOT NULL;

-- Customer Information columns
ALTER TABLE amc_contracts
ADD COLUMN IF NOT EXISTS customer_name VARCHAR(255),
ADD COLUMN IF NOT EXISTS contact_person VARCHAR(255),
ADD COLUMN IF NOT EXISTS customer_phone VARCHAR(50),
ADD COLUMN IF NOT EXISTS customer_email VARCHAR(255),
ADD COLUMN IF NOT EXISTS service_address TEXT,
ADD COLUMN IF NOT EXISTS city VARCHAR(100),
ADD COLUMN IF NOT EXISTS state VARCHAR(100),
ADD COLUMN IF NOT EXISTS pincode VARCHAR(20);

-- Equipment Information
ALTER TABLE amc_contracts
ADD COLUMN IF NOT EXISTS equipment_details JSONB DEFAULT '[]'::jsonb;

-- Contract Details
ALTER TABLE amc_contracts
ADD COLUMN IF NOT EXISTS contract_type VARCHAR(50) DEFAULT 'comprehensive',
ADD COLUMN IF NOT EXISTS contract_value DECIMAL(15,2),
ADD COLUMN IF NOT EXISTS payment_terms TEXT;

-- Contract Period
ALTER TABLE amc_contracts
ADD COLUMN IF NOT EXISTS start_date DATE,
ADD COLUMN IF NOT EXISTS end_date DATE,
ADD COLUMN IF NOT EXISTS duration_months INTEGER DEFAULT 12;

-- Service Details
ALTER TABLE amc_contracts
ADD COLUMN IF NOT EXISTS service_frequency VARCHAR(50) DEFAULT 'quarterly',
ADD COLUMN IF NOT EXISTS number_of_services INTEGER DEFAULT 4,
ADD COLUMN IF NOT EXISTS services_completed INTEGER DEFAULT 0;

-- Coverage Details
ALTER TABLE amc_contracts
ADD COLUMN IF NOT EXISTS labour_charges_included BOOLEAN DEFAULT true,
ADD COLUMN IF NOT EXISTS spare_parts_included BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS emergency_support BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS response_time_hours INTEGER DEFAULT 24;

-- Status
ALTER TABLE amc_contracts
ADD COLUMN IF NOT EXISTS status VARCHAR(50) DEFAULT 'Active';

-- Assignment
ALTER TABLE amc_contracts
ADD COLUMN IF NOT EXISTS assigned_technician VARCHAR(255),
ADD COLUMN IF NOT EXISTS service_manager VARCHAR(255);

-- Additional Information
ALTER TABLE amc_contracts
ADD COLUMN IF NOT EXISTS terms_and_conditions TEXT,
ADD COLUMN IF NOT EXISTS exclusions TEXT,
ADD COLUMN IF NOT EXISTS special_instructions TEXT;

-- Auto-renewal
ALTER TABLE amc_contracts
ADD COLUMN IF NOT EXISTS auto_renewal BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS renewal_notice_days INTEGER DEFAULT 30;

-- System columns (if not already present)
ALTER TABLE amc_contracts
ADD COLUMN IF NOT EXISTS company_id UUID,
ADD COLUMN IF NOT EXISTS created_by UUID,
ADD COLUMN IF NOT EXISTS updated_by UUID,
ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();

-- Verification - Show all columns
SELECT
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'amc_contracts'
ORDER BY ordinal_position;

SELECT '✅ AMC_CONTRACTS TABLE COMPLETELY UPDATED!' AS status;
