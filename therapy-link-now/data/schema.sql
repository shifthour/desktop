-- PhysioConnect Supabase Schema
-- Run this in the Supabase SQL Editor BEFORE running seed.sql

-- ============================================================
-- 1. TABLES
-- ============================================================

-- Users
CREATE TABLE physioconnect_users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email TEXT UNIQUE NOT NULL,
  password TEXT NOT NULL,
  full_name TEXT NOT NULL,
  phone TEXT,
  role TEXT DEFAULT 'patient',
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Physiotherapists
CREATE TABLE physioconnect_physiotherapists (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID REFERENCES physioconnect_users(id),
  full_name TEXT NOT NULL,
  email TEXT,
  phone TEXT,
  bio TEXT,
  photo_url TEXT,
  specializations JSONB DEFAULT '[]'::jsonb,
  qualifications JSONB DEFAULT '[]'::jsonb,
  certificate_urls JSONB DEFAULT '[]'::jsonb,
  experience_years INTEGER DEFAULT 0,
  consultation_fee NUMERIC DEFAULT 0,
  visit_type TEXT DEFAULT 'clinic',
  clinic_name TEXT,
  clinic_address TEXT,
  city TEXT,
  session_duration INTEGER DEFAULT 45,
  languages JSONB DEFAULT '["English"]'::jsonb,
  available_days JSONB DEFAULT '[]'::jsonb,
  working_hours_start TEXT DEFAULT '09:00',
  working_hours_end TEXT DEFAULT '17:00',
  status TEXT DEFAULT 'pending',
  rating NUMERIC DEFAULT 0,
  review_count INTEGER DEFAULT 0,
  gender TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Appointments
CREATE TABLE physioconnect_appointments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID REFERENCES physioconnect_users(id),
  physio_id UUID REFERENCES physioconnect_physiotherapists(id),
  physio_name TEXT,
  appointment_date TEXT,
  time_slot TEXT,
  visit_type TEXT,
  consultation_fee NUMERIC,
  patient_name TEXT,
  patient_email TEXT,
  patient_phone TEXT,
  patient_address TEXT,
  notes TEXT,
  status TEXT DEFAULT 'confirmed',
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Reviews
CREATE TABLE physioconnect_reviews (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID REFERENCES physioconnect_users(id),
  physio_id UUID REFERENCES physioconnect_physiotherapists(id),
  patient_name TEXT,
  rating NUMERIC,
  comment TEXT,
  created_date TEXT DEFAULT to_char(now(), 'YYYY-MM-DD')
);

-- Blocked Slots
CREATE TABLE physioconnect_blocked_slots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  physio_id UUID REFERENCES physioconnect_physiotherapists(id),
  date TEXT,
  time_slot TEXT
);

-- Availability Overrides
CREATE TABLE physioconnect_availability_overrides (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  physio_id UUID NOT NULL REFERENCES physioconnect_physiotherapists(id),
  date TEXT NOT NULL,
  is_available BOOLEAN DEFAULT true,
  start_time TEXT,
  end_time TEXT
);

CREATE UNIQUE INDEX idx_override_physio_date
  ON physioconnect_availability_overrides(physio_id, date);

-- ============================================================
-- 2. STORAGE BUCKET
-- ============================================================

INSERT INTO storage.buckets (id, name, public)
VALUES ('physioconnect-uploads', 'physioconnect-uploads', true)
ON CONFLICT (id) DO NOTHING;

-- Allow anyone to upload files to the bucket
CREATE POLICY "Allow public uploads"
  ON storage.objects FOR INSERT
  WITH CHECK (bucket_id = 'physioconnect-uploads');

-- Allow anyone to read files from the bucket
CREATE POLICY "Allow public reads"
  ON storage.objects FOR SELECT
  USING (bucket_id = 'physioconnect-uploads');
