-- ================================================
-- PhysioConnect Admin System — Tables + Seed
-- Run this in Supabase SQL Editor
-- ================================================

-- ────────────────────────────────────────────────
-- 1. Admin Users
-- ────────────────────────────────────────────────
CREATE TABLE physioconnect_admin_users (
  id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  username text UNIQUE NOT NULL,
  "passwordHash" text NOT NULL,
  "displayName" text NOT NULL DEFAULT '',
  "createdAt" timestamptz DEFAULT now(),
  "lastLoginAt" timestamptz
);

ALTER TABLE physioconnect_admin_users ENABLE ROW LEVEL SECURITY;
-- No public read policy — accessed only via service role

-- ────────────────────────────────────────────────
-- 2. Admin Sessions
-- ────────────────────────────────────────────────
CREATE TABLE physioconnect_admin_sessions (
  id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "adminUserId" text NOT NULL REFERENCES physioconnect_admin_users(id) ON DELETE CASCADE,
  token text UNIQUE NOT NULL,
  "expiresAt" timestamptz NOT NULL,
  "createdAt" timestamptz DEFAULT now()
);

CREATE INDEX idx_session_token ON physioconnect_admin_sessions (token);
CREATE INDEX idx_session_expires ON physioconnect_admin_sessions ("expiresAt");

ALTER TABLE physioconnect_admin_sessions ENABLE ROW LEVEL SECURITY;
-- No public policy

-- ────────────────────────────────────────────────
-- 3. Applications (physio registration submissions)
-- ────────────────────────────────────────────────
CREATE TABLE physioconnect_applications (
  id text PRIMARY KEY DEFAULT gen_random_uuid()::text,

  -- Personal
  title text,
  gender text,
  "firstName" text NOT NULL,
  "lastName" text NOT NULL,
  email text NOT NULL,
  phone text NOT NULL,
  "dateOfBirth" text,

  -- File storage paths (Supabase Storage)
  "profilePhotoUrl" text,
  "resumeUrl" text,
  "idProofUrl" text,

  -- Professional
  "hcpcNumber" text NOT NULL,
  "professionalBody" text NOT NULL,
  "membershipNumber" text NOT NULL,
  "yearsExperience" text NOT NULL,
  qualifications text,
  specialisations text[] DEFAULT '{}',

  -- Work Preferences
  "serviceRadius" text,
  "homeVisit" boolean DEFAULT false,
  online boolean DEFAULT false,
  "weeklySchedule" jsonb DEFAULT '{}',

  -- About
  bio text,

  -- Right to Work
  "eligibilityType" text,
  "visaDocUrl" text,
  "passportPage1Url" text,
  "passportPage2Url" text,

  -- Consent flags
  "agreeTerms" boolean DEFAULT false,
  "agreePrivacy" boolean DEFAULT false,
  "agreeDBS" boolean DEFAULT false,
  "agreeRightToWork" boolean DEFAULT false,

  -- Review status
  status text DEFAULT 'pending',
  "reviewNotes" text,
  "reviewedBy" text REFERENCES physioconnect_admin_users(id),
  "reviewedAt" timestamptz,

  -- Timestamps
  "createdAt" timestamptz DEFAULT now(),
  "updatedAt" timestamptz DEFAULT now()
);

CREATE INDEX idx_app_status ON physioconnect_applications (status);
CREATE INDEX idx_app_email ON physioconnect_applications (email);
CREATE INDEX idx_app_created ON physioconnect_applications ("createdAt");

ALTER TABLE physioconnect_applications ENABLE ROW LEVEL SECURITY;

-- Public can INSERT (form submissions) but cannot read
CREATE POLICY "Public insert" ON physioconnect_applications FOR INSERT WITH CHECK (true);

-- ────────────────────────────────────────────────
-- 4. Supabase Storage bucket for application files
-- ────────────────────────────────────────────────
INSERT INTO storage.buckets (id, name, public)
VALUES ('applications', 'applications', false)
ON CONFLICT (id) DO NOTHING;

-- Allow anonymous uploads to the applications bucket
CREATE POLICY "Allow public uploads to applications"
ON storage.objects FOR INSERT
WITH CHECK (bucket_id = 'applications');

-- ────────────────────────────────────────────────
-- 5. Seed admin user (admin / PhysioAdmin2026)
-- ────────────────────────────────────────────────
INSERT INTO physioconnect_admin_users (username, "passwordHash", "displayName")
VALUES ('admin', '$2b$10$KDUHo5gWsK51YO6R.fopzO1/taoURGoF6TE.TnGUcOXaLK5YxuB2.', 'Admin');
