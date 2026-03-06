-- ============================================================
-- ALDRIN CORNERSTONE — Supabase Table Setup
-- Project: Commercial Snow Management & Grounds Services (#3746)
-- Run this in: Supabase Dashboard → SQL Editor
-- ============================================================

-- 1. VISITOR TRACKING
-- Tracks each unique visit/session to the landing page
CREATE TABLE IF NOT EXISTS cornerstone_visits (
  id            BIGSERIAL PRIMARY KEY,
  session_id    UUID NOT NULL,
  referrer      TEXT,
  user_agent    TEXT,
  device_type   VARCHAR(20),        -- 'mobile', 'tablet', 'desktop'
  screen_width  INT,
  screen_height INT,
  utm_source    VARCHAR(100),
  utm_medium    VARCHAR(100),
  utm_campaign  VARCHAR(100),
  duration_seconds INT DEFAULT 0,
  max_scroll_percent INT DEFAULT 0,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- 2. EVENT TRACKING
-- Granular events: page_view, scroll_milestone, section_view, button_click, modal_open, form_submit, etc.
CREATE TABLE IF NOT EXISTS cornerstone_events (
  id            BIGSERIAL PRIMARY KEY,
  session_id    UUID NOT NULL,
  event_type    VARCHAR(50) NOT NULL,   -- 'page_view', 'scroll_milestone', 'section_view', 'button_click', 'modal_open', 'form_submit'
  event_target  TEXT,                   -- what was clicked/viewed (e.g. 'buyer_profile_btn', 'nda_btn', '75%')
  event_data    JSONB DEFAULT '{}',     -- flexible metadata
  section_id    VARCHAR(100),           -- which page section
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- 3. BUYER PROFILES
-- Captures buyer information submitted through the "Complete Buyer Profile" form
CREATE TABLE IF NOT EXISTS cornerstone_buyer_profiles (
  id                BIGSERIAL PRIMARY KEY,
  session_id        UUID,
  full_name         VARCHAR(200) NOT NULL,
  email             VARCHAR(200) NOT NULL,
  phone             VARCHAR(50),
  company           VARCHAR(200),
  title             VARCHAR(200),          -- job title / role
  location          VARCHAR(200),          -- city / province / country
  investment_range  VARCHAR(100),          -- e.g. '$1M-$3M', '$3M-$5M', '$5M+'
  funding_source    VARCHAR(200),          -- e.g. 'Self-funded', 'Private equity', 'SBA loan'
  industry_experience TEXT,                -- relevant background
  acquisition_timeline VARCHAR(100),       -- e.g. 'Immediate', '3-6 months', '6-12 months'
  additional_notes  TEXT,                  -- free-form notes
  status            VARCHAR(20) DEFAULT 'pending',  -- 'pending', 'reviewed', 'approved', 'rejected'
  reviewed_by       VARCHAR(200),
  reviewed_at       TIMESTAMPTZ,
  created_at        TIMESTAMPTZ DEFAULT NOW()
);

-- 4. NDA SUBMISSIONS
-- Captures NDA acceptance/signature
CREATE TABLE IF NOT EXISTS cornerstone_nda_submissions (
  id                BIGSERIAL PRIMARY KEY,
  session_id        UUID,
  full_name         VARCHAR(200) NOT NULL,
  email             VARCHAR(200) NOT NULL,
  phone             VARCHAR(50),
  company           VARCHAR(200),
  signature_text    VARCHAR(200),          -- typed signature
  ip_address        VARCHAR(50),           -- for legal record
  agreed_to_terms   BOOLEAN DEFAULT TRUE,
  nda_version       VARCHAR(20) DEFAULT '1.0',
  status            VARCHAR(20) DEFAULT 'executed',  -- 'executed', 'revoked'
  created_at        TIMESTAMPTZ DEFAULT NOW()
);

-- 5. INDEXES for query performance
CREATE INDEX IF NOT EXISTS idx_cv_session ON cornerstone_visits(session_id);
CREATE INDEX IF NOT EXISTS idx_cv_created ON cornerstone_visits(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ce_session ON cornerstone_events(session_id);
CREATE INDEX IF NOT EXISTS idx_ce_type    ON cornerstone_events(event_type);
CREATE INDEX IF NOT EXISTS idx_ce_created ON cornerstone_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_cbp_email  ON cornerstone_buyer_profiles(email);
CREATE INDEX IF NOT EXISTS idx_cbp_status ON cornerstone_buyer_profiles(status);
CREATE INDEX IF NOT EXISTS idx_cns_email  ON cornerstone_nda_submissions(email);

-- 6. ROW LEVEL SECURITY (RLS)
-- Enable RLS on all tables
ALTER TABLE cornerstone_visits ENABLE ROW LEVEL SECURITY;
ALTER TABLE cornerstone_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE cornerstone_buyer_profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE cornerstone_nda_submissions ENABLE ROW LEVEL SECURITY;

-- Allow anonymous inserts (landing page uses anon key)
CREATE POLICY "Allow anonymous inserts" ON cornerstone_visits
  FOR INSERT TO anon WITH CHECK (true);

CREATE POLICY "Allow anonymous inserts" ON cornerstone_events
  FOR INSERT TO anon WITH CHECK (true);

CREATE POLICY "Allow anonymous inserts" ON cornerstone_buyer_profiles
  FOR INSERT TO anon WITH CHECK (true);

CREATE POLICY "Allow anonymous inserts" ON cornerstone_nda_submissions
  FOR INSERT TO anon WITH CHECK (true);

-- Allow anonymous select on all tables (for analytics dashboard)
CREATE POLICY "Allow anonymous select" ON cornerstone_visits
  FOR SELECT TO anon USING (true);

CREATE POLICY "Allow anonymous update own visit" ON cornerstone_visits
  FOR UPDATE TO anon USING (true) WITH CHECK (true);

CREATE POLICY "Allow anonymous select" ON cornerstone_events
  FOR SELECT TO anon USING (true);

CREATE POLICY "Allow anonymous select" ON cornerstone_buyer_profiles
  FOR SELECT TO anon USING (true);

CREATE POLICY "Allow anonymous update" ON cornerstone_buyer_profiles
  FOR UPDATE TO anon USING (true) WITH CHECK (true);

CREATE POLICY "Allow anonymous select" ON cornerstone_nda_submissions
  FOR SELECT TO anon USING (true);

-- Service role has full access (for admin dashboard / Supabase dashboard)
CREATE POLICY "Service role full access" ON cornerstone_visits
  FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY "Service role full access" ON cornerstone_events
  FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY "Service role full access" ON cornerstone_buyer_profiles
  FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY "Service role full access" ON cornerstone_nda_submissions
  FOR ALL TO service_role USING (true) WITH CHECK (true);

-- ============================================================
-- DONE! Tables are ready.
-- Next: Update the landing page with your Supabase URL and anon key.
-- ============================================================
