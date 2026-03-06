-- ============================================
-- Supabase Setup for Shat-R-Shield Analytics
-- Run this entire script in the Supabase SQL Editor
-- Tables: shatrshield_visits, shatrshield_events, shatrshield_leads
-- ============================================

-- =========================
-- TABLE 1: Page Visits
-- One row per browser session
-- =========================
CREATE TABLE IF NOT EXISTS shatrshield_visits (
  id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  session_id          UUID NOT NULL,
  referrer            TEXT,
  user_agent          TEXT,
  device_type         TEXT CHECK (device_type IN ('desktop', 'tablet', 'mobile')),
  screen_width        INTEGER,
  screen_height       INTEGER,
  duration_seconds    INTEGER DEFAULT 0,
  max_scroll_percent  INTEGER DEFAULT 0,
  country             TEXT,
  utm_source          TEXT,
  utm_medium          TEXT,
  utm_campaign        TEXT,
  created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- =========================
-- TABLE 2: Events
-- All user interactions tracked
-- =========================
CREATE TABLE IF NOT EXISTS shatrshield_events (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  session_id      UUID NOT NULL,
  event_type      TEXT NOT NULL CHECK (event_type IN (
    'page_view',
    'section_view',
    'modal_open',
    'modal_close',
    'tab_switch',
    'expand_toggle',
    'cta_click',
    'scroll_milestone',
    'content_unlock',
    'form_open',
    'chart_interaction',
    'nav_click',
    'cycle_switch'
  )),
  event_target    TEXT,
  event_data      JSONB DEFAULT '{}',
  section_id      TEXT,
  created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- =========================
-- TABLE 3: Leads
-- Form submissions & Google sign-ins
-- =========================
CREATE TABLE IF NOT EXISTS shatrshield_leads (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  session_id      UUID NOT NULL,
  full_name       TEXT,
  work_email      TEXT,
  phone           TEXT,
  company         TEXT,
  message         TEXT,
  nda_accepted    BOOLEAN DEFAULT FALSE,
  unlock_method   TEXT CHECK (unlock_method IN ('form', 'google_signin')),
  created_at      TIMESTAMPTZ DEFAULT NOW()
);



-- =========================
-- TABLE 4: Google Sign-ins
-- Captures email from real Google OAuth
-- =========================
CREATE TABLE IF NOT EXISTS shatrshield_google_signins (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  session_id      UUID NOT NULL,
  email           TEXT NOT NULL,
  name            TEXT,
  picture_url     TEXT,
  nda_accepted    BOOLEAN DEFAULT TRUE,
  created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- =========================
-- INDEXES
-- =========================
CREATE INDEX IF NOT EXISTS idx_visits_session   ON shatrshield_visits (session_id);
CREATE INDEX IF NOT EXISTS idx_visits_created   ON shatrshield_visits (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_visits_device    ON shatrshield_visits (device_type);

CREATE INDEX IF NOT EXISTS idx_events_session   ON shatrshield_events (session_id);
CREATE INDEX IF NOT EXISTS idx_events_type      ON shatrshield_events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_created   ON shatrshield_events (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_section   ON shatrshield_events (section_id);

CREATE INDEX IF NOT EXISTS idx_leads_created    ON shatrshield_leads (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_leads_email      ON shatrshield_leads (work_email);

CREATE INDEX IF NOT EXISTS idx_gsignins_created ON shatrshield_google_signins (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_gsignins_email   ON shatrshield_google_signins (email);

-- =========================
-- ROW LEVEL SECURITY (RLS)
-- =========================

-- Enable RLS
ALTER TABLE shatrshield_visits ENABLE ROW LEVEL SECURITY;
ALTER TABLE shatrshield_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE shatrshield_leads  ENABLE ROW LEVEL SECURITY;
ALTER TABLE shatrshield_google_signins ENABLE ROW LEVEL SECURITY;

-- Visits: anon can INSERT (tracking), SELECT (dashboard), UPDATE (flush on exit)
CREATE POLICY "anon_insert_visits" ON shatrshield_visits FOR INSERT TO anon WITH CHECK (true);
CREATE POLICY "anon_select_visits" ON shatrshield_visits FOR SELECT TO anon USING (true);
CREATE POLICY "anon_update_visits" ON shatrshield_visits FOR UPDATE TO anon USING (true) WITH CHECK (true);

-- Events: anon can INSERT (tracking) and SELECT (dashboard)
CREATE POLICY "anon_insert_events" ON shatrshield_events FOR INSERT TO anon WITH CHECK (true);
CREATE POLICY "anon_select_events" ON shatrshield_events FOR SELECT TO anon USING (true);

-- Leads: anon can INSERT (form submit) and SELECT (dashboard)
CREATE POLICY "anon_insert_leads" ON shatrshield_leads FOR INSERT TO anon WITH CHECK (true);
CREATE POLICY "anon_select_leads" ON shatrshield_leads FOR SELECT TO anon USING (true);

-- Google Sign-ins: anon can INSERT (sign-in capture) and SELECT (dashboard)
CREATE POLICY "anon_insert_gsignins" ON shatrshield_google_signins FOR INSERT TO anon WITH CHECK (true);
CREATE POLICY "anon_select_gsignins" ON shatrshield_google_signins FOR SELECT TO anon USING (true);


-- =========================
-- TABLE 5: Email Access Control
-- Whitelist for email-based page access
-- =========================
CREATE TABLE IF NOT EXISTS shatrshield_email_access (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  email           TEXT NOT NULL,
  status          TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
  added_by        TEXT DEFAULT 'user' CHECK (added_by IN ('user', 'admin')),
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  reviewed_at     TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_email_access_email  ON shatrshield_email_access (email);
CREATE INDEX IF NOT EXISTS idx_email_access_status ON shatrshield_email_access (status);
CREATE INDEX IF NOT EXISTS idx_email_access_created ON shatrshield_email_access (created_at DESC);

ALTER TABLE shatrshield_email_access ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_insert_email_access" ON shatrshield_email_access FOR INSERT TO anon WITH CHECK (true);
CREATE POLICY "anon_select_email_access" ON shatrshield_email_access FOR SELECT TO anon USING (true);
CREATE POLICY "anon_update_email_access" ON shatrshield_email_access FOR UPDATE TO anon USING (true) WITH CHECK (true);