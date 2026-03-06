-- ============================================
-- CUSTOM AUTH: Drop Supabase Auth dependency
-- Run this ENTIRE script in Supabase SQL Editor
-- This replaces auth.users with our own yajur_profiles table
-- ============================================

-- Step 1: Drop old trigger on auth.users
DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;
DROP FUNCTION IF EXISTS yajur_handle_new_user() CASCADE;
DROP FUNCTION IF EXISTS yajur_is_admin() CASCADE;
DROP FUNCTION IF EXISTS yajur_set_updated_at() CASCADE;

-- Step 2: Drop old tables (CASCADE to remove any remaining policies/deps)
DROP TABLE IF EXISTS yajur_activity_log CASCADE;
DROP TABLE IF EXISTS yajur_projects CASCADE;
DROP TABLE IF EXISTS yajur_profiles CASCADE;

-- Step 3: Create yajur_profiles with password_hash (NO auth.users dependency)
CREATE TABLE yajur_profiles (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  full_name       TEXT NOT NULL,
  email           TEXT NOT NULL UNIQUE,
  password_hash   TEXT NOT NULL,
  role            TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('admin', 'user')),
  is_active       BOOLEAN NOT NULL DEFAULT TRUE,
  created_by      UUID,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Step 4: Create yajur_projects (FK to yajur_profiles)
CREATE TABLE yajur_projects (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id             UUID NOT NULL REFERENCES yajur_profiles(id) ON DELETE CASCADE,
  name                TEXT NOT NULL,
  slug                TEXT NOT NULL,
  pdf_storage_path    TEXT,
  pdf_original_name   TEXT,
  prompt              TEXT NOT NULL DEFAULT '',
  generated_html      TEXT,
  github_repo_url     TEXT,
  vercel_url          TEXT,
  vercel_project_name TEXT,
  status              TEXT NOT NULL DEFAULT 'draft' CHECK (
    status IN ('draft', 'generating', 'deploying', 'deployed', 'error')
  ),
  error_message       TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Step 5: Create yajur_activity_log (FK to yajur_profiles)
CREATE TABLE yajur_activity_log (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  user_id         UUID NOT NULL REFERENCES yajur_profiles(id) ON DELETE CASCADE,
  action          TEXT NOT NULL CHECK (action IN (
    'user_login',
    'user_created',
    'user_updated',
    'user_deactivated',
    'project_created',
    'project_updated',
    'project_generated',
    'project_deployed',
    'project_deleted',
    'pdf_uploaded'
  )),
  resource_type   TEXT CHECK (resource_type IN ('user', 'project', 'pdf')),
  resource_id     TEXT,
  details         JSONB DEFAULT '{}',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Step 6: Indexes
CREATE INDEX IF NOT EXISTS idx_yajur_profiles_email     ON yajur_profiles (email);
CREATE INDEX IF NOT EXISTS idx_yajur_profiles_role      ON yajur_profiles (role);
CREATE INDEX IF NOT EXISTS idx_yajur_profiles_active    ON yajur_profiles (is_active);

CREATE INDEX IF NOT EXISTS idx_yajur_projects_user_id   ON yajur_projects (user_id);
CREATE INDEX IF NOT EXISTS idx_yajur_projects_status    ON yajur_projects (status);
CREATE INDEX IF NOT EXISTS idx_yajur_projects_created   ON yajur_projects (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_yajur_projects_updated   ON yajur_projects (updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_yajur_activity_user      ON yajur_activity_log (user_id);
CREATE INDEX IF NOT EXISTS idx_yajur_activity_action    ON yajur_activity_log (action);
CREATE INDEX IF NOT EXISTS idx_yajur_activity_created   ON yajur_activity_log (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_yajur_activity_resource  ON yajur_activity_log (resource_type, resource_id);

-- Step 7: Updated_at trigger
CREATE OR REPLACE FUNCTION yajur_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER yajur_profiles_updated_at
  BEFORE UPDATE ON yajur_profiles
  FOR EACH ROW EXECUTE FUNCTION yajur_set_updated_at();

CREATE OR REPLACE TRIGGER yajur_projects_updated_at
  BEFORE UPDATE ON yajur_projects
  FOR EACH ROW EXECUTE FUNCTION yajur_set_updated_at();

-- Step 8: Disable RLS (we handle auth in application code with service role key)
ALTER TABLE yajur_profiles DISABLE ROW LEVEL SECURITY;
ALTER TABLE yajur_projects DISABLE ROW LEVEL SECURITY;
ALTER TABLE yajur_activity_log DISABLE ROW LEVEL SECURITY;

-- Step 9: Storage bucket (keep as-is)
INSERT INTO storage.buckets (id, name, public)
VALUES ('yajur-pdfs', 'yajur-pdfs', false)
ON CONFLICT (id) DO NOTHING;

-- Step 10: Reload schema cache
NOTIFY pgrst, 'reload schema';

-- ============================================
-- NOTE: Admin user will be created via the /api/setup endpoint
-- after deploying the app. No need to insert manually.
-- ============================================
