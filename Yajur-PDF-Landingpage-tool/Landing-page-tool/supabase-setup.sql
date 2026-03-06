-- ============================================
-- Supabase Setup for Yajur Landing Page Tool
-- Run this entire script in the Supabase SQL Editor
-- Tables: yajur_profiles, yajur_projects, yajur_activity_log
-- ============================================

-- =========================
-- TABLE 1: User Profiles
-- Extends auth.users with app-specific fields
-- =========================
CREATE TABLE IF NOT EXISTS yajur_profiles (
  id              UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
  full_name       TEXT NOT NULL,
  email           TEXT NOT NULL,
  role            TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('admin', 'user')),
  is_active       BOOLEAN NOT NULL DEFAULT TRUE,
  created_by      UUID REFERENCES auth.users(id),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =========================
-- TABLE 2: Projects
-- Replaces file-based JSON storage
-- =========================
CREATE TABLE IF NOT EXISTS yajur_projects (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id             UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
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

-- =========================
-- TABLE 3: Activity Log
-- Audit trail for all actions
-- =========================
CREATE TABLE IF NOT EXISTS yajur_activity_log (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  user_id         UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
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

-- =========================
-- INDEXES
-- =========================
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

-- =========================
-- UPDATED_AT TRIGGER FUNCTION
-- =========================
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

-- =========================
-- AUTO-CREATE PROFILE ON AUTH SIGNUP
-- =========================
CREATE OR REPLACE FUNCTION yajur_handle_new_user()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO yajur_profiles (id, full_name, email, role)
  VALUES (
    NEW.id,
    COALESCE(NEW.raw_user_meta_data->>'full_name', NEW.email),
    NEW.email,
    COALESCE(NEW.raw_user_meta_data->>'role', 'user')
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Drop existing trigger if it exists to avoid conflicts
DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;

CREATE TRIGGER on_auth_user_created
  AFTER INSERT ON auth.users
  FOR EACH ROW EXECUTE FUNCTION yajur_handle_new_user();

-- =========================
-- ROW LEVEL SECURITY (RLS)
-- =========================

-- Enable RLS on all tables
ALTER TABLE yajur_profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE yajur_projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE yajur_activity_log ENABLE ROW LEVEL SECURITY;

-- SECURITY DEFINER function to check admin status
-- This avoids recursive RLS on yajur_profiles (policy querying its own table)
CREATE OR REPLACE FUNCTION yajur_is_admin()
RETURNS BOOLEAN AS $$
  SELECT EXISTS (
    SELECT 1 FROM yajur_profiles
    WHERE id = auth.uid() AND role = 'admin'
  );
$$ LANGUAGE sql SECURITY DEFINER STABLE;

-- PROFILES: Users can read their own profile
CREATE POLICY "users_read_own_profile"
  ON yajur_profiles FOR SELECT
  TO authenticated
  USING (id = auth.uid());

-- PROFILES: Admins can read all profiles (uses SECURITY DEFINER function)
CREATE POLICY "admins_read_all_profiles"
  ON yajur_profiles FOR SELECT
  TO authenticated
  USING (yajur_is_admin());

-- PROFILES: Admins can update any profile (uses SECURITY DEFINER function)
CREATE POLICY "admins_update_profiles"
  ON yajur_profiles FOR UPDATE
  TO authenticated
  USING (yajur_is_admin());

-- PROJECTS: Users can CRUD their own projects
CREATE POLICY "users_read_own_projects"
  ON yajur_projects FOR SELECT
  TO authenticated
  USING (user_id = auth.uid());

CREATE POLICY "users_insert_own_projects"
  ON yajur_projects FOR INSERT
  TO authenticated
  WITH CHECK (user_id = auth.uid());

CREATE POLICY "users_update_own_projects"
  ON yajur_projects FOR UPDATE
  TO authenticated
  USING (user_id = auth.uid());

CREATE POLICY "users_delete_own_projects"
  ON yajur_projects FOR DELETE
  TO authenticated
  USING (user_id = auth.uid());

-- PROJECTS: Admins can read/update/delete all projects
CREATE POLICY "admins_read_all_projects"
  ON yajur_projects FOR SELECT
  TO authenticated
  USING (yajur_is_admin());

CREATE POLICY "admins_update_all_projects"
  ON yajur_projects FOR UPDATE
  TO authenticated
  USING (yajur_is_admin());

CREATE POLICY "admins_delete_all_projects"
  ON yajur_projects FOR DELETE
  TO authenticated
  USING (yajur_is_admin());

-- ACTIVITY LOG: Users can read their own activity
CREATE POLICY "users_read_own_activity"
  ON yajur_activity_log FOR SELECT
  TO authenticated
  USING (user_id = auth.uid());

-- ACTIVITY LOG: Admins can read all activity
CREATE POLICY "admins_read_all_activity"
  ON yajur_activity_log FOR SELECT
  TO authenticated
  USING (yajur_is_admin());

-- ACTIVITY LOG: Authenticated users can insert their own activity
CREATE POLICY "users_insert_own_activity"
  ON yajur_activity_log FOR INSERT
  TO authenticated
  WITH CHECK (user_id = auth.uid());

-- Reload PostgREST schema cache
NOTIFY pgrst, 'reload schema';

-- =========================
-- SUPABASE STORAGE BUCKET
-- =========================
INSERT INTO storage.buckets (id, name, public)
VALUES ('yajur-pdfs', 'yajur-pdfs', false)
ON CONFLICT (id) DO NOTHING;

-- Storage policy: Authenticated users can upload PDFs
CREATE POLICY "authenticated_upload_pdfs"
  ON storage.objects FOR INSERT
  TO authenticated
  WITH CHECK (bucket_id = 'yajur-pdfs');

-- Storage policy: Users can read their own PDFs (path starts with user_id/)
CREATE POLICY "users_read_own_pdfs"
  ON storage.objects FOR SELECT
  TO authenticated
  USING (
    bucket_id = 'yajur-pdfs'
    AND (storage.foldername(name))[1] = auth.uid()::text
  );

-- Storage policy: Users can delete their own PDFs
CREATE POLICY "users_delete_own_pdfs"
  ON storage.objects FOR DELETE
  TO authenticated
  USING (
    bucket_id = 'yajur-pdfs'
    AND (storage.foldername(name))[1] = auth.uid()::text
  );

-- =========================
-- INITIAL ADMIN USER SETUP
-- =========================
-- After running this script, create your first admin user:
--
-- Option 1: Via Supabase Dashboard
--   Go to Authentication > Users > Add User
--   Then run: UPDATE yajur_profiles SET role = 'admin' WHERE email = 'admin@yourdomain.com';
--
-- Option 2: The tool's API will support admin user creation once deployed
-- =========================
