-- ============================================
-- FIX: RLS Policy Recursion + Schema Cache Reload
-- Run this ENTIRE script in Supabase SQL Editor
-- This fixes "Database error querying schema"
-- ============================================

-- =========================
-- STEP 1: Create SECURITY DEFINER function
-- This function bypasses RLS, breaking the recursion
-- =========================
CREATE OR REPLACE FUNCTION yajur_is_admin()
RETURNS BOOLEAN AS $$
  SELECT EXISTS (
    SELECT 1 FROM yajur_profiles
    WHERE id = auth.uid() AND role = 'admin'
  );
$$ LANGUAGE sql SECURITY DEFINER STABLE;

-- =========================
-- STEP 2: Drop ALL existing policies on ALL tables
-- =========================

-- Drop yajur_profiles policies
DROP POLICY IF EXISTS "users_read_own_profile" ON yajur_profiles;
DROP POLICY IF EXISTS "admins_read_all_profiles" ON yajur_profiles;
DROP POLICY IF EXISTS "admins_update_profiles" ON yajur_profiles;

-- Drop yajur_projects policies
DROP POLICY IF EXISTS "users_read_own_projects" ON yajur_projects;
DROP POLICY IF EXISTS "users_insert_own_projects" ON yajur_projects;
DROP POLICY IF EXISTS "users_update_own_projects" ON yajur_projects;
DROP POLICY IF EXISTS "users_delete_own_projects" ON yajur_projects;
DROP POLICY IF EXISTS "admins_read_all_projects" ON yajur_projects;
DROP POLICY IF EXISTS "admins_update_all_projects" ON yajur_projects;
DROP POLICY IF EXISTS "admins_delete_all_projects" ON yajur_projects;

-- Drop yajur_activity_log policies
DROP POLICY IF EXISTS "users_read_own_activity" ON yajur_activity_log;
DROP POLICY IF EXISTS "admins_read_all_activity" ON yajur_activity_log;
DROP POLICY IF EXISTS "users_insert_own_activity" ON yajur_activity_log;

-- =========================
-- STEP 3: Ensure RLS is enabled
-- =========================
ALTER TABLE yajur_profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE yajur_projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE yajur_activity_log ENABLE ROW LEVEL SECURITY;

-- =========================
-- STEP 4: Recreate ALL policies using yajur_is_admin()
-- =========================

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

-- =========================
-- STEP 5: Reload PostgREST schema cache
-- This is CRITICAL - without this, PostgREST uses stale schema
-- =========================
NOTIFY pgrst, 'reload schema';

-- =========================
-- VERIFICATION: Check policies were created correctly
-- =========================
SELECT schemaname, tablename, policyname, cmd
FROM pg_policies
WHERE tablename LIKE 'yajur_%'
ORDER BY tablename, policyname;
