-- =====================================================
-- Serene Living - Supervisor User Setup
-- Run this in Supabase SQL Editor
-- =====================================================

-- 1. Check if supervisor already exists
SELECT id, email, name, role, created_at
FROM serene_users
WHERE email = 'supervisor@sereneliving.com';

-- 2. Insert Supervisor User (if not exists)
-- Password: supervisor123
-- Note: The password is already hashed using bcrypt
INSERT INTO serene_users (email, password, name, role)
VALUES (
  'supervisor@sereneliving.com',
  '$2a$10$R0645DKY.DnxVPD9vIHfAunq9CCgMmoqGlbn6qlXq4x6PEQHg4OlK',
  'Supervisor User',
  'supervisor'
)
ON CONFLICT (email) DO NOTHING;

-- 3. Or update existing user to supervisor role (if user already exists)
UPDATE serene_users
SET role = 'supervisor'
WHERE email = 'supervisor@sereneliving.com';

-- 4. Verify the supervisor was created/updated
SELECT id, email, name, role, created_at
FROM serene_users
WHERE email = 'supervisor@sereneliving.com';

-- =====================================================
-- Optional: Reset supervisor password
-- =====================================================
-- If you need to reset the supervisor password to 'supervisor123', run:
UPDATE serene_users
SET password = '$2a$10$R0645DKY.DnxVPD9vIHfAunq9CCgMmoqGlbn6qlXq4x6PEQHg4OlK'
WHERE email = 'supervisor@sereneliving.com';

-- =====================================================
-- View all admin and supervisor users
-- =====================================================
SELECT id, email, name, role, created_at
FROM serene_users
WHERE role IN ('admin', 'supervisor')
ORDER BY created_at;

-- =====================================================
-- CREDENTIALS
-- =====================================================
-- Email: supervisor@sereneliving.com
-- Password: supervisor123
-- Role: supervisor
--
-- Access:
--   ✅ PG Properties, Rooms, Tenants, Payments, Expenses
--   ❌ Dashboard, Payment Review (admin only)
-- =====================================================
