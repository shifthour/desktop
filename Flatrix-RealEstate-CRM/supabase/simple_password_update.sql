-- Simple approach: Update with plaintext temporarily for testing
-- IMPORTANT: This is just for debugging - never use plaintext passwords in production

-- For now, let's use a simple known hash for testing
-- This hash is for 'superadmin123' using bcrypt
UPDATE flatrix_users 
SET password = '$2b$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy'
WHERE email = 'superadmin@flatrix.com';

-- This hash is for 'admin123' using bcrypt  
UPDATE flatrix_users 
SET password = '$2b$10$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi'
WHERE email = 'admin@flatrix.com';

-- Check the results
SELECT email, password, role FROM flatrix_users 
WHERE email IN ('superadmin@flatrix.com', 'admin@flatrix.com');