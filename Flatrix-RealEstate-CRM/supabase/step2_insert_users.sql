-- Step 2: Insert users (run this after step 1 is committed)

-- Insert default super admin user
-- Password is 'superadmin123' hashed with bcrypt
INSERT INTO flatrix_users (email, password, role, full_name, name) 
VALUES (
    'superadmin@flatrix.com', 
    '$2a$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewkyO4dMCFGUwfDe', 
    'super_admin',
    'Super Administrator',
    'Super Administrator'
) ON CONFLICT (email) DO NOTHING;

-- Insert default admin user
-- Password is 'admin123' hashed with bcrypt
INSERT INTO flatrix_users (email, password, role, full_name, name) 
VALUES (
    'admin@flatrix.com', 
    '$2a$12$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi', 
    'ADMIN',
    'Administrator',
    'Administrator'
) ON CONFLICT (email) DO NOTHING;