-- Fix notifications RLS policies for proper notification creation
-- Run this in Supabase SQL Editor

-- Step 1: Drop existing policies
DROP POLICY IF EXISTS "Super admins can access all notifications" ON notifications;
DROP POLICY IF EXISTS "Users can access their own notifications" ON notifications;
DROP POLICY IF EXISTS "Company admins can access company notifications" ON notifications;

-- Step 2: Create policies that allow both reading and writing
-- Super admins can do everything with notifications
CREATE POLICY "Super admins full access" ON notifications
    FOR ALL 
    USING (
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_super_admin = true
        )
    );

-- Allow INSERT for system notifications (no user_id required)
CREATE POLICY "Allow system notifications" ON notifications
    FOR INSERT 
    WITH CHECK (user_id IS NULL OR company_id IS NULL);

-- Users can read their own notifications
CREATE POLICY "Users read own notifications" ON notifications
    FOR SELECT 
    USING (user_id = auth.uid());

-- Company admins can read company notifications
CREATE POLICY "Company admins read company notifications" ON notifications
    FOR SELECT 
    USING (
        company_id IN (
            SELECT company_id FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_admin = true
        )
    );

-- Step 3: Insert test notifications
INSERT INTO notifications (title, message, type, entity_type, created_at) VALUES
('System Ready', 'Notification system is now fully operational', 'success', 'system', NOW()),
('Test Notification', 'This is a test notification to verify the system works', 'info', 'system', NOW() - INTERVAL '5 minutes'),
('Welcome', 'Welcome to the real-time notification center', 'info', 'system', NOW() - INTERVAL '1 hour')
ON CONFLICT DO NOTHING;

-- Step 4: Verify the setup
SELECT 
    'Notifications setup complete!' as status,
    COUNT(*) as total_notifications,
    COUNT(CASE WHEN is_read = false THEN 1 END) as unread_notifications
FROM notifications;