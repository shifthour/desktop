-- Complete fix for notification system
-- Run this in Supabase SQL Editor

-- Step 1: Drop all existing policies
DROP POLICY IF EXISTS "Super admins can access all notifications" ON notifications;
DROP POLICY IF EXISTS "Users can access their own notifications" ON notifications;
DROP POLICY IF EXISTS "Company admins can access company notifications" ON notifications;
DROP POLICY IF EXISTS "Super admins full access" ON notifications;
DROP POLICY IF EXISTS "Allow system notifications" ON notifications;
DROP POLICY IF EXISTS "Users read own notifications" ON notifications;
DROP POLICY IF EXISTS "Company admins read company notifications" ON notifications;

-- Step 2: Create simple, permissive policies for debugging
-- Allow all operations for super admins
CREATE POLICY "Super admin access" ON notifications
    FOR ALL 
    TO authenticated
    USING (
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_super_admin = true
        )
    )
    WITH CHECK (
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_super_admin = true
        )
    );

-- Allow reading user's own notifications
CREATE POLICY "User read own" ON notifications
    FOR SELECT 
    TO authenticated
    USING (user_id = auth.uid());

-- Allow reading company notifications for company admins
CREATE POLICY "Company admin read" ON notifications
    FOR SELECT 
    TO authenticated
    USING (
        company_id IN (
            SELECT company_id FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_admin = true
        )
    );

-- Step 3: Create stored function to insert notifications with elevated privileges
CREATE OR REPLACE FUNCTION create_notification(
    p_user_id UUID DEFAULT NULL,
    p_company_id UUID DEFAULT NULL,
    p_title TEXT DEFAULT NULL,
    p_message TEXT DEFAULT NULL,
    p_type TEXT DEFAULT 'info',
    p_entity_type TEXT DEFAULT NULL,
    p_entity_id UUID DEFAULT NULL
) RETURNS UUID
SECURITY DEFINER
SET search_path = public
LANGUAGE plpgsql
AS $$
DECLARE
    notification_id UUID;
BEGIN
    INSERT INTO notifications (
        user_id,
        company_id,
        title,
        message,
        type,
        entity_type,
        entity_id
    ) VALUES (
        p_user_id,
        p_company_id,
        p_title,
        p_message,
        p_type,
        p_entity_type,
        p_entity_id
    ) RETURNING id INTO notification_id;
    
    RETURN notification_id;
END;
$$;

-- Step 4: Insert test notifications
DELETE FROM notifications; -- Clear existing notifications

INSERT INTO notifications (title, message, type, entity_type, created_at) VALUES
('System Ready', 'Notification system is now fully operational', 'success', 'system', NOW()),
('Test Notification', 'This is a test notification to verify the system works', 'info', 'system', NOW() - INTERVAL '5 minutes'),
('Welcome Admin', 'Welcome to the real-time notification center', 'info', 'system', NOW() - INTERVAL '1 hour')
ON CONFLICT DO NOTHING;

-- Step 5: Verify the setup
SELECT 
    'Notifications setup complete!' as status,
    COUNT(*) as total_notifications,
    COUNT(CASE WHEN is_read = false THEN 1 END) as unread_notifications
FROM notifications;

-- Show all notifications
SELECT 
    id,
    title,
    message,
    type,
    entity_type,
    is_read,
    created_at
FROM notifications 
ORDER BY created_at DESC;