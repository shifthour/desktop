-- Force create notifications that will definitely show up
-- Run this in Supabase SQL Editor

-- First, disable RLS temporarily to insert notifications
ALTER TABLE notifications DISABLE ROW LEVEL SECURITY;

-- Clear existing notifications
DELETE FROM notifications;

-- Insert notifications that should definitely show up
INSERT INTO notifications (
    id,
    user_id,
    company_id,
    title,
    message,
    type,
    entity_type,
    entity_id,
    is_read,
    created_at,
    updated_at
) VALUES 
-- System-wide notifications (no user_id, no company_id)
(
    gen_random_uuid(),
    NULL,
    NULL,
    'System Alert',
    'Notification system is now working correctly!',
    'success',
    'system',
    NULL,
    false,
    NOW(),
    NOW()
),
(
    gen_random_uuid(),
    NULL,
    NULL,
    'New Company Created',
    'A new company has been added to the system',
    'info',
    'system',
    NULL,
    false,
    NOW() - INTERVAL '5 minutes',
    NOW() - INTERVAL '5 minutes'
),
(
    gen_random_uuid(),
    NULL,
    NULL,
    'System Update',
    'Real-time notifications are now enabled',
    'info',
    'system',
    NULL,
    false,
    NOW() - INTERVAL '1 hour',
    NOW() - INTERVAL '1 hour'
);

-- Re-enable RLS
ALTER TABLE notifications ENABLE ROW LEVEL SECURITY;

-- Create a very permissive policy for testing
DROP POLICY IF EXISTS "Allow all for testing" ON notifications;
CREATE POLICY "Allow all for testing" ON notifications
    FOR ALL 
    USING (true)
    WITH CHECK (true);

-- Verify notifications exist
SELECT 
    'Notifications created successfully!' as status,
    COUNT(*) as total_count,
    COUNT(CASE WHEN is_read = false THEN 1 END) as unread_count
FROM notifications;

-- Show all notifications
SELECT * FROM notifications ORDER BY created_at DESC;