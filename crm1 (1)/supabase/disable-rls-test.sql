-- Temporarily disable RLS to test notifications
-- Run this in Supabase SQL Editor

-- Step 1: Disable RLS completely to test
ALTER TABLE notifications DISABLE ROW LEVEL SECURITY;

-- Step 2: Clear existing notifications and insert fresh test data
DELETE FROM notifications;

-- Step 3: Insert test notifications
INSERT INTO notifications (
    title, 
    message, 
    type, 
    entity_type, 
    is_read, 
    created_at
) VALUES 
('System Ready', 'Notification system is now working correctly!', 'success', 'system', false, NOW()),
('Test Notification', 'This notification should appear for super admin', 'info', 'system', false, NOW() - INTERVAL '5 minutes'),
('Company Activity', 'Test company-related notification', 'info', 'company', false, NOW() - INTERVAL '10 minutes');

-- Step 4: Verify notifications exist
SELECT 
    'RLS disabled - testing notifications' as status,
    COUNT(*) as total_notifications,
    COUNT(CASE WHEN is_read = false THEN 1 END) as unread_notifications
FROM notifications;

-- Step 5: Show all notifications
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