-- Just disable RLS completely - keep notifications working!
-- Run this in Supabase SQL Editor

-- Step 1: Disable RLS completely
ALTER TABLE notifications DISABLE ROW LEVEL SECURITY;

-- Step 2: Verify notifications exist and are accessible
SELECT 
    'RLS disabled - notifications should work for everyone!' as status,
    COUNT(*) as total_notifications,
    COUNT(CASE WHEN is_read = false THEN 1 END) as unread_notifications
FROM notifications;

-- Step 3: Show recent notifications to verify they're there
SELECT 
    id,
    title,
    message,
    type,
    entity_type,
    is_read,
    created_at
FROM notifications 
ORDER BY created_at DESC
LIMIT 10;