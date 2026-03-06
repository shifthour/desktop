-- Clean up and enable RLS properly with working role-based policies
-- Run this in Supabase SQL Editor

-- Step 1: Re-enable RLS first
ALTER TABLE notifications ENABLE ROW LEVEL SECURITY;

-- Step 2: Drop ALL existing policies (comprehensive cleanup)
DROP POLICY IF EXISTS "Super admin sees all notifications" ON notifications;
DROP POLICY IF EXISTS "Company admin sees company notifications" ON notifications;
DROP POLICY IF EXISTS "Users see personal and company notifications" ON notifications;
DROP POLICY IF EXISTS "Users can mark notifications as read" ON notifications;
DROP POLICY IF EXISTS "System can create notifications" ON notifications;
DROP POLICY IF EXISTS "Super admin full access" ON notifications;
DROP POLICY IF EXISTS "Company admin access" ON notifications;
DROP POLICY IF EXISTS "User personal access" ON notifications;
DROP POLICY IF EXISTS "User company access" ON notifications;
DROP POLICY IF EXISTS "Update notifications" ON notifications;
DROP POLICY IF EXISTS "System create notifications" ON notifications;
DROP POLICY IF EXISTS "Super admin system access" ON notifications;
DROP POLICY IF EXISTS "Update accessible notifications" ON notifications;
DROP POLICY IF EXISTS "Allow all for testing" ON notifications;

-- Step 3: Create new working policies

-- Super admins can see ALL notifications (system + company notifications)
CREATE POLICY "super_admin_all_access" ON notifications
    FOR SELECT 
    TO authenticated
    USING (
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_super_admin = true
        )
    );

-- Company admins see only their company notifications + personal notifications  
CREATE POLICY "company_admin_company_access" ON notifications
    FOR SELECT 
    TO authenticated
    USING (
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_admin = true
            AND users.company_id = notifications.company_id
        )
        OR notifications.user_id = auth.uid()
    );

-- Regular users see personal notifications + their company notifications
CREATE POLICY "users_personal_company_access" ON notifications
    FOR SELECT 
    TO authenticated
    USING (
        notifications.user_id = auth.uid()
        OR (
            notifications.company_id IN (
                SELECT company_id FROM users 
                WHERE users.id = auth.uid()
                AND users.company_id IS NOT NULL
            )
            AND NOT EXISTS (
                SELECT 1 FROM users 
                WHERE users.id = auth.uid() 
                AND users.is_admin = true
            )
        )
    );

-- Allow everyone to update notifications (for marking as read)
CREATE POLICY "users_mark_read_access" ON notifications
    FOR UPDATE 
    TO authenticated
    USING (
        -- Can see the notification (reuse SELECT logic)
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_super_admin = true
        )
        OR (
            EXISTS (
                SELECT 1 FROM users 
                WHERE users.id = auth.uid() 
                AND users.is_admin = true
                AND users.company_id = notifications.company_id
            )
            OR notifications.user_id = auth.uid()
        )
        OR (
            notifications.user_id = auth.uid()
            OR (
                notifications.company_id IN (
                    SELECT company_id FROM users 
                    WHERE users.id = auth.uid()
                    AND users.company_id IS NOT NULL
                )
                AND NOT EXISTS (
                    SELECT 1 FROM users 
                    WHERE users.id = auth.uid() 
                    AND users.is_admin = true
                )
            )
        )
    );

-- Allow system/API to create notifications without restrictions
CREATE POLICY "system_create_access" ON notifications
    FOR INSERT 
    TO authenticated
    WITH CHECK (true);

-- Step 4: Verify setup
SELECT 
    'RLS enabled with clean role-based policies!' as status,
    COUNT(*) as total_notifications
FROM notifications;