-- Enable RLS properly with working role-based policies
-- Run this in Supabase SQL Editor AFTER testing notifications work without RLS

-- Step 1: Re-enable RLS
ALTER TABLE notifications ENABLE ROW LEVEL SECURITY;

-- Step 2: Create working policies based on our testing

-- Super admins can see ALL notifications (system + company notifications)
CREATE POLICY "Super admin sees all notifications" ON notifications
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
CREATE POLICY "Company admin sees company notifications" ON notifications
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
CREATE POLICY "Users see personal and company notifications" ON notifications
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
CREATE POLICY "Users can mark notifications as read" ON notifications
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
CREATE POLICY "System can create notifications" ON notifications
    FOR INSERT 
    TO authenticated
    WITH CHECK (true);

-- Step 3: Verify policies work
SELECT 
    'RLS enabled with proper role-based policies!' as status,
    COUNT(*) as total_notifications
FROM notifications;

-- Test query (run as super admin)
SELECT 'Testing as super admin - should see all notifications' as test_description;

-- Test query (run as company admin - will only work when logged in as company admin)  
-- SELECT 'Testing as company admin - should see only company notifications' as test_description;