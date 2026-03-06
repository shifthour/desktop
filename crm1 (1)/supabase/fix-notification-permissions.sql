-- Fix notification permissions for proper role-based access
-- Run this in Supabase SQL Editor

-- Step 1: Drop the overly permissive testing policy
DROP POLICY IF EXISTS "Allow all for testing" ON notifications;

-- Step 2: Create proper role-based policies

-- Super admins can see all system notifications and manage everything
CREATE POLICY "Super admin system access" ON notifications
    FOR ALL 
    TO authenticated
    USING (
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_super_admin = true
        ) 
        AND (company_id IS NULL OR entity_type = 'system')
    )
    WITH CHECK (
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_super_admin = true
        )
    );

-- Company admins can see notifications for their company
CREATE POLICY "Company admin access" ON notifications
    FOR SELECT 
    TO authenticated
    USING (
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_admin = true
            AND users.company_id = notifications.company_id
        )
    );

-- Users can see their personal notifications
CREATE POLICY "User personal access" ON notifications
    FOR SELECT 
    TO authenticated
    USING (user_id = auth.uid());

-- Users can see notifications for their company
CREATE POLICY "User company access" ON notifications
    FOR SELECT 
    TO authenticated
    USING (
        company_id IN (
            SELECT company_id FROM users 
            WHERE users.id = auth.uid()
        )
        AND company_id IS NOT NULL
    );

-- Allow updates for marking as read (users can only update their accessible notifications)
CREATE POLICY "Update accessible notifications" ON notifications
    FOR UPDATE 
    TO authenticated
    USING (
        -- Super admins can update all
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_super_admin = true
        )
        OR 
        -- Users can update their own notifications
        user_id = auth.uid()
        OR 
        -- Company admins can update their company notifications
        (
            EXISTS (
                SELECT 1 FROM users 
                WHERE users.id = auth.uid() 
                AND users.is_admin = true
                AND users.company_id = notifications.company_id
            )
        )
    )
    WITH CHECK (
        -- Super admins can update to anything
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_super_admin = true
        )
        OR 
        -- Others can only update read status
        (
            user_id = OLD.user_id 
            AND company_id = OLD.company_id 
            AND title = OLD.title 
            AND message = OLD.message
        )
    );

-- Verify the policies are working
SELECT 
    'Notification permissions updated!' as status,
    COUNT(*) as total_notifications
FROM notifications;