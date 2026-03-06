-- Simple working notification system
-- Run this in Supabase SQL Editor

-- Step 1: Drop all existing policies to start fresh
DROP POLICY IF EXISTS "Super admin system access" ON notifications;
DROP POLICY IF EXISTS "Company admin access" ON notifications;  
DROP POLICY IF EXISTS "User personal access" ON notifications;
DROP POLICY IF EXISTS "User company access" ON notifications;
DROP POLICY IF EXISTS "Update notifications" ON notifications;

-- Step 2: Create simple, working policies

-- Super admins can do everything
CREATE POLICY "Super admin full access" ON notifications
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

-- Company admins see their company notifications + personal notifications
CREATE POLICY "Company admin see company notifications" ON notifications
    FOR SELECT 
    TO authenticated
    USING (
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_admin = true
            AND (
                users.company_id = notifications.company_id
                OR notifications.user_id = auth.uid()
            )
        )
    );

-- Regular users see their personal notifications + company notifications
CREATE POLICY "Users see personal and company notifications" ON notifications
    FOR SELECT 
    TO authenticated
    USING (
        notifications.user_id = auth.uid()
        OR 
        notifications.company_id IN (
            SELECT company_id FROM users 
            WHERE users.id = auth.uid()
            AND users.company_id IS NOT NULL
        )
    );

-- Allow everyone to update notifications they can see (for marking as read)
CREATE POLICY "Update accessible notifications" ON notifications
    FOR UPDATE 
    TO authenticated
    USING (
        -- Same logic as SELECT policies
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_super_admin = true
        )
        OR
        (
            EXISTS (
                SELECT 1 FROM users 
                WHERE users.id = auth.uid() 
                AND users.is_admin = true
                AND (
                    users.company_id = notifications.company_id
                    OR notifications.user_id = auth.uid()
                )
            )
        )
        OR
        (
            notifications.user_id = auth.uid()
            OR 
            notifications.company_id IN (
                SELECT company_id FROM users 
                WHERE users.id = auth.uid()
                AND users.company_id IS NOT NULL
            )
        )
    );

-- Step 3: Allow system to create notifications (for APIs)
CREATE POLICY "System create notifications" ON notifications
    FOR INSERT 
    TO authenticated
    WITH CHECK (true);

-- Verify setup
SELECT 
    'Simple notification policies created!' as status,
    COUNT(*) as total_notifications,
    COUNT(CASE WHEN is_read = false THEN 1 END) as unread_notifications
FROM notifications;