-- Comprehensive notifications migration script
-- Run this in Supabase SQL Editor to create the notifications system

-- Step 1: Drop table if exists (for clean installation)
DROP TABLE IF EXISTS notifications CASCADE;

-- Step 2: Create notifications table
CREATE TABLE notifications (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE, -- NULL for system-wide notifications
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE, -- NULL for super admin notifications
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'info' CHECK (type IN ('info', 'success', 'warning', 'error')),
    entity_type TEXT, -- 'company', 'user', 'system'
    entity_id UUID, -- Reference to the related entity
    is_read BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL
);

-- Step 3: Create indexes for performance
CREATE INDEX idx_notifications_user_id ON notifications(user_id);
CREATE INDEX idx_notifications_company_id ON notifications(company_id);
CREATE INDEX idx_notifications_created_at ON notifications(created_at DESC);
CREATE INDEX idx_notifications_unread ON notifications(user_id, is_read, created_at DESC);

-- Step 4: Enable Row Level Security
ALTER TABLE notifications ENABLE ROW LEVEL SECURITY;

-- Step 5: Create RLS Policies
-- Super admins can access all notifications
CREATE POLICY "Super admins can access all notifications" ON notifications
    FOR ALL USING (
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_super_admin = true
        )
    );

-- Users can access their own notifications
CREATE POLICY "Users can access their own notifications" ON notifications
    FOR SELECT USING (user_id = auth.uid());

-- Company admins can access company notifications
CREATE POLICY "Company admins can access company notifications" ON notifications
    FOR SELECT USING (
        company_id IN (
            SELECT company_id FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_admin = true
        )
    );

-- Step 6: Create trigger for updated_at
CREATE TRIGGER update_notifications_updated_at BEFORE UPDATE ON notifications
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Step 7: Insert initial system notifications for testing
-- Get the super admin user ID for initial notifications
DO $$ 
DECLARE 
    super_admin_id UUID;
    sample_company_id UUID;
BEGIN
    -- Get super admin user ID
    SELECT id INTO super_admin_id FROM users WHERE is_super_admin = true LIMIT 1;
    
    -- Get a sample company ID if exists
    SELECT id INTO sample_company_id FROM companies LIMIT 1;
    
    -- Insert system notifications (visible to super admin)
    INSERT INTO notifications (title, message, type, entity_type) VALUES
    ('System Activated', 'Real-time notification system is now active and operational', 'success', 'system'),
    ('Welcome to CRM', 'Your notification center will show real-time updates for company and user activities', 'info', 'system'),
    ('Database Updated', 'Notifications table has been successfully created and configured', 'success', 'system');
    
    -- Insert sample company notification if company exists
    IF sample_company_id IS NOT NULL THEN
        INSERT INTO notifications (title, message, type, entity_type, entity_id) VALUES
        ('Sample Company Activity', 'This is a test notification for company management activities', 'info', 'company', sample_company_id);
    END IF;
    
END $$;

-- Step 8: Verify installation
SELECT 
    'Notifications table created successfully!' as status,
    COUNT(*) as notification_count,
    COUNT(CASE WHEN is_read = false THEN 1 END) as unread_count
FROM notifications;

-- Show sample data
SELECT 
    id,
    title,
    message,
    type,
    is_read,
    created_at
FROM notifications 
ORDER BY created_at DESC 
LIMIT 5;