-- Notifications table migration
-- Run this in Supabase SQL Editor

-- Notifications table
CREATE TABLE IF NOT EXISTS notifications (
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

-- Create indexes for notifications
CREATE INDEX IF NOT EXISTS idx_notifications_user_id ON notifications(user_id);
CREATE INDEX IF NOT EXISTS idx_notifications_company_id ON notifications(company_id);
CREATE INDEX IF NOT EXISTS idx_notifications_created_at ON notifications(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_unread ON notifications(user_id, is_read, created_at DESC);

-- Enable RLS for notifications
ALTER TABLE notifications ENABLE ROW LEVEL SECURITY;

-- Drop existing policies if they exist
DROP POLICY IF EXISTS "Super admins can access all notifications" ON notifications;
DROP POLICY IF EXISTS "Users can access their own notifications" ON notifications;
DROP POLICY IF EXISTS "Company admins can access company notifications" ON notifications;

-- RLS Policies for notifications
CREATE POLICY "Super admins can access all notifications" ON notifications
    FOR ALL USING (
        EXISTS (
            SELECT 1 FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_super_admin = true
        )
    );

CREATE POLICY "Users can access their own notifications" ON notifications
    FOR SELECT USING (user_id = auth.uid());

CREATE POLICY "Company admins can access company notifications" ON notifications
    FOR SELECT USING (
        company_id IN (
            SELECT company_id FROM users 
            WHERE users.id = auth.uid() 
            AND users.is_admin = true
        )
    );

-- Drop existing trigger if it exists
DROP TRIGGER IF EXISTS update_notifications_updated_at ON notifications;

-- Trigger for updated_at
CREATE TRIGGER update_notifications_updated_at BEFORE UPDATE ON notifications
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert some sample notifications for super admin to test
INSERT INTO notifications (title, message, type, entity_type) VALUES
('System Ready', 'Real-time notification system has been activated', 'success', 'system'),
('Welcome Message', 'Your CRM notification system is now live and ready to receive real-time updates', 'info', 'system')
ON CONFLICT DO NOTHING;