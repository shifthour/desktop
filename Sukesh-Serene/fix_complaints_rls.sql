-- Disable Row Level Security on complaints table
ALTER TABLE serene_complaints DISABLE ROW LEVEL SECURITY;

-- OR if you want to keep RLS enabled, add these policies:
-- (Comment out the line above and uncomment the lines below)

-- Enable RLS
-- ALTER TABLE serene_complaints ENABLE ROW LEVEL SECURITY;

-- Allow tenants to insert their own complaints
-- CREATE POLICY "Tenants can insert own complaints"
--   ON serene_complaints
--   FOR INSERT
--   WITH CHECK (true);

-- Allow tenants to view their own complaints
-- CREATE POLICY "Tenants can view own complaints"
--   ON serene_complaints
--   FOR SELECT
--   USING (true);

-- Allow admins to view all complaints
-- CREATE POLICY "Admins can view all complaints"
--   ON serene_complaints
--   FOR SELECT
--   USING (true);

-- Allow admins to update complaints
-- CREATE POLICY "Admins can update complaints"
--   ON serene_complaints
--   FOR UPDATE
--   USING (true);
