-- ================================================
-- CLEANUP SAMPLE DATA FROM FLATRIX CRM
-- ================================================
-- Run this script to remove all sample data from your database
-- This will leave the tables structure intact but remove all demo data

-- Disable foreign key checks temporarily to avoid constraint errors
SET session_replication_role = replica;

-- Delete all data from tables in correct order (respecting foreign keys)
DELETE FROM flatrix_commissions;
DELETE FROM flatrix_deals;
DELETE FROM flatrix_activities;
DELETE FROM flatrix_tasks;

-- Clean up properties and projects
DELETE FROM flatrix_properties;
DELETE FROM flatrix_project_partners;
DELETE FROM flatrix_projects;

-- Clean up channel partners and users
DELETE FROM flatrix_channel_partners;
DELETE FROM flatrix_users WHERE email IN ('admin@flatrix.com', 'manager@flatrix.com', 'agent1@flatrix.com', 'agent2@flatrix.com');

-- Reset sequences
ALTER SEQUENCE IF EXISTS deal_number_seq RESTART WITH 1;

-- Re-enable foreign key checks
SET session_replication_role = DEFAULT;

-- Verification queries - uncomment to check if cleanup was successful
-- SELECT 'flatrix_users' as table_name, COUNT(*) as record_count FROM flatrix_users
-- UNION ALL
-- SELECT 'flatrix_channel_partners', COUNT(*) FROM flatrix_channel_partners
-- UNION ALL
-- SELECT 'flatrix_projects', COUNT(*) FROM flatrix_projects
-- UNION ALL
-- SELECT 'flatrix_properties', COUNT(*) FROM flatrix_properties
-- UNION ALL
-- SELECT 'flatrix_leads', COUNT(*) FROM flatrix_leads
-- UNION ALL
-- SELECT 'flatrix_deals', COUNT(*) FROM flatrix_deals
-- UNION ALL
-- SELECT 'flatrix_commissions', COUNT(*) FROM flatrix_commissions
-- UNION ALL
-- SELECT 'flatrix_activities', COUNT(*) FROM flatrix_activities
-- UNION ALL
-- SELECT 'flatrix_tasks', COUNT(*) FROM flatrix_tasks;

-- Note: This script preserves the flatrix_leads table data as it was mentioned to already exist
-- If you want to clean leads data as well, uncomment the line below:
-- DELETE FROM flatrix_leads;