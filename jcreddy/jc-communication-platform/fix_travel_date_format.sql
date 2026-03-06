-- =====================================================
-- FIX TRAVEL_DATE FORMAT IN JC_BOOKINGS
-- Run this in Supabase SQL Editor
-- =====================================================

-- This script fixes travel_date values that are stored in DD-MM-YYYY format
-- and converts them to YYYY-MM-DD format (ISO standard)

-- First, let's see how many records have the wrong format
SELECT
  COUNT(*) as total_records,
  SUM(CASE WHEN travel_date ~ '^\d{2}-\d{2}-\d{4}$' THEN 1 ELSE 0 END) as dd_mm_yyyy_format,
  SUM(CASE WHEN travel_date ~ '^\d{4}-\d{2}-\d{2}' THEN 1 ELSE 0 END) as yyyy_mm_dd_format
FROM jc_bookings
WHERE travel_date IS NOT NULL;

-- Update records with DD-MM-YYYY format to YYYY-MM-DD
UPDATE jc_bookings
SET travel_date =
  SUBSTRING(travel_date FROM 7 FOR 4) || '-' ||  -- Year
  SUBSTRING(travel_date FROM 4 FOR 2) || '-' ||  -- Month
  SUBSTRING(travel_date FROM 1 FOR 2)            -- Day
WHERE travel_date ~ '^\d{2}-\d{2}-\d{4}$';

-- Verify the fix
SELECT
  COUNT(*) as total_records,
  SUM(CASE WHEN travel_date ~ '^\d{2}-\d{2}-\d{4}$' THEN 1 ELSE 0 END) as dd_mm_yyyy_format,
  SUM(CASE WHEN travel_date ~ '^\d{4}-\d{2}-\d{2}' THEN 1 ELSE 0 END) as yyyy_mm_dd_format
FROM jc_bookings
WHERE travel_date IS NOT NULL;
