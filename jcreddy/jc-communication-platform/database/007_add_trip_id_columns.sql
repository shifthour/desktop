-- ═══════════════════════════════════════════════════════════════════════════════
-- ADD TRIP ID COLUMNS TO FARE COMPARISONS
-- Stores operator trip identifiers for on-demand seat layout fetching
-- ═══════════════════════════════════════════════════════════════════════════════

ALTER TABLE jc_fare_comparisons
  ADD COLUMN IF NOT EXISTS mythri_trip_id TEXT DEFAULT '',
  ADD COLUMN IF NOT EXISTS competitor_trip_id TEXT DEFAULT '';
