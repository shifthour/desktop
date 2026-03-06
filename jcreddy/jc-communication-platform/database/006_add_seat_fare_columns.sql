-- ═══════════════════════════════════════════════════════════════════════════════
-- ADD SEAT TYPE FARE COLUMNS TO FARE COMPARISONS
-- Stores independent per-seat-type fare arrays for Mythri and competitor
-- ═══════════════════════════════════════════════════════════════════════════════

ALTER TABLE jc_fare_comparisons
  ADD COLUMN IF NOT EXISTS mythri_seat_type_fares JSONB DEFAULT '[]',
  ADD COLUMN IF NOT EXISTS competitor_seat_type_fares JSONB DEFAULT '[]';
