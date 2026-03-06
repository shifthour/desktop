-- ═══════════════════════════════════════════════════════════════════════════════
-- ADD VKAVERI AS SECOND COMPETITOR + RESET TO BANGALORE→WARANGAL ONLY
-- ═══════════════════════════════════════════════════════════════════════════════

-- 1. Add VKaveri columns to route config
ALTER TABLE jc_competitor_route_config
  ADD COLUMN IF NOT EXISTS vkaveri_from_id TEXT DEFAULT '',
  ADD COLUMN IF NOT EXISTS vkaveri_to_id TEXT DEFAULT '';

-- 2. Add competitor_operator column to fare comparisons
ALTER TABLE jc_fare_comparisons
  ADD COLUMN IF NOT EXISTS competitor_operator TEXT DEFAULT 'vikram';

-- 3. Add competitor_operator column to competitor fares
ALTER TABLE jc_competitor_fares
  ADD COLUMN IF NOT EXISTS competitor_operator TEXT;
-- Backfill existing rows
UPDATE jc_competitor_fares SET competitor_operator = operator WHERE competitor_operator IS NULL;

-- 4. Deactivate ALL existing routes
UPDATE jc_competitor_route_config SET is_active = false;

-- 5. Expire ALL pending comparisons (old data with wrong matching)
UPDATE jc_fare_comparisons SET status = 'expired' WHERE status = 'pending';

-- 6. Add Bangalore → Warangal with all 3 operators
INSERT INTO jc_competitor_route_config (route_name, route_from, route_to, mythri_from_id, mythri_to_id, vikram_from_code, vikram_to_code, vkaveri_from_id, vkaveri_to_id)
VALUES ('Bangalore → Warangal', 'Bangalore', 'Warangal', '89', '283', 'STF3OEX206', 'ST5B861339E', '18', '561')
ON CONFLICT (route_from, route_to) DO UPDATE SET
  is_active = true,
  vkaveri_from_id = '18',
  vkaveri_to_id = '561';
