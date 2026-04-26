-- Чистим дубликаты (zone_id, number): второй, третий и т.д. получают суффикс "-dupN".
-- У дублей с бронями история остаётся связанной через FK на id.
WITH ranked AS (
  SELECT id,
         ROW_NUMBER() OVER (
           PARTITION BY zone_id, number
           ORDER BY id
         ) AS rn
  FROM tables
)
UPDATE tables t
SET number = t.number || '-dup' || r.rn
FROM ranked r
WHERE r.id = t.id
  AND r.rn > 1;

-- Уникальный индекс: в одной зоне номер стола уникален.
CREATE UNIQUE INDEX IF NOT EXISTS "tables_zone_number_unique"
  ON "tables" ("zone_id", "number");
