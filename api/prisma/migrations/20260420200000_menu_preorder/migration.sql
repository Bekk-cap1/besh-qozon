-- MenuItem
CREATE TABLE IF NOT EXISTS "menu_items" (
  "id"          TEXT PRIMARY KEY,
  "branch_id"   TEXT NOT NULL,
  "name"        TEXT NOT NULL,
  "description" TEXT,
  "category"    TEXT NOT NULL DEFAULT 'Asosiy',
  "price"       NUMERIC(12,2) NOT NULL,
  "image_url"   TEXT,
  "is_active"   BOOLEAN NOT NULL DEFAULT true,
  "sort_order"  INTEGER NOT NULL DEFAULT 0,
  "created_at"  TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at"  TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "menu_items_branch_fk" FOREIGN KEY ("branch_id") REFERENCES "branches"("id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "menu_items_branch_active_sort_idx" ON "menu_items"("branch_id", "is_active", "sort_order");

-- ReservationItem
CREATE TABLE IF NOT EXISTS "reservation_items" (
  "id"             TEXT PRIMARY KEY,
  "reservation_id" TEXT NOT NULL,
  "menu_item_id"   TEXT NOT NULL,
  "name"           TEXT NOT NULL,
  "unit_price"     NUMERIC(12,2) NOT NULL,
  "quantity"       INTEGER NOT NULL DEFAULT 1,
  "notes"          TEXT,
  "created_at"     TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "reservation_items_reservation_fk" FOREIGN KEY ("reservation_id") REFERENCES "reservations"("id") ON DELETE CASCADE,
  CONSTRAINT "reservation_items_menu_item_fk"   FOREIGN KEY ("menu_item_id")   REFERENCES "menu_items"("id")   ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS "reservation_items_reservation_idx" ON "reservation_items"("reservation_id");
