CREATE TABLE IF NOT EXISTS "reviews" (
  "id"             TEXT PRIMARY KEY,
  "branch_id"      TEXT NOT NULL,
  "user_id"        TEXT NOT NULL,
  "reservation_id" TEXT UNIQUE,
  "rating"         INTEGER NOT NULL,
  "comment"        TEXT,
  "is_published"   BOOLEAN NOT NULL DEFAULT true,
  "created_at"     TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "reviews_branch_fk"      FOREIGN KEY ("branch_id")      REFERENCES "branches"("id")      ON DELETE CASCADE,
  CONSTRAINT "reviews_user_fk"        FOREIGN KEY ("user_id")        REFERENCES "users"("id")         ON DELETE CASCADE,
  CONSTRAINT "reviews_reservation_fk" FOREIGN KEY ("reservation_id") REFERENCES "reservations"("id")  ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS "reviews_branch_published_idx" ON "reviews"("branch_id", "is_published");
