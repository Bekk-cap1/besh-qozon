-- Telegram auth fields for users
ALTER TABLE "users"
  ADD COLUMN IF NOT EXISTS "telegram_id" TEXT,
  ADD COLUMN IF NOT EXISTS "telegram_chat_id" TEXT,
  ADD COLUMN IF NOT EXISTS "telegram_username" TEXT,
  ADD COLUMN IF NOT EXISTS "telegram_first_name" TEXT,
  ADD COLUMN IF NOT EXISTS "telegram_linked_at" TIMESTAMP(3);

CREATE UNIQUE INDEX IF NOT EXISTS "users_telegram_id_key" ON "users"("telegram_id");
