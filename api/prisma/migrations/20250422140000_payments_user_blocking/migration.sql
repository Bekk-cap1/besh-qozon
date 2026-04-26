-- AlterTable
ALTER TABLE "users" ADD COLUMN "no_show_streak" INTEGER NOT NULL DEFAULT 0;
ALTER TABLE "users" ADD COLUMN "booking_blocked_until" TIMESTAMP(3);

-- AlterTable
ALTER TABLE "payments" ADD COLUMN "provider" TEXT;
ALTER TABLE "payments" ADD COLUMN "checkout_url" TEXT;
