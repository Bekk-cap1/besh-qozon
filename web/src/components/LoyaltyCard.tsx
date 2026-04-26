"use client";

import type { LoyaltyInfo } from "@/context/AuthContext";

type Props = {
  loyalty: LoyaltyInfo;
  className?: string;
};

export function LoyaltyCard({ loyalty, className }: Props) {
  const { threshold, inCycle, toNext, bonuses, bonusValueUzs } = loyalty;
  const currentStep = inCycle === 0 && loyalty.completed > 0 ? threshold : inCycle;
  const progress = Math.min(1, currentStep / threshold);

  return (
    <div
      className={[
        "overflow-hidden rounded-2xl border-2 border-[color:var(--brand-100)] bg-gradient-to-br from-[color:var(--brand-50)] via-white to-[color:var(--brand-50)] p-5 shadow-sm",
        className ?? "",
      ].join(" ")}
    >
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-3">
          <span className="flex h-12 w-12 items-center justify-center rounded-2xl bg-[color:var(--brand)] text-2xl">
            🎁
          </span>
          <div>
            <div className="text-[10px] font-bold uppercase tracking-wider text-[color:var(--brand-700)]">
              Sodiqlik dasturi
            </div>
            <div className="text-lg font-extrabold text-[color:var(--fg)]">
              {bonuses > 0 ? (
                <>
                  Sizda <span className="text-[color:var(--brand-700)]">{bonuses} ta bonus</span>!
                </>
              ) : toNext === 0 ? (
                "Keyingi bron — bonus!"
              ) : (
                <>
                  Bonusgacha: <span className="text-[color:var(--brand-700)]">{toNext} ta bron</span>
                </>
              )}
            </div>
          </div>
        </div>
        {bonuses > 0 ? (
          <div className="rounded-xl bg-[color:var(--brand)] px-3 py-2 text-right text-white shadow-[0_4px_12px_rgba(27,122,78,0.25)]">
            <div className="text-[10px] uppercase tracking-wider opacity-80">Har biri</div>
            <div className="text-sm font-extrabold">
              {bonusValueUzs.toLocaleString("ru-RU")} so&apos;m
            </div>
          </div>
        ) : null}
      </div>

      {/* Dots progress: 1..threshold */}
      <div className="mt-4 flex items-center justify-between gap-2">
        {Array.from({ length: threshold }).map((_, i) => {
          const filled = i < currentStep;
          const isBonus = i === threshold - 1;
          return (
            <div
              key={i}
              className="flex flex-col items-center gap-1"
              style={{ flex: 1 }}
            >
              <span
                className={[
                  "relative flex h-8 w-8 items-center justify-center rounded-full border-2 text-xs font-extrabold transition-all",
                  filled
                    ? isBonus
                      ? "border-[color:var(--brand)] bg-[color:var(--brand)] text-white shadow-[0_0_0_4px_rgba(27,122,78,0.2)]"
                      : "border-[color:var(--brand)] bg-[color:var(--brand)] text-white"
                    : "border-[color:var(--brand-100)] bg-white text-[color:var(--muted-2)]",
                ].join(" ")}
              >
                {isBonus ? "🎁" : i + 1}
              </span>
              {i < threshold - 1 ? null : null}
            </div>
          );
        })}
      </div>
      <div className="relative mt-1 h-1.5 overflow-hidden rounded-full bg-[color:var(--brand-100)]">
        <div
          className="h-full rounded-full bg-[color:var(--brand)] transition-[width] duration-500"
          style={{ width: `${progress * 100}%` }}
        />
      </div>
      <div className="mt-2 text-xs text-[color:var(--muted)]">
        Har <b>{threshold}</b> ta yakunlangan bron uchun —{" "}
        <b>{bonusValueUzs.toLocaleString("ru-RU")} so&apos;m</b> bonus. Keyingi bronda depozit
        o&apos;rniga ishlatasiz.
      </div>
    </div>
  );
}

export function LoyaltyBonusToggle({
  loyalty,
  active,
  onToggle,
  disabled,
}: {
  loyalty: LoyaltyInfo;
  active: boolean;
  onToggle: (next: boolean) => void;
  disabled?: boolean;
}) {
  if (loyalty.bonuses <= 0) return null;
  return (
    <label
      className={[
        "flex cursor-pointer items-center justify-between gap-3 rounded-2xl border-2 p-4 transition",
        active
          ? "border-[color:var(--brand)] bg-[color:var(--brand-50)]"
          : "border-[color:var(--border)] bg-white hover:border-[color:var(--brand-100)]",
        disabled ? "cursor-not-allowed opacity-60" : "",
      ].join(" ")}
    >
      <div className="flex items-center gap-3">
        <span className="flex h-10 w-10 items-center justify-center rounded-xl bg-[color:var(--brand)] text-xl text-white">
          🎁
        </span>
        <div>
          <div className="text-sm font-extrabold text-[color:var(--fg)]">
            Bonus qo&apos;llash ({loyalty.bonuses} ta bor)
          </div>
          <div className="text-xs text-[color:var(--muted)]">
            Depozit 0 so&apos;m · Bron avtomatik tasdiqlanadi
          </div>
        </div>
      </div>
      <input
        type="checkbox"
        checked={active}
        onChange={(e) => onToggle(e.target.checked)}
        disabled={disabled}
        className="h-5 w-5 accent-[color:var(--brand)]"
      />
    </label>
  );
}
