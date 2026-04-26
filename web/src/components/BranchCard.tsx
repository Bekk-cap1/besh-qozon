"use client";

import type { MouseEventHandler } from "react";

export type BranchCardData = {
  id: string;
  name: string;
  address?: string | null;
  workHours?: unknown; // Json — может быть "10:00-23:00" или объектом
  phone?: string | null;
  lat?: number | null;
  lng?: number | null;
  _count?: { zones?: number };
  totalTables?: number;
  availableTables?: number;
  averageRating?: number | null;
  reviewsCount?: number;
};

type Variant = "select" | "link";

type Props = {
  branch: BranchCardData;
  /** index нужен для детерминированной цветовой палитры обложки */
  index?: number;
  /** выбранная карточка подсвечивается зелёной рамкой и галочкой */
  selected?: boolean;
  /** вариант отрисовки:
   *  - "select": <button> для шага бронирования (без перехода)
   *  - "link":   <a> с переходом на /bron?branch=... */
  variant?: Variant;
  href?: string;
  onClick?: MouseEventHandler<HTMLButtonElement | HTMLAnchorElement>;
  /** короткий ориентир/метро под адресом */
  orientation?: string;
  /** отображать ли бейдж live-доступности (если totalTables > 0) */
  showAvailability?: boolean;
  /** мини-размер для плотных списков */
  compact?: boolean;
};

const PALETTES: Array<[string, string]> = [
  ["#1b7a4e", "#0c3a24"],
  ["#d97706", "#78350f"],
  ["#0891b2", "#0e3a46"],
  ["#9333ea", "#4c1d95"],
  ["#ea580c", "#7c2d12"],
  ["#0f766e", "#134e4a"],
];

/** Детерминированный рейтинг 4.5–4.9 по id (пока нет реального) */
function fakeRating(id: string): string {
  let h = 0;
  for (let i = 0; i < id.length; i++) h = (h * 31 + id.charCodeAt(i)) | 0;
  const n = Math.abs(h) % 5; // 0..4
  return (4.5 + n * 0.1).toFixed(1);
}

function workHoursLabel(wh: unknown): string {
  if (!wh) return "10:00 – 23:00";
  if (typeof wh === "string") return wh;
  if (typeof wh === "object" && wh !== null) {
    const o = wh as { open?: string; close?: string; from?: string; to?: string };
    const from = o.open ?? o.from;
    const to = o.close ?? o.to;
    if (from && to) return `${from} – ${to}`;
  }
  return "10:00 – 23:00";
}

function AvailabilityBadge({ available, total }: { available: number; total: number }) {
  // Цветовая шкала: >= 50% — зелёный, 20–50% — тёплый, <20% — красный
  const ratio = total > 0 ? available / total : 0;
  let bg = "#dcfce7";
  let fg = "#166534";
  let dot = "#22c55e";
  let label = `${available} ta bo'sh stol`;
  if (ratio < 0.2) {
    bg = "#fee2e2";
    fg = "#991b1b";
    dot = "#ef4444";
    if (available === 0) label = "Bo'sh stol yo'q";
  } else if (ratio < 0.5) {
    bg = "#fef3c7";
    fg = "#92400e";
    dot = "#f59e0b";
  }
  return (
    <span
      className="inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-[11px] font-bold"
      style={{ background: bg, color: fg }}
    >
      <span className="h-1.5 w-1.5 rounded-full" style={{ background: dot }} />
      {label}
    </span>
  );
}

function RatingPill({ value, count }: { value: string; count?: number }) {
  return (
    <span className="inline-flex items-center gap-1 rounded-full bg-white/95 px-2.5 py-1 text-[11px] font-bold text-[color:var(--fg)] shadow-sm">
      <svg width="11" height="11" viewBox="0 0 24 24" fill="#f59e0b" aria-hidden>
        <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z" />
      </svg>
      {value}
      {count !== undefined && count > 0 ? (
        <span className="font-normal text-[color:var(--muted)]">({count})</span>
      ) : null}
    </span>
  );
}

function Checkmark() {
  return (
    <span
      aria-hidden
      className="inline-flex h-7 w-7 items-center justify-center rounded-full bg-[color:var(--brand)] text-white shadow-[0_4px_12px_rgba(27,122,78,0.45)]"
    >
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
        <path d="M5 12l5 5L20 7" />
      </svg>
    </span>
  );
}

export function BranchCard({
  branch,
  index = 0,
  selected = false,
  variant = "select",
  href,
  onClick,
  orientation,
  showAvailability = true,
  compact = false,
}: Props) {
  const [from, to] = PALETTES[index % PALETTES.length];
  const realRating = branch.averageRating != null && branch.reviewsCount && branch.reviewsCount > 0
    ? branch.averageRating.toFixed(1)
    : null;
  const rating = realRating ?? fakeRating(branch.id);
  const reviewsCount = realRating ? branch.reviewsCount : undefined;
  const zonesCount = branch._count?.zones ?? 0;
  const hasAvailability = showAvailability && (branch.totalTables ?? 0) > 0;
  const coverHeight = compact ? "aspect-[5/3]" : "aspect-[4/3]";

  const coverBg = `linear-gradient(135deg, ${from} 0%, ${to} 100%)`;

  const Cover = (
    <div className={`relative w-full ${coverHeight}`} style={{ background: coverBg }}>
      {/* soft highlights */}
      <div
        aria-hidden
        className="absolute inset-0 opacity-40"
        style={{
          backgroundImage:
            "radial-gradient(circle at 20% 20%, rgba(255,255,255,0.40), transparent 45%), radial-gradient(circle at 80% 85%, rgba(255,255,255,0.25), transparent 40%)",
        }}
      />
      {/* top-left: rating */}
      <div className="absolute left-3 top-3 flex items-center gap-2">
        <RatingPill value={rating} count={reviewsCount} />
      </div>
      {/* top-right: live availability */}
      <div className="absolute right-3 top-3">
        {hasAvailability ? (
          <AvailabilityBadge
            available={branch.availableTables ?? 0}
            total={branch.totalTables ?? 0}
          />
        ) : (
          <span className="inline-flex items-center gap-1.5 rounded-full bg-white/95 px-2.5 py-1 text-[11px] font-bold text-[color:var(--brand-700)] shadow-sm">
            <span className="h-1.5 w-1.5 rounded-full bg-[color:var(--brand)]" />
            Bron mavjud
          </span>
        )}
      </div>
      {/* selected tick */}
      {selected ? (
        <div className="absolute bottom-3 right-3">
          <Checkmark />
        </div>
      ) : null}
      {/* bottom: name + address */}
      <div className="absolute inset-x-3 bottom-3">
        <div className="text-[17px] font-extrabold leading-tight text-white drop-shadow-sm">
          {branch.name}
        </div>
        {branch.address ? (
          <div className="mt-0.5 line-clamp-1 text-xs text-white/90 drop-shadow-sm">
            {branch.address}
          </div>
        ) : null}
      </div>
    </div>
  );

  const Body = (
    <div className="grid gap-1.5 p-4 text-sm">
      <div className="flex items-center gap-2 text-[color:var(--muted)]">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" aria-hidden>
          <circle cx="12" cy="12" r="9" />
          <path d="M12 7v5l3 2" />
        </svg>
        <span>{workHoursLabel(branch.workHours)}</span>
        <span className="mx-1 text-[color:var(--muted-2)]">·</span>
        <span>{zonesCount} zona</span>
      </div>
      {orientation ? (
        <div className="flex items-center gap-2 text-[color:var(--muted)]">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" aria-hidden>
            <path d="M12 21s-7-6.5-7-12a7 7 0 1 1 14 0c0 5.5-7 12-7 12z" />
            <circle cx="12" cy="9" r="2.5" />
          </svg>
          <span className="line-clamp-1">{orientation}</span>
        </div>
      ) : null}
    </div>
  );

  const baseCls = [
    "bq-card overflow-hidden text-left transition",
    "bq-card-hover",
    selected
      ? "ring-2 ring-[color:var(--brand)] border-[color:var(--brand)] shadow-[0_10px_30px_rgba(27,122,78,0.18)]"
      : "",
  ].join(" ");

  if (variant === "link") {
    return (
      <a href={href ?? `/bron?branch=${branch.id}`} onClick={onClick as MouseEventHandler<HTMLAnchorElement>} className={baseCls}>
        {Cover}
        {Body}
      </a>
    );
  }

  return (
    <button
      type="button"
      onClick={onClick as MouseEventHandler<HTMLButtonElement>}
      aria-pressed={selected}
      className={`${baseCls} w-full`}
    >
      {Cover}
      {Body}
    </button>
  );
}
