"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import { AdminShell } from "@/components/admin/AdminShell";
import { apiFetch } from "@/lib/api";
import { useAdminToken } from "@/lib/adminAuth";
import { useBeshSocket } from "@/lib/useBeshSocket";

type Daily = {
  date: string;
  count: number;
  revenue: number;
  confirmed: number;
  noShow: number;
};

type Overview = {
  metrics: {
    todayCount: number;
    yesterdayCount: number;
    todayRevenue: number;
    todayNoShow: number;
    todayPending: number;
    totalTables: number;
    availableTables: number;
  };
  daily: Daily[];
  statusBreakdown: { status: string; count: number }[];
  branches: {
    id: string;
    name: string;
    totalTables: number;
    availableTables: number;
  }[];
  rangeStart: string;
  rangeEnd: string;
};

type ReservationRow = {
  id: string;
  startAt: string;
  status: string;
  guestsCount?: number;
  branch: { name: string };
  user: { phone: string; name?: string | null };
  table: { number: string };
};

const isoDay = (d = new Date()) => d.toISOString().slice(0, 10);

export default function AdminDashboardPage() {
  return (
    <AdminShell
      title="Dashboard"
      subtitle="Bugungi ko'rsatkichlar va umumiy holat"
    >
      <DashboardBody />
    </AdminShell>
  );
}

function DashboardBody() {
  const { token } = useAdminToken();
  const [overview, setOverview] = useState<Overview | null>(null);
  const [today, setToday] = useState<ReservationRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [days, setDays] = useState(14);

  const load = useCallback(async () => {
    if (!token) return;
    setLoading(true);
    setErr(null);
    try {
      const [ov, todayRows] = await Promise.all([
        apiFetch<Overview>(`/admin/stats/overview?days=${days}`, { token }),
        apiFetch<ReservationRow[]>(`/admin/reservations?date=${isoDay()}`, { token }),
      ]);
      setOverview(ov);
      setToday(todayRows);
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setLoading(false);
    }
  }, [token, days]);

  useEffect(() => {
    void load();
  }, [load]);

  useBeshSocket({
    admin: Boolean(token),
    enabled: Boolean(token),
    onAdminRefresh: (p) => {
      if (p.scope === "reservations") void load();
    },
  });

  const m = overview?.metrics;
  const delta = useMemo(() => {
    if (!m) return null;
    const y = m.yesterdayCount;
    const t = m.todayCount;
    if (y === 0 && t === 0) return { pct: 0, sign: 0 };
    if (y === 0) return { pct: 100, sign: 1 };
    const pct = Math.round(((t - y) / y) * 100);
    return { pct: Math.abs(pct), sign: Math.sign(t - y) };
  }, [m]);

  return (
    <div className="space-y-5">
      {err ? (
        <div className="rounded-xl border border-[color:var(--danger)]/40 bg-red-50 px-3 py-2 text-sm text-red-700">
          {err}
        </div>
      ) : null}

      {/* Метрики */}
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <MetricCard
          title="Bugungi bronlar"
          value={m ? String(m.todayCount) : "…"}
          hint={
            delta && m
              ? `${delta.sign >= 0 ? "+" : "−"}${delta.pct}% kechaga nisbatan`
              : undefined
          }
          trend={delta?.sign ?? 0}
          tone="brand"
          icon={IconCalendar}
        />
        <MetricCard
          title="Bo'sh stollar"
          value={m ? String(m.availableTables) : "…"}
          hint={m ? `Jami ${m.totalTables} stol` : undefined}
          tone="success"
          icon={IconTables}
        />
        <MetricCard
          title="Bugungi daromad"
          value={m ? `${new Intl.NumberFormat("uz-UZ").format(m.todayRevenue)}` : "…"}
          hint="so'm · depozit"
          tone="info"
          icon={IconMoney}
        />
        <MetricCard
          title="No-show"
          value={m ? String(m.todayNoShow) : "…"}
          hint={m ? `${m.todayPending} ta tasdiqlash kerak` : undefined}
          tone="danger"
          icon={IconX}
        />
      </div>

      {/* Графики */}
      <div className="grid gap-4 lg:grid-cols-3">
        <div className="rounded-2xl border border-[color:var(--border)] bg-white p-5 shadow-[var(--shadow-sm)] lg:col-span-2">
          <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
            <div>
              <h2 className="text-base font-bold text-[color:var(--fg)]">
                Bronlar dinamikasi
              </h2>
              <p className="text-xs text-[color:var(--muted)]">
                So'nggi {days} kun · kunlik bronlar va daromad
              </p>
            </div>
            <div className="inline-flex gap-1 rounded-xl border border-[color:var(--border)] bg-[color:var(--bg-soft)] p-1 text-xs">
              {[7, 14, 30].map((d) => (
                <button
                  key={d}
                  type="button"
                  onClick={() => setDays(d)}
                  className={`rounded-lg px-3 py-1.5 font-semibold transition ${
                    days === d
                      ? "bg-[color:var(--brand)] text-white shadow"
                      : "text-[color:var(--muted)] hover:bg-white"
                  }`}
                >
                  {d} kun
                </button>
              ))}
            </div>
          </div>
          {overview ? (
            <BookingsChart daily={overview.daily} />
          ) : (
            <div className="h-56 animate-pulse rounded-xl bg-[color:var(--bg-soft)]" />
          )}
        </div>

        <div className="rounded-2xl border border-[color:var(--border)] bg-white p-5 shadow-[var(--shadow-sm)]">
          <h2 className="text-base font-bold text-[color:var(--fg)]">
            Holat bo'yicha taqsimot
          </h2>
          <p className="text-xs text-[color:var(--muted)]">
            So'nggi {days} kun · bronlar statusi
          </p>
          {overview ? (
            <StatusBreakdown items={overview.statusBreakdown} />
          ) : (
            <div className="mt-3 h-48 animate-pulse rounded-xl bg-[color:var(--bg-soft)]" />
          )}
        </div>
      </div>

      {/* Бронирования сегодня + филиалы */}
      <div className="grid gap-4 lg:grid-cols-3">
        <div className="rounded-2xl border border-[color:var(--border)] bg-white p-5 shadow-[var(--shadow-sm)] lg:col-span-2">
          <div className="mb-3 flex items-center justify-between">
            <div>
              <h2 className="text-base font-bold text-[color:var(--fg)]">
                Bugungi bronlar
              </h2>
              <p className="text-xs text-[color:var(--muted)]">
                Eng yaqin vaqtdagilari
              </p>
            </div>
            <Link
              href="/admin/bronlar"
              className="text-xs font-semibold text-[color:var(--brand)] hover:underline"
            >
              Barchasi →
            </Link>
          </div>

          {loading && !today.length ? (
            <SkeletonList />
          ) : !today.length ? (
            <EmptyState
              title="Bugun bronlar yo'q"
              description="Yangi bronlar kelganda bu yerda ko'rinadi."
            />
          ) : (
            <ul className="divide-y divide-[color:var(--border)]">
              {today
                .sort((a, b) => a.startAt.localeCompare(b.startAt))
                .slice(0, 8)
                .map((r) => (
                  <li key={r.id} className="flex items-center gap-3 py-2.5">
                    <div className="w-16 rounded-lg bg-[color:var(--bg-soft)] px-2 py-1.5 text-center">
                      <div className="text-xs font-bold text-[color:var(--fg)]">
                        {new Date(r.startAt).toLocaleTimeString("uz-UZ", {
                          hour: "2-digit",
                          minute: "2-digit",
                        })}
                      </div>
                    </div>
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center gap-2">
                        <span className="truncate text-sm font-semibold text-[color:var(--fg)]">
                          {r.user.name || r.user.phone}
                        </span>
                        <StatusPill status={r.status} />
                      </div>
                      <div className="text-xs text-[color:var(--muted)]">
                        {r.branch.name} · Stol {r.table.number}
                        {r.guestsCount ? ` · ${r.guestsCount} ta mehmon` : ""}
                      </div>
                    </div>
                  </li>
                ))}
            </ul>
          )}
        </div>

        <div className="space-y-4">
          <div className="rounded-2xl border border-[color:var(--border)] bg-white p-5 shadow-[var(--shadow-sm)]">
            <h2 className="text-base font-bold text-[color:var(--fg)]">Filiallar</h2>
            <p className="mb-3 text-xs text-[color:var(--muted)]">
              Bandlik darajasi hozir
            </p>
            {!overview ? (
              <div className="h-20 animate-pulse rounded-xl bg-[color:var(--bg-soft)]" />
            ) : (
              <ul className="space-y-2.5">
                {overview.branches.map((b) => {
                  const total = b.totalTables;
                  const free = b.availableTables;
                  const pct = total ? Math.round(((total - free) / total) * 100) : 0;
                  return (
                    <li key={b.id}>
                      <div className="flex items-center justify-between text-xs">
                        <span className="truncate font-semibold text-[color:var(--fg)]">
                          {b.name}
                        </span>
                        <span className="text-[color:var(--muted)]">
                          {free}/{total}
                        </span>
                      </div>
                      <div className="mt-1 h-1.5 overflow-hidden rounded-full bg-[color:var(--bg-soft)]">
                        <div
                          className="h-full rounded-full bg-[color:var(--brand)] transition-all"
                          style={{ width: `${pct}%` }}
                        />
                      </div>
                    </li>
                  );
                })}
              </ul>
            )}
          </div>

          <div className="rounded-2xl border border-[color:var(--border)] bg-white p-5 shadow-[var(--shadow-sm)]">
            <h2 className="text-base font-bold text-[color:var(--fg)]">
              Tezkor amallar
            </h2>
            <div className="mt-3 grid grid-cols-2 gap-2">
              <Link href="/admin/bronlar" className={quickCls}>
                <span className="text-lg">📋</span>
                <span className="text-xs font-bold text-[color:var(--fg)]">Bronlar</span>
              </Link>
              <Link href="/admin/zonalar" className={quickCls}>
                <span className="text-lg">🪑</span>
                <span className="text-xs font-bold text-[color:var(--fg)]">Zonalar</span>
              </Link>
              <Link href="/admin/filiallar" className={quickCls}>
                <span className="text-lg">🏢</span>
                <span className="text-xs font-bold text-[color:var(--fg)]">Filiallar</span>
              </Link>
              <Link href="/admin/hisobotlar" className={quickCls}>
                <span className="text-lg">📊</span>
                <span className="text-xs font-bold text-[color:var(--fg)]">Hisobotlar</span>
              </Link>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

const quickCls =
  "flex flex-col items-start gap-1 rounded-xl border border-[color:var(--border)] bg-[color:var(--bg-soft)] p-3 text-left transition hover:border-[color:var(--brand)] hover:bg-white";

// ---------- Chart: Bookings by day (grouped bars) ----------
function BookingsChart({ daily }: { daily: Daily[] }) {
  const max = Math.max(1, ...daily.map((d) => d.count));
  const maxRevenue = Math.max(1, ...daily.map((d) => d.revenue));
  const width = 640;
  const height = 220;
  const padX = 28;
  const padTop = 16;
  const padBottom = 32;
  const innerW = width - padX * 2;
  const innerH = height - padTop - padBottom;
  const step = daily.length > 1 ? innerW / (daily.length - 1) : innerW;

  const points = daily.map((d, i) => {
    const x = padX + i * step;
    const y = padTop + innerH - (d.count / max) * innerH;
    return { x, y, d };
  });

  const pathLine = points
    .map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(1)},${p.y.toFixed(1)}`)
    .join(" ");
  const pathArea = points.length
    ? `${pathLine} L${points[points.length - 1].x.toFixed(1)},${(padTop + innerH).toFixed(1)} L${points[0].x.toFixed(1)},${(padTop + innerH).toFixed(1)} Z`
    : "";

  const barW = daily.length ? Math.max(3, Math.min(18, (innerW / daily.length) * 0.55)) : 0;

  const fmtDate = (iso: string) => {
    const d = new Date(iso + "T00:00:00");
    return d.toLocaleDateString("uz-UZ", { day: "2-digit", month: "short" });
  };

  return (
    <div className="relative">
      <svg
        viewBox={`0 0 ${width} ${height}`}
        preserveAspectRatio="none"
        className="h-56 w-full"
      >
        <defs>
          <linearGradient id="bqArea" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#1b7a4e" stopOpacity="0.22" />
            <stop offset="100%" stopColor="#1b7a4e" stopOpacity="0" />
          </linearGradient>
        </defs>

        {/* Сетка (4 линии) */}
        {[0, 0.25, 0.5, 0.75, 1].map((t) => {
          const y = padTop + innerH - t * innerH;
          return (
            <line
              key={t}
              x1={padX}
              x2={width - padX}
              y1={y}
              y2={y}
              stroke="#e5e7eb"
              strokeDasharray="3 4"
              strokeWidth={1}
            />
          );
        })}

        {/* Столбцы доходов (фон) */}
        {daily.map((d, i) => {
          const x = padX + i * step - barW / 2;
          const h = (d.revenue / maxRevenue) * innerH;
          const y = padTop + innerH - h;
          return (
            <rect
              key={`r-${d.date}`}
              x={x}
              y={y}
              width={barW}
              height={h}
              rx={3}
              fill="#1b7a4e"
              opacity={0.12}
            />
          );
        })}

        {/* Областная заливка */}
        {pathArea ? <path d={pathArea} fill="url(#bqArea)" /> : null}

        {/* Линия */}
        {pathLine ? (
          <path d={pathLine} fill="none" stroke="#1b7a4e" strokeWidth={2.5} strokeLinejoin="round" strokeLinecap="round" />
        ) : null}

        {/* Точки */}
        {points.map((p) => (
          <g key={`p-${p.d.date}`}>
            <circle cx={p.x} cy={p.y} r={3.5} fill="#fff" stroke="#1b7a4e" strokeWidth={2} />
            <title>{`${fmtDate(p.d.date)}: ${p.d.count} bron, ${new Intl.NumberFormat("uz-UZ").format(p.d.revenue)} so'm`}</title>
          </g>
        ))}

        {/* Подписи дат (каждая N-я, ~6 меток) */}
        {daily.map((d, i) => {
          const skip = Math.max(1, Math.floor(daily.length / 6));
          if (i % skip !== 0 && i !== daily.length - 1) return null;
          const x = padX + i * step;
          return (
            <text
              key={`t-${d.date}`}
              x={x}
              y={height - 10}
              textAnchor="middle"
              fontSize="10"
              fill="#6b7280"
            >
              {fmtDate(d.date)}
            </text>
          );
        })}
      </svg>

      {/* Легенда */}
      <div className="mt-1 flex items-center gap-4 text-[11px] text-[color:var(--muted)]">
        <span className="inline-flex items-center gap-1.5">
          <span className="h-2 w-5 rounded bg-[#1b7a4e]" /> Bronlar soni
        </span>
        <span className="inline-flex items-center gap-1.5">
          <span className="h-2 w-5 rounded bg-[#1b7a4e]/30" /> Daromad
        </span>
      </div>
    </div>
  );
}

// ---------- Status breakdown ----------
function StatusBreakdown({ items }: { items: { status: string; count: number }[] }) {
  const total = items.reduce((s, x) => s + x.count, 0);

  const palette: Record<string, string> = {
    PENDING_PAYMENT: "#f59e0b",
    CONFIRMED: "#1b7a4e",
    COMPLETED: "#2563eb",
    NO_SHOW: "#dc2626",
    CANCELLED_BY_USER: "#9ca3af",
    CANCELLED_BY_RESTAURANT: "#6b7280",
  };
  const labels: Record<string, string> = {
    PENDING_PAYMENT: "To'lov kutilmoqda",
    CONFIRMED: "Tasdiqlangan",
    COMPLETED: "Keldi",
    NO_SHOW: "No-show",
    CANCELLED_BY_USER: "Mijoz bekor",
    CANCELLED_BY_RESTAURANT: "Restoran bekor",
  };

  if (!total) {
    return (
      <div className="mt-4 flex h-48 items-center justify-center rounded-xl border border-dashed border-[color:var(--border)] text-xs text-[color:var(--muted)]">
        Ma'lumot yo'q
      </div>
    );
  }

  return (
    <div className="mt-4 space-y-3">
      {/* Горизонтальная стек-лента */}
      <div className="flex h-3 w-full overflow-hidden rounded-full bg-[color:var(--bg-soft)]">
        {items.map((x) => {
          const pct = (x.count / total) * 100;
          return (
            <div
              key={x.status}
              style={{ width: `${pct}%`, background: palette[x.status] ?? "#9ca3af" }}
              title={`${labels[x.status] ?? x.status}: ${x.count}`}
            />
          );
        })}
      </div>
      {/* Список */}
      <ul className="space-y-1.5">
        {items
          .slice()
          .sort((a, b) => b.count - a.count)
          .map((x) => {
            const pct = Math.round((x.count / total) * 100);
            return (
              <li key={x.status} className="flex items-center gap-2 text-xs">
                <span
                  className="inline-block h-2.5 w-2.5 rounded-full"
                  style={{ background: palette[x.status] ?? "#9ca3af" }}
                />
                <span className="flex-1 font-semibold text-[color:var(--fg)]">
                  {labels[x.status] ?? x.status}
                </span>
                <span className="text-[color:var(--muted)]">
                  {x.count}{" "}
                  <span className="text-[color:var(--muted-2)]">· {pct}%</span>
                </span>
              </li>
            );
          })}
      </ul>
    </div>
  );
}

// ---------- UI primitives ----------
function MetricCard({
  title,
  value,
  hint,
  icon,
  tone,
  trend,
}: {
  title: string;
  value: string;
  hint?: string;
  icon: React.ReactNode;
  tone: "brand" | "success" | "info" | "danger";
  trend?: number;
}) {
  const toneMap: Record<typeof tone, { bg: string; fg: string }> = {
    brand: { bg: "#e6f2ec", fg: "#1b7a4e" },
    success: { bg: "#e8f5ee", fg: "#15803d" },
    info: { bg: "#e8effb", fg: "#1d4ed8" },
    danger: { bg: "#fdecec", fg: "#b91c1c" },
  };
  const t = toneMap[tone];
  return (
    <div className="rounded-2xl border border-[color:var(--border)] bg-white p-5 shadow-[var(--shadow-sm)]">
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="text-[11px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
            {title}
          </div>
          <div className="mt-1 text-2xl font-extrabold text-[color:var(--fg)]">
            {value}
          </div>
          {hint ? (
            <div
              className="mt-0.5 flex items-center gap-1 text-xs"
              style={{
                color:
                  trend === undefined || trend === 0
                    ? "var(--muted)"
                    : trend > 0
                      ? "#15803d"
                      : "#b91c1c",
              }}
            >
              {trend !== undefined && trend !== 0 ? (
                <svg
                  width="10"
                  height="10"
                  viewBox="0 0 10 10"
                  fill="currentColor"
                  style={{ transform: trend < 0 ? "rotate(180deg)" : undefined }}
                >
                  <path d="M5 1l4 5H1z" />
                </svg>
              ) : null}
              {hint}
            </div>
          ) : null}
        </div>
        <span
          className="inline-flex h-10 w-10 shrink-0 items-center justify-center rounded-xl"
          style={{ background: t.bg, color: t.fg }}
        >
          {icon}
        </span>
      </div>
    </div>
  );
}

function StatusPill({ status }: { status: string }) {
  const map: Record<string, { bg: string; fg: string; label: string }> = {
    PENDING_PAYMENT: { bg: "#fef3c7", fg: "#a16207", label: "To'lov" },
    CONFIRMED: { bg: "#e8f5ee", fg: "#15803d", label: "Tasdiqlangan" },
    COMPLETED: { bg: "#e8effb", fg: "#1d4ed8", label: "Keldi" },
    NO_SHOW: { bg: "#fdecec", fg: "#b91c1c", label: "No-show" },
    CANCELLED_BY_USER: { bg: "#f3f4f6", fg: "#4b5563", label: "Bekor" },
    CANCELLED_BY_RESTAURANT: { bg: "#f3f4f6", fg: "#4b5563", label: "Bekor" },
  };
  const mm = map[status] || { bg: "#f3f4f6", fg: "#4b5563", label: status };
  return (
    <span
      className="inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-bold"
      style={{ background: mm.bg, color: mm.fg }}
    >
      {mm.label}
    </span>
  );
}

function SkeletonList() {
  return (
    <div className="space-y-2">
      {[0, 1, 2, 3].map((i) => (
        <div key={i} className="h-12 animate-pulse rounded-xl bg-[color:var(--bg-soft)]" />
      ))}
    </div>
  );
}

function EmptyState({ title, description }: { title: string; description: string }) {
  return (
    <div className="flex flex-col items-center justify-center gap-1 py-10 text-center">
      <div className="text-3xl">📭</div>
      <div className="text-sm font-bold text-[color:var(--fg)]">{title}</div>
      <div className="text-xs text-[color:var(--muted)]">{description}</div>
    </div>
  );
}

// ---------- Icons ----------
const IconCalendar = (
  <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <rect x="3" y="5" width="18" height="16" rx="2" />
    <path d="M3 9h18M8 3v4M16 3v4" />
  </svg>
);
const IconTables = (
  <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <rect x="3" y="7" width="8" height="8" rx="1" />
    <rect x="13" y="4" width="8" height="5" rx="1" />
    <rect x="13" y="13" width="8" height="8" rx="1" />
  </svg>
);
const IconMoney = (
  <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M12 2v20M17 5H9a3 3 0 0 0 0 6h6a3 3 0 0 1 0 6H7" />
  </svg>
);
const IconX = (
  <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="12" cy="12" r="10" />
    <path d="M15 9l-6 6M9 9l6 6" />
  </svg>
);
