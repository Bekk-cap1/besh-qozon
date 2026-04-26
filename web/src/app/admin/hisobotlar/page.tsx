"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { AdminShell } from "@/components/admin/AdminShell";
import { apiFetch } from "@/lib/api";
import { useAdminToken } from "@/lib/adminAuth";

type Branch = { id: string; name: string };
type Report = {
  range: { from: string; to: string; days: number };
  summary: {
    total: number;
    completed: number;
    confirmed: number;
    noShow: number;
    cancelled: number;
    noShowPct: number;
    revenue: number;
    preorderRevenue: number;
    avgCheck: number;
  };
  topDishes: Array<{ menuItemId: string; name: string; quantity: number; revenue: number }>;
  hourly: Array<{ hour: number; count: number }>;
  byBranch: Array<{ branchId: string; name: string; count: number; revenue: number }>;
};

const DAY_OPTIONS = [7, 14, 30, 60, 90];

export default function AdminHisobotlarPage() {
  return (
    <AdminShell title="Hisobotlar" subtitle="Daromad, top taomlar, no-show va band soatlar">
      <Body />
    </AdminShell>
  );
}

function Body() {
  const { token } = useAdminToken();
  const [branches, setBranches] = useState<Branch[]>([]);
  const [branchId, setBranchId] = useState("");
  const [days, setDays] = useState(30);
  const [data, setData] = useState<Report | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    apiFetch<Branch[]>("/branches").then(setBranches).catch(() => {});
  }, []);

  const load = useCallback(async () => {
    if (!token) return;
    setLoading(true);
    setErr(null);
    try {
      const params = new URLSearchParams();
      params.set("days", String(days));
      if (branchId) params.set("branchId", branchId);
      const d = await apiFetch<Report>(`/admin/stats/reports?${params}`, { token });
      setData(d);
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setLoading(false);
    }
  }, [token, days, branchId]);

  useEffect(() => {
    void load();
  }, [load]);

  const maxHourly = useMemo(
    () => (data ? Math.max(1, ...data.hourly.map((h) => h.count)) : 1),
    [data],
  );

  return (
    <div className="space-y-4">
      {/* Filters */}
      <div className="flex flex-wrap items-end gap-3 rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)]">
        <div className="min-w-[200px] flex-1">
          <label className="mb-1 block text-[10px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
            Filial
          </label>
          <select
            value={branchId}
            onChange={(e) => setBranchId(e.target.value)}
            className="w-full rounded-xl border border-[color:var(--border)] bg-white px-3 py-2 text-sm"
          >
            <option value="">Barcha filiallar</option>
            {branches.map((b) => (
              <option key={b.id} value={b.id}>
                {b.name}
              </option>
            ))}
          </select>
        </div>
        <div>
          <label className="mb-1 block text-[10px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
            Davr
          </label>
          <div className="inline-flex overflow-hidden rounded-xl border border-[color:var(--border)]">
            {DAY_OPTIONS.map((d) => (
              <button
                key={d}
                type="button"
                onClick={() => setDays(d)}
                className={`px-3 py-2 text-xs font-semibold transition ${
                  days === d
                    ? "bg-[color:var(--brand)] text-white"
                    : "bg-white text-[color:var(--fg)] hover:bg-[color:var(--bg-soft)]"
                }`}
              >
                {d}k
              </button>
            ))}
          </div>
        </div>
      </div>

      {err ? (
        <div className="rounded-xl border border-rose-200 bg-rose-50 p-3 text-sm text-rose-700">
          {err}
        </div>
      ) : null}

      {loading && !data ? (
        <div className="rounded-2xl border border-[color:var(--border)] bg-white p-8 text-center text-sm text-[color:var(--muted)]">
          Yuklanmoqda…
        </div>
      ) : data ? (
        <>
          {/* KPIs */}
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <Kpi
              label="Daromad"
              value={formatMoney(data.summary.revenue + data.summary.preorderRevenue)}
              suffix="so'm"
              sub={`Depozit: ${formatMoney(data.summary.revenue)} · Ovqat: ${formatMoney(
                data.summary.preorderRevenue,
              )}`}
              tone="brand"
            />
            <Kpi
              label="O'rtacha chek"
              value={formatMoney(data.summary.avgCheck)}
              suffix="so'm"
              sub={`${data.summary.completed + data.summary.confirmed} bron bo'yicha`}
            />
            <Kpi
              label="Bronlar"
              value={String(data.summary.total)}
              sub={`${data.summary.completed} keldi · ${data.summary.cancelled} bekor`}
            />
            <Kpi
              label="No-show"
              value={`${data.summary.noShowPct}%`}
              sub={`${data.summary.noShow} ta kelmadi`}
              tone={data.summary.noShowPct > 10 ? "danger" : "neutral"}
            />
          </div>

          <div className="grid gap-4 lg:grid-cols-2">
            {/* Top dishes */}
            <div className="rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)]">
              <div className="flex items-center justify-between">
                <h3 className="text-sm font-extrabold text-[color:var(--fg)]">
                  Top taomlar
                </h3>
                <span className="text-xs text-[color:var(--muted)]">
                  {data.topDishes.length} ta
                </span>
              </div>
              {data.topDishes.length === 0 ? (
                <p className="mt-4 text-center text-xs text-[color:var(--muted)]">
                  Davr bo&apos;yicha oldindan buyurtmalar yo&apos;q
                </p>
              ) : (
                <div className="mt-3 space-y-2">
                  {data.topDishes.map((d, i) => {
                    const max = data.topDishes[0].quantity;
                    const pct = Math.round((d.quantity / max) * 100);
                    return (
                      <div key={d.menuItemId}>
                        <div className="flex items-center justify-between text-xs">
                          <span className="font-semibold text-[color:var(--fg)]">
                            <span className="mr-1 inline-flex h-5 w-5 items-center justify-center rounded-full bg-[color:var(--bg-soft)] text-[10px] font-bold text-[color:var(--muted)]">
                              {i + 1}
                            </span>
                            {d.name}
                          </span>
                          <span className="tabular-nums text-[color:var(--muted)]">
                            {d.quantity}× · {formatMoney(d.revenue)} so&apos;m
                          </span>
                        </div>
                        <div className="mt-1 h-2 overflow-hidden rounded-full bg-[color:var(--bg-soft)]">
                          <div
                            className="h-full rounded-full bg-[color:var(--brand)]"
                            style={{ width: `${pct}%` }}
                          />
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>

            {/* Hourly */}
            <div className="rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)]">
              <h3 className="text-sm font-extrabold text-[color:var(--fg)]">
                Soatlik bandlik
              </h3>
              <div className="mt-3 flex h-40 items-end justify-between gap-0.5">
                {data.hourly.map((h) => {
                  const hh = Math.round((h.count / maxHourly) * 100);
                  return (
                    <div key={h.hour} className="group relative flex-1">
                      <div
                        className="w-full rounded-t bg-[color:var(--brand)]/80 transition group-hover:bg-[color:var(--brand)]"
                        style={{ height: `${hh}%`, minHeight: h.count > 0 ? 3 : 0 }}
                        title={`${h.hour}:00 — ${h.count}`}
                      />
                    </div>
                  );
                })}
              </div>
              <div className="mt-1 flex justify-between text-[9px] text-[color:var(--muted)]">
                <span>00</span>
                <span>06</span>
                <span>12</span>
                <span>18</span>
                <span>24</span>
              </div>
            </div>
          </div>

          {/* By branches */}
          <div className="rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)]">
            <h3 className="text-sm font-extrabold text-[color:var(--fg)]">Filial bo&apos;yicha</h3>
            <div className="mt-3 overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead>
                  <tr className="text-left text-[10px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
                    <th className="py-2">Filial</th>
                    <th className="py-2 text-right">Bronlar</th>
                    <th className="py-2 text-right">Daromad</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-[color:var(--border)]">
                  {data.byBranch.map((b) => (
                    <tr key={b.branchId}>
                      <td className="py-2 font-semibold text-[color:var(--fg)]">{b.name}</td>
                      <td className="py-2 text-right tabular-nums">{b.count}</td>
                      <td className="py-2 text-right tabular-nums text-[color:var(--brand-700)]">
                        {formatMoney(b.revenue)} so&apos;m
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="text-center text-xs text-[color:var(--muted)]">
            {data.range.from} — {data.range.to} · {data.range.days} kun
          </div>
        </>
      ) : null}
    </div>
  );
}

function Kpi({
  label,
  value,
  suffix,
  sub,
  tone = "neutral",
}: {
  label: string;
  value: string;
  suffix?: string;
  sub?: string;
  tone?: "brand" | "danger" | "neutral";
}) {
  const toneCls =
    tone === "brand"
      ? "border-[color:var(--brand)] bg-[color:var(--brand)]/5"
      : tone === "danger"
        ? "border-rose-200 bg-rose-50"
        : "border-[color:var(--border)] bg-white";
  return (
    <div className={`rounded-2xl border p-4 shadow-[var(--shadow-sm)] ${toneCls}`}>
      <div className="text-[10px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
        {label}
      </div>
      <div className="mt-1 text-2xl font-extrabold text-[color:var(--fg)]">
        {value}
        {suffix ? (
          <span className="ml-1 text-xs font-normal text-[color:var(--muted)]">{suffix}</span>
        ) : null}
      </div>
      {sub ? <div className="mt-0.5 text-[11px] text-[color:var(--muted)]">{sub}</div> : null}
    </div>
  );
}

function formatMoney(n: number): string {
  return Math.round(n).toLocaleString("ru-RU");
}
