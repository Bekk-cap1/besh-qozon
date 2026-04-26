"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { AdminShell } from "@/components/admin/AdminShell";
import { apiFetch, apiUrl } from "@/lib/api";
import { useAdminToken } from "@/lib/adminAuth";
import { useBeshSocket } from "@/lib/useBeshSocket";

type Branch = { id: string; name: string };
type Reservation = {
  id: string;
  startAt: string;
  status: string;
  guestsCount?: number;
  depositAmount?: number | string | null;
  branchId: string;
  branch: { id: string; name: string };
  user: { id: string; phone: string; name?: string | null };
  table: { id: string; number: string };
};

const STATUSES = [
  "PENDING_PAYMENT",
  "CONFIRMED",
  "COMPLETED",
  "NO_SHOW",
  "CANCELLED_BY_USER",
  "CANCELLED_BY_RESTAURANT",
] as const;

const STATUS_LABELS: Record<string, string> = {
  PENDING_PAYMENT: "To'lov kutilmoqda",
  CONFIRMED: "Tasdiqlangan",
  COMPLETED: "Keldi",
  NO_SHOW: "No-show",
  CANCELLED_BY_USER: "Mijoz bekor qildi",
  CANCELLED_BY_RESTAURANT: "Restoran bekor qildi",
};

const isoDay = (d = new Date()) => d.toISOString().slice(0, 10);

export default function AdminBronlarPage() {
  return (
    <AdminShell title="Bronlar" subtitle="Barcha bronlar ro'yxati, filtrlash va boshqaruv">
      <ReservationsTable />
    </AdminShell>
  );
}

function ReservationsTable() {
  const { token } = useAdminToken();
  const [branches, setBranches] = useState<Branch[]>([]);
  const [rows, setRows] = useState<Reservation[]>([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);

  const [branchId, setBranchId] = useState<string>("");
  const [date, setDate] = useState<string>(isoDay());
  const [statusFilter, setStatusFilter] = useState<string>("ALL");
  const [q, setQ] = useState("");

  const loadBranches = useCallback(async () => {
    try {
      const b = await apiFetch<Branch[]>("/branches");
      setBranches(b);
    } catch (e) {
      setErr((e as Error).message);
    }
  }, []);

  const load = useCallback(async () => {
    if (!token) return;
    setLoading(true);
    setErr(null);
    try {
      const params = new URLSearchParams();
      if (branchId) params.set("branchId", branchId);
      if (date) params.set("date", date);
      const url = `/admin/reservations${params.toString() ? `?${params}` : ""}`;
      const data = await apiFetch<Reservation[]>(url, { token });
      setRows(data);
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setLoading(false);
    }
  }, [token, branchId, date]);

  useEffect(() => {
    void loadBranches();
  }, [loadBranches]);

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

  const filtered = useMemo(() => {
    const needle = q.trim().toLowerCase();
    return rows.filter((r) => {
      if (statusFilter !== "ALL" && r.status !== statusFilter) return false;
      if (!needle) return true;
      return (
        r.user.phone?.toLowerCase().includes(needle) ||
        r.user.name?.toLowerCase().includes(needle) ||
        r.table.number?.toLowerCase().includes(needle) ||
        r.branch.name?.toLowerCase().includes(needle)
      );
    });
  }, [rows, q, statusFilter]);

  const setStatus = async (id: string, status: (typeof STATUSES)[number]) => {
    if (!token) return;
    const confirmMsg = askConfirm(status);
    if (confirmMsg && !confirm(confirmMsg)) return;
    setBusy(id);
    try {
      await apiFetch(`/admin/reservations/${id}`, {
        method: "PATCH",
        token,
        body: JSON.stringify({ status }),
      });
      await load();
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="space-y-4">
      {/* Панель фильтров */}
      <div className="flex flex-wrap items-end gap-3 rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)]">
        <div className="min-w-[120px] flex-1">
          <label className="mb-1 block text-[11px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
            Sana
          </label>
          <input
            type="date"
            className="bq-input h-10"
            value={date}
            onChange={(e) => setDate(e.target.value)}
          />
        </div>
        <div className="min-w-[160px] flex-1">
          <label className="mb-1 block text-[11px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
            Filial
          </label>
          <select
            className="bq-input h-10"
            value={branchId}
            onChange={(e) => setBranchId(e.target.value)}
          >
            <option value="">Barcha filiallar</option>
            {branches.map((b) => (
              <option key={b.id} value={b.id}>
                {b.name}
              </option>
            ))}
          </select>
        </div>
        <div className="min-w-[160px] flex-1">
          <label className="mb-1 block text-[11px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
            Holat
          </label>
          <select
            className="bq-input h-10"
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
          >
            <option value="ALL">Barchasi</option>
            {STATUSES.map((s) => (
              <option key={s} value={s}>
                {STATUS_LABELS[s]}
              </option>
            ))}
          </select>
        </div>
        <div className="min-w-[220px] flex-[2]">
          <label className="mb-1 block text-[11px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
            Qidirish
          </label>
          <div className="relative">
            <input
              className="bq-input h-10 pl-9"
              placeholder="Telefon, ism, stol, filial…"
              value={q}
              onChange={(e) => setQ(e.target.value)}
            />
            <svg
              width="16"
              height="16"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-[color:var(--muted)]"
            >
              <circle cx="11" cy="11" r="7" />
              <path d="M21 21l-4.3-4.3" />
            </svg>
          </div>
        </div>
        <button
          type="button"
          onClick={() => {
            setBranchId("");
            setDate(isoDay());
            setStatusFilter("ALL");
            setQ("");
          }}
          className="bq-btn bq-btn-ghost h-10 text-sm"
        >
          Tozalash
        </button>
        <button
          type="button"
          onClick={async () => {
            if (!token) return;
            const params = new URLSearchParams();
            if (branchId) params.set("branchId", branchId);
            if (date) {
              params.set("from", date);
              params.set("to", date);
            }
            if (statusFilter !== "ALL") params.set("status", statusFilter);
            try {
              const res = await fetch(apiUrl(`/admin/reservations/export.csv?${params}`), {
                headers: { Authorization: `Bearer ${token}` },
              });
              if (!res.ok) throw new Error("Export failed");
              const blob = await res.blob();
              const url = URL.createObjectURL(blob);
              const a = document.createElement("a");
              a.href = url;
              a.download = `reservations-${date || isoDay()}.csv`;
              document.body.appendChild(a);
              a.click();
              a.remove();
              URL.revokeObjectURL(url);
            } catch (e) {
              setErr((e as Error).message);
            }
          }}
          className="bq-btn bq-btn-primary h-10 text-sm"
          title="CSV-da yuklab olish (Excel)"
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="mr-1 inline-block">
            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
            <polyline points="7 10 12 15 17 10" />
            <line x1="12" y1="15" x2="12" y2="3" />
          </svg>
          CSV
        </button>
      </div>

      {err ? (
        <div className="rounded-xl border border-[color:var(--danger)]/40 bg-red-50 px-3 py-2 text-sm text-red-700">
          {err}
        </div>
      ) : null}

      {/* Статистика по фильтру */}
      <div className="grid gap-2 sm:grid-cols-4">
        <MiniStat label="Jami" value={String(filtered.length)} />
        <MiniStat
          label="Tasdiqlangan"
          value={String(filtered.filter((r) => r.status === "CONFIRMED").length)}
          tone="#15803d"
        />
        <MiniStat
          label="Kutilmoqda"
          value={String(filtered.filter((r) => r.status === "PENDING_PAYMENT").length)}
          tone="#a16207"
        />
        <MiniStat
          label="No-show"
          value={String(filtered.filter((r) => r.status === "NO_SHOW").length)}
          tone="#b91c1c"
        />
      </div>

      {/* Таблица */}
      <div className="overflow-hidden rounded-2xl border border-[color:var(--border)] bg-white shadow-[var(--shadow-sm)]">
        <div className="max-h-[68vh] overflow-auto">
          <table className="min-w-full text-left text-sm">
            <thead className="sticky top-0 border-b border-[color:var(--border)] bg-[color:var(--bg-soft)] text-[11px] uppercase tracking-wider text-[color:var(--muted)]">
              <tr>
                <th className="px-4 py-3">Vaqt</th>
                <th className="px-4 py-3">Mijoz</th>
                <th className="px-4 py-3">Filial / Stol</th>
                <th className="px-4 py-3">Mehmon</th>
                <th className="px-4 py-3">Depozit</th>
                <th className="px-4 py-3">Holat</th>
                <th className="px-4 py-3 text-right">Amallar</th>
              </tr>
            </thead>
            <tbody>
              {loading && !rows.length
                ? [0, 1, 2, 3, 4].map((i) => (
                    <tr key={i}>
                      <td colSpan={7} className="px-4 py-2">
                        <div className="h-10 animate-pulse rounded-lg bg-[color:var(--bg-soft)]" />
                      </td>
                    </tr>
                  ))
                : filtered.length === 0
                  ? (
                      <tr>
                        <td colSpan={7} className="px-4 py-10 text-center text-sm text-[color:var(--muted)]">
                          Tanlangan filtrlar bo'yicha bronlar topilmadi.
                        </td>
                      </tr>
                    )
                  : filtered.map((r) => (
                      <tr
                        key={r.id}
                        className="border-b border-[color:var(--border)] last:border-b-0 hover:bg-[color:var(--bg-soft)]/60"
                      >
                        <td className="whitespace-nowrap px-4 py-3">
                          <div className="text-sm font-bold text-[color:var(--fg)]">
                            {new Date(r.startAt).toLocaleTimeString("uz-UZ", {
                              hour: "2-digit",
                              minute: "2-digit",
                            })}
                          </div>
                          <div className="text-[11px] text-[color:var(--muted)]">
                            {new Date(r.startAt).toLocaleDateString("uz-UZ")}
                          </div>
                        </td>
                        <td className="px-4 py-3">
                          <div className="text-sm font-semibold text-[color:var(--fg)]">
                            {r.user.name || "—"}
                          </div>
                          <div className="text-[11px] text-[color:var(--muted)]">
                            {r.user.phone}
                          </div>
                        </td>
                        <td className="px-4 py-3">
                          <div className="text-sm font-semibold text-[color:var(--fg)]">
                            {r.branch.name}
                          </div>
                          <div className="text-[11px] text-[color:var(--muted)]">
                            Stol {r.table.number}
                          </div>
                        </td>
                        <td className="px-4 py-3 text-sm text-[color:var(--fg)]">
                          {r.guestsCount ? `${r.guestsCount} ta` : "—"}
                        </td>
                        <td className="px-4 py-3 text-sm text-[color:var(--fg)]">
                          {r.depositAmount
                            ? `${new Intl.NumberFormat("uz-UZ").format(Number(r.depositAmount))} so'm`
                            : "—"}
                        </td>
                        <td className="px-4 py-3">
                          <StatusPill status={r.status} />
                        </td>
                        <td className="px-4 py-3">
                          <RowActions
                            status={r.status}
                            busy={busy === r.id}
                            onAction={(s) => void setStatus(r.id, s)}
                          />
                        </td>
                      </tr>
                    ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function askConfirm(status: string): string | null {
  if (status === "NO_SHOW") return "Mijoz kelmadi deb belgilansinmi? Depozit qaytarilmaydi.";
  if (status === "CANCELLED_BY_RESTAURANT") return "Bronni restoran tomonidan bekor qilasizmi?";
  return null;
}

function RowActions({
  status,
  busy,
  onAction,
}: {
  status: string;
  busy: boolean;
  onAction: (s: (typeof STATUSES)[number]) => void;
}) {
  const canConfirm = status === "PENDING_PAYMENT";
  const canComplete = status === "CONFIRMED" || status === "PENDING_PAYMENT";
  const canNoShow = status === "CONFIRMED" || status === "PENDING_PAYMENT";
  const canCancel =
    status !== "CANCELLED_BY_RESTAURANT" &&
    status !== "CANCELLED_BY_USER" &&
    status !== "COMPLETED" &&
    status !== "NO_SHOW";

  const Btn = ({
    label,
    onClick,
    tone = "ghost",
    disabled,
  }: {
    label: string;
    onClick: () => void;
    tone?: "primary" | "ghost" | "danger";
    disabled?: boolean;
  }) => (
    <button
      type="button"
      disabled={disabled || busy}
      onClick={onClick}
      className={`rounded-lg border px-2.5 py-1 text-[11px] font-bold transition disabled:cursor-not-allowed disabled:opacity-40 ${
        tone === "primary"
          ? "border-[color:var(--brand)] bg-[color:var(--brand)] text-white hover:brightness-110"
          : tone === "danger"
            ? "border-[color:var(--danger)]/50 text-[color:var(--danger)] hover:bg-red-50"
            : "border-[color:var(--border)] text-[color:var(--fg)] hover:border-[color:var(--brand)] hover:bg-[color:var(--bg-soft)]"
      }`}
    >
      {label}
    </button>
  );

  return (
    <div className="flex flex-wrap justify-end gap-1.5">
      {canConfirm ? (
        <Btn label="Tasdiqlash" tone="primary" onClick={() => onAction("CONFIRMED")} />
      ) : null}
      {canComplete ? (
        <Btn label="Keldi" onClick={() => onAction("COMPLETED")} />
      ) : null}
      {canNoShow ? (
        <Btn label="No-show" tone="danger" onClick={() => onAction("NO_SHOW")} />
      ) : null}
      {canCancel ? (
        <Btn
          label="Bekor"
          tone="danger"
          onClick={() => onAction("CANCELLED_BY_RESTAURANT")}
        />
      ) : null}
    </div>
  );
}

function StatusPill({ status }: { status: string }) {
  const map: Record<string, { bg: string; fg: string; label: string }> = {
    PENDING_PAYMENT: { bg: "#fef3c7", fg: "#a16207", label: "To'lov kutilmoqda" },
    CONFIRMED: { bg: "#e8f5ee", fg: "#15803d", label: "Tasdiqlangan" },
    COMPLETED: { bg: "#e8effb", fg: "#1d4ed8", label: "Keldi" },
    NO_SHOW: { bg: "#fdecec", fg: "#b91c1c", label: "No-show" },
    CANCELLED_BY_USER: { bg: "#f3f4f6", fg: "#4b5563", label: "Mijoz bekor" },
    CANCELLED_BY_RESTAURANT: { bg: "#f3f4f6", fg: "#4b5563", label: "Restoran bekor" },
  };
  const m = map[status] || { bg: "#f3f4f6", fg: "#4b5563", label: status };
  return (
    <span
      className="inline-flex items-center rounded-full px-2.5 py-1 text-[11px] font-bold"
      style={{ background: m.bg, color: m.fg }}
    >
      {m.label}
    </span>
  );
}

function MiniStat({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone?: string;
}) {
  return (
    <div className="rounded-xl border border-[color:var(--border)] bg-white p-3 shadow-[var(--shadow-sm)]">
      <div className="text-[10px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
        {label}
      </div>
      <div
        className="text-xl font-extrabold"
        style={{ color: tone ?? "var(--fg)" }}
      >
        {value}
      </div>
    </div>
  );
}
