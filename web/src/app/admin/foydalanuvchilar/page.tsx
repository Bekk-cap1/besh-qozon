"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { AdminShell } from "@/components/admin/AdminShell";
import { apiFetch } from "@/lib/api";
import { useAdminToken } from "@/lib/adminAuth";

type User = {
  id: string;
  phone: string;
  name: string | null;
  noShowStreak: number;
  bookingBlockedUntil: string | null;
  completedBookings: number;
  loyaltyBonuses: number;
  createdAt: string;
  telegramUsername: string | null;
  _count: { reservations: number };
};

type ListResp = { users: User[]; total: number; page: number; limit: number };

const LIMIT = 30;

function isBlocked(u: User): boolean {
  if (!u.bookingBlockedUntil) return false;
  return new Date(u.bookingBlockedUntil) > new Date();
}

export default function FoydalanuvchilarPage() {
  return (
    <AdminShell title="Foydalanuvchilar" subtitle="Mijozlar ro'yxati, bloklash va o'chirish">
      <UsersBody />
    </AdminShell>
  );
}

function UsersBody() {
  const { token } = useAdminToken();
  const [users, setUsers] = useState<User[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [q, setQ] = useState("");
  const [filter, setFilter] = useState<"all" | "blocked">("all");
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [actionErr, setActionErr] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [confirmDelete, setConfirmDelete] = useState<User | null>(null);
  const searchTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const load = useCallback(
    async (pg = page, search = q, f = filter) => {
      if (!token) return;
      setLoading(true);
      setErr(null);
      try {
        const params = new URLSearchParams({
          page: String(pg),
          limit: String(LIMIT),
          ...(search.trim() ? { q: search.trim() } : {}),
          ...(f === "blocked" ? { filter: "blocked" } : {}),
        });
        const data = await apiFetch<ListResp>(`/admin/users?${params}`, { token });
        setUsers(data.users);
        setTotal(data.total);
        setPage(pg);
      } catch (e) {
        setErr((e as Error).message);
      } finally {
        setLoading(false);
      }
    },
    [token, page, q, filter],
  );

  useEffect(() => {
    void load(1, q, filter);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [token, filter]);

  const onSearch = (v: string) => {
    setQ(v);
    if (searchTimer.current) clearTimeout(searchTimer.current);
    searchTimer.current = setTimeout(() => void load(1, v, filter), 350);
  };

  const action = async (userId: string, kind: "block" | "unblock" | "delete") => {
    if (!token) return;
    setBusy(userId);
    setActionErr(null);
    try {
      if (kind === "delete") {
        await apiFetch(`/admin/users/${userId}`, { method: "DELETE", token });
      } else {
        await apiFetch(`/admin/users/${userId}/${kind}`, { method: "PATCH", token });
      }
      setConfirmDelete(null);
      await load(page, q, filter);
    } catch (e) {
      setActionErr((e as Error).message);
    } finally {
      setBusy(null);
    }
  };

  const totalPages = Math.ceil(total / LIMIT);

  return (
    <div className="space-y-4">
      {/* Toolbar */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="relative flex-1 min-w-48">
          <span className="pointer-events-none absolute inset-y-0 left-3 flex items-center text-[color:var(--muted)]">
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="11" cy="11" r="8" /><path d="M21 21l-4.35-4.35" />
            </svg>
          </span>
          <input
            className="bq-input h-10 pl-9 text-sm"
            placeholder="Telefon yoki ism bo'yicha qidirish…"
            value={q}
            onChange={(e) => onSearch(e.target.value)}
          />
        </div>
        <div className="flex rounded-xl border border-[color:var(--border)] overflow-hidden text-sm font-semibold">
          {(["all", "blocked"] as const).map((f) => (
            <button
              key={f}
              type="button"
              onClick={() => { setFilter(f); void load(1, q, f); }}
              className={`px-4 py-2 transition ${
                filter === f
                  ? "bg-[color:var(--brand)] text-white"
                  : "bg-white text-[color:var(--muted)] hover:bg-[color:var(--bg-soft)]"
              }`}
            >
              {f === "all" ? "Barchasi" : "Bloklangan"}
            </button>
          ))}
        </div>
        <span className="text-xs text-[color:var(--muted)] whitespace-nowrap">
          Jami: <b>{total}</b>
        </span>
      </div>

      {err ? (
        <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">{err}</div>
      ) : null}
      {actionErr ? (
        <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {actionErr}
          <button type="button" onClick={() => setActionErr(null)} className="ml-3 font-bold">✕</button>
        </div>
      ) : null}

      {/* Table */}
      <div className="overflow-x-auto rounded-2xl border border-[color:var(--border)] bg-white">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="border-b border-[color:var(--border)] bg-[color:var(--bg-soft)]">
              <th className="px-4 py-3 text-left text-xs font-bold uppercase tracking-wider text-[color:var(--muted)]">Foydalanuvchi</th>
              <th className="px-4 py-3 text-center text-xs font-bold uppercase tracking-wider text-[color:var(--muted)]">Bronlar</th>
              <th className="px-4 py-3 text-center text-xs font-bold uppercase tracking-wider text-[color:var(--muted)]">No-show</th>
              <th className="px-4 py-3 text-center text-xs font-bold uppercase tracking-wider text-[color:var(--muted)]">Holat</th>
              <th className="px-4 py-3 text-center text-xs font-bold uppercase tracking-wider text-[color:var(--muted)]">Amallar</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-[color:var(--border)]">
            {loading ? (
              <tr>
                <td colSpan={5} className="py-12 text-center text-[color:var(--muted)]">
                  <svg className="inline-block animate-spin mr-2" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <path d="M21 12a9 9 0 1 1-6.219-8.56" />
                  </svg>
                  Yuklanmoqda…
                </td>
              </tr>
            ) : users.length === 0 ? (
              <tr>
                <td colSpan={5} className="py-12 text-center text-[color:var(--muted)]">
                  {filter === "blocked" ? "Bloklangan foydalanuvchilar yo'q" : "Foydalanuvchilar topilmadi"}
                </td>
              </tr>
            ) : (
              users.map((u) => {
                const blocked = isBlocked(u);
                const isBusy = busy === u.id;
                return (
                  <tr key={u.id} className={`transition hover:bg-[color:var(--bg-soft)] ${blocked ? "bg-red-50/40" : ""}`}>
                    {/* User info */}
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-3">
                        <span className="inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-[color:var(--brand)]/10 text-sm font-bold text-[color:var(--brand)]">
                          {u.name ? u.name[0].toUpperCase() : u.phone.slice(-2)}
                        </span>
                        <div>
                          <div className="font-semibold text-[color:var(--fg)]">
                            {u.name ?? <span className="text-[color:var(--muted)] font-normal italic">Ism yo'q</span>}
                          </div>
                          <div className="text-xs text-[color:var(--muted)]">{u.phone}</div>
                          {u.telegramUsername ? (
                            <div className="text-[10px] text-sky-600">@{u.telegramUsername}</div>
                          ) : null}
                          <div className="text-[10px] text-[color:var(--muted)]">
                            {new Date(u.createdAt).toLocaleDateString("uz-UZ")}
                          </div>
                        </div>
                      </div>
                    </td>

                    {/* Reservations count */}
                    <td className="px-4 py-3 text-center">
                      <span className="rounded-full bg-[color:var(--bg-soft)] px-2.5 py-1 text-xs font-semibold text-[color:var(--fg)]">
                        {u._count.reservations}
                      </span>
                    </td>

                    {/* No-show streak */}
                    <td className="px-4 py-3 text-center">
                      <span className={`rounded-full px-2.5 py-1 text-xs font-bold ${
                        u.noShowStreak >= 3
                          ? "bg-red-100 text-red-700"
                          : u.noShowStreak > 0
                          ? "bg-amber-100 text-amber-700"
                          : "bg-emerald-100 text-emerald-700"
                      }`}>
                        {u.noShowStreak}x
                      </span>
                    </td>

                    {/* Status */}
                    <td className="px-4 py-3 text-center">
                      {blocked ? (
                        <span className="inline-flex items-center gap-1.5 rounded-full bg-red-100 px-2.5 py-1 text-xs font-bold text-red-700">
                          <span className="inline-block h-1.5 w-1.5 rounded-full bg-red-500" />
                          Bloklangan
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1.5 rounded-full bg-emerald-100 px-2.5 py-1 text-xs font-bold text-emerald-700">
                          <span className="inline-block h-1.5 w-1.5 rounded-full bg-emerald-500" />
                          Faol
                        </span>
                      )}
                    </td>

                    {/* Actions */}
                    <td className="px-4 py-3">
                      <div className="flex items-center justify-center gap-2">
                        {blocked ? (
                          <button
                            type="button"
                            disabled={isBusy}
                            onClick={() => void action(u.id, "unblock")}
                            className="rounded-xl border border-emerald-300 bg-emerald-50 px-3 py-1.5 text-xs font-semibold text-emerald-700 transition hover:bg-emerald-100 disabled:opacity-50"
                          >
                            {isBusy ? "…" : "Blokdan chiqarish"}
                          </button>
                        ) : (
                          <button
                            type="button"
                            disabled={isBusy}
                            onClick={() => void action(u.id, "block")}
                            className="rounded-xl border border-amber-300 bg-amber-50 px-3 py-1.5 text-xs font-semibold text-amber-700 transition hover:bg-amber-100 disabled:opacity-50"
                          >
                            {isBusy ? "…" : "Bloklash"}
                          </button>
                        )}
                        <button
                          type="button"
                          disabled={isBusy}
                          onClick={() => setConfirmDelete(u)}
                          className="rounded-xl border border-rose-200 bg-rose-50 px-3 py-1.5 text-xs font-semibold text-rose-700 transition hover:bg-rose-100 disabled:opacity-50"
                          title="O'chirish"
                        >
                          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                            <polyline points="3 6 5 6 21 6" />
                            <path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6" />
                            <path d="M10 11v6M14 11v6" />
                            <path d="M9 6V4h6v2" />
                          </svg>
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 ? (
        <div className="flex items-center justify-center gap-2 pt-1">
          <button
            type="button"
            disabled={page <= 1 || loading}
            onClick={() => void load(page - 1)}
            className="bq-btn bq-btn-ghost h-9 px-4 text-sm disabled:opacity-40"
          >
            ← Oldingi
          </button>
          <span className="rounded-xl border border-[color:var(--border)] bg-white px-4 py-2 text-sm font-semibold">
            {page} / {totalPages}
          </span>
          <button
            type="button"
            disabled={page >= totalPages || loading}
            onClick={() => void load(page + 1)}
            className="bq-btn bq-btn-ghost h-9 px-4 text-sm disabled:opacity-40"
          >
            Keyingi →
          </button>
        </div>
      ) : null}

      {/* Delete confirm modal */}
      {confirmDelete ? (
        <DeleteModal
          user={confirmDelete}
          busy={busy === confirmDelete.id}
          onConfirm={() => void action(confirmDelete.id, "delete")}
          onCancel={() => setConfirmDelete(null)}
        />
      ) : null}
    </div>
  );
}

function DeleteModal({
  user,
  busy,
  onConfirm,
  onCancel,
}: {
  user: User;
  busy: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4" onClick={onCancel}>
      <div
        className="w-full max-w-sm rounded-2xl border border-[color:var(--border)] bg-white p-6 shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="mb-4 flex h-12 w-12 items-center justify-center rounded-full bg-red-100">
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#dc2626" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" />
            <line x1="12" y1="9" x2="12" y2="13" />
            <line x1="12" y1="17" x2="12.01" y2="17" />
          </svg>
        </div>
        <h2 className="mb-1 text-base font-bold text-[color:var(--fg)]">Foydalanuvchini o'chirish</h2>
        <p className="mb-1 text-sm text-[color:var(--muted)]">
          <b className="text-[color:var(--fg)]">{user.phone}</b>
          {user.name ? ` (${user.name})` : ""} — barcha bronlari va ma'lumotlari bilan birga o'chiriladi.
        </p>
        <p className="mb-5 text-xs font-semibold text-red-600">Bu amalni ortga qaytarib bo'lmaydi!</p>
        <div className="flex gap-3">
          <button
            type="button"
            onClick={onCancel}
            disabled={busy}
            className="bq-btn bq-btn-ghost h-10 flex-1 text-sm"
          >
            Bekor qilish
          </button>
          <button
            type="button"
            onClick={onConfirm}
            disabled={busy}
            className="h-10 flex-1 rounded-xl bg-red-600 text-sm font-bold text-white transition hover:bg-red-700 disabled:opacity-50"
          >
            {busy ? "O'chirilmoqda…" : "Ha, o'chirish"}
          </button>
        </div>
      </div>
    </div>
  );
}
