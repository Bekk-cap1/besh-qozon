"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { AdminShell } from "@/components/admin/AdminShell";
import { apiFetch } from "@/lib/api";
import { useAdminToken } from "@/lib/adminAuth";

type Branch = { id: string; name: string };

type MenuItem = {
  id: string;
  branchId: string;
  name: string;
  description: string | null;
  category: string;
  price: string | number;
  imageUrl: string | null;
  isActive: boolean;
  sortOrder: number;
};

type Draft = {
  name: string;
  description: string;
  category: string;
  price: string;
  imageUrl: string;
  sortOrder: string;
};

const EMPTY_DRAFT: Draft = {
  name: "",
  description: "",
  category: "Asosiy",
  price: "",
  imageUrl: "",
  sortOrder: "0",
};

export default function AdminMenuPage() {
  return (
    <AdminShell title="Menyu" subtitle="Filial bo'yicha taom va ichimliklar">
      <MenuBody />
    </AdminShell>
  );
}

function MenuBody() {
  const { token } = useAdminToken();
  const [branches, setBranches] = useState<Branch[]>([]);
  const [branchId, setBranchId] = useState<string | null>(null);
  const [items, setItems] = useState<MenuItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [draft, setDraft] = useState<Draft>(EMPTY_DRAFT);
  const [editing, setEditing] = useState<MenuItem | null>(null);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    apiFetch<Branch[]>("/branches").then((b) => {
      setBranches(b);
      if (b[0]) setBranchId(b[0].id);
    });
  }, []);

  const loadMenu = useCallback(async () => {
    if (!branchId || !token) return;
    setLoading(true);
    setErr(null);
    try {
      const data = await apiFetch<MenuItem[]>(
        `/admin/branches/${branchId}/menu`,
        { token },
      );
      setItems(data);
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setLoading(false);
    }
  }, [branchId, token]);

  useEffect(() => {
    void loadMenu();
  }, [loadMenu]);

  const grouped = useMemo(() => {
    const map = new Map<string, MenuItem[]>();
    for (const it of items) {
      const arr = map.get(it.category) ?? [];
      arr.push(it);
      map.set(it.category, arr);
    }
    return Array.from(map.entries()).sort((a, b) => a[0].localeCompare(b[0]));
  }, [items]);

  const submit = async () => {
    if (!branchId || !token) return;
    if (!draft.name.trim() || !draft.price) {
      setErr("Nomi va narx majburiy");
      return;
    }
    setBusy(true);
    setErr(null);
    setMsg(null);
    try {
      const payload = {
        name: draft.name.trim(),
        description: draft.description.trim() || undefined,
        category: draft.category.trim() || "Asosiy",
        price: Number(draft.price),
        imageUrl: draft.imageUrl.trim() || undefined,
        sortOrder: Number(draft.sortOrder) || 0,
      };
      if (editing) {
        await apiFetch(`/admin/menu/${editing.id}`, {
          method: "PUT",
          token,
          body: JSON.stringify(payload),
        });
        setMsg("Yangilandi");
      } else {
        await apiFetch(`/admin/branches/${branchId}/menu`, {
          method: "POST",
          token,
          body: JSON.stringify(payload),
        });
        setMsg("Qo'shildi");
      }
      setEditing(null);
      setDraft(EMPTY_DRAFT);
      await loadMenu();
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const beginEdit = (it: MenuItem) => {
    setEditing(it);
    setDraft({
      name: it.name,
      description: it.description ?? "",
      category: it.category,
      price: String(Number(it.price)),
      imageUrl: it.imageUrl ?? "",
      sortOrder: String(it.sortOrder),
    });
  };

  const toggleActive = async (it: MenuItem) => {
    if (!token) return;
    try {
      await apiFetch(`/admin/menu/${it.id}`, {
        method: "PUT",
        token,
        body: JSON.stringify({ isActive: !it.isActive }),
      });
      await loadMenu();
    } catch (e) {
      setErr((e as Error).message);
    }
  };

  const remove = async (it: MenuItem) => {
    if (!token) return;
    if (!confirm(`"${it.name}" ni o'chirasizmi?`)) return;
    try {
      await apiFetch(`/admin/menu/${it.id}`, { method: "DELETE", token });
      await loadMenu();
    } catch (e) {
      setErr((e as Error).message);
    }
  };

  return (
    <div className="space-y-4">
      {/* Branch selector */}
      <div className="rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)]">
        <label className="block text-xs font-bold uppercase tracking-wider text-[color:var(--muted)]">
          Filial
        </label>
        <select
          value={branchId ?? ""}
          onChange={(e) => setBranchId(e.target.value || null)}
          className="mt-2 w-full rounded-xl border border-[color:var(--border)] bg-white px-3 py-2 text-sm sm:max-w-xs"
        >
          {branches.map((b) => (
            <option key={b.id} value={b.id}>
              {b.name}
            </option>
          ))}
        </select>
      </div>

      <div className="grid gap-4 lg:grid-cols-[1fr_360px]">
        {/* Items list */}
        <div className="space-y-3">
          {loading ? (
            <div className="rounded-2xl border border-[color:var(--border)] bg-white p-8 text-center text-sm text-[color:var(--muted)]">
              Yuklanmoqda…
            </div>
          ) : items.length === 0 ? (
            <div className="rounded-2xl border border-dashed border-[color:var(--border)] bg-white p-8 text-center">
              <div className="text-3xl">🍽</div>
              <div className="mt-2 text-sm font-bold text-[color:var(--fg)]">
                Hozircha menyu bo&apos;sh
              </div>
              <p className="mt-1 text-xs text-[color:var(--muted)]">
                O&apos;ng tarafda yangi pozitsiyani qo&apos;shing
              </p>
            </div>
          ) : (
            grouped.map(([cat, arr]) => (
              <div key={cat} className="overflow-hidden rounded-2xl border border-[color:var(--border)] bg-white shadow-[var(--shadow-sm)]">
                <div className="flex items-center justify-between border-b border-[color:var(--border)] bg-[color:var(--bg-soft)] px-4 py-2 text-xs font-bold uppercase tracking-wider text-[color:var(--muted)]">
                  <span>{cat}</span>
                  <span className="text-[10px]">{arr.length} ta pozitsiya</span>
                </div>
                <div>
                  {arr
                    .sort((a, b) => a.sortOrder - b.sortOrder)
                    .map((it) => (
                      <div
                        key={it.id}
                        className={`flex items-center gap-3 border-b border-[color:var(--border)] px-4 py-3 last:border-b-0 ${
                          !it.isActive ? "opacity-50" : ""
                        }`}
                      >
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2">
                            <span className="truncate text-sm font-bold text-[color:var(--fg)]">
                              {it.name}
                            </span>
                            {!it.isActive ? (
                              <span className="rounded-full bg-[color:var(--bg-soft)] px-1.5 text-[9px] font-bold text-[color:var(--muted)]">
                                YASHIRIN
                              </span>
                            ) : null}
                          </div>
                          {it.description ? (
                            <div className="line-clamp-1 text-xs text-[color:var(--muted)]">
                              {it.description}
                            </div>
                          ) : null}
                        </div>
                        <div className="text-right text-sm font-extrabold text-[color:var(--brand-700)]">
                          {Number(it.price).toLocaleString("ru-RU")}
                          <div className="text-[9px] font-normal text-[color:var(--muted)]">
                            so&apos;m · #{it.sortOrder}
                          </div>
                        </div>
                        <div className="flex items-center gap-1">
                          <button
                            type="button"
                            onClick={() => toggleActive(it)}
                            title={it.isActive ? "Yashirish" : "Ko'rsatish"}
                            className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-[color:var(--border)] text-[color:var(--muted)] hover:border-[color:var(--brand)] hover:text-[color:var(--brand-700)]"
                          >
                            {it.isActive ? "👁" : "🚫"}
                          </button>
                          <button
                            type="button"
                            onClick={() => beginEdit(it)}
                            title="Tahrirlash"
                            className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-[color:var(--border)] text-[color:var(--muted)] hover:border-[color:var(--brand)] hover:text-[color:var(--brand-700)]"
                          >
                            ✎
                          </button>
                          <button
                            type="button"
                            onClick={() => remove(it)}
                            title="O'chirish"
                            className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-rose-200 text-rose-600 hover:border-rose-400 hover:bg-rose-50"
                          >
                            🗑
                          </button>
                        </div>
                      </div>
                    ))}
                </div>
              </div>
            ))
          )}
        </div>

        {/* Form */}
        <div className="space-y-3 lg:sticky lg:top-20 lg:self-start">
          <div className="rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)]">
            <div className="text-xs font-bold uppercase tracking-wider text-[color:var(--muted)]">
              {editing ? "Tahrirlash" : "Yangi pozitsiya"}
            </div>
            <div className="mt-3 space-y-2.5">
              <Field label="Nomi *">
                <input
                  type="text"
                  value={draft.name}
                  onChange={(e) => setDraft((d) => ({ ...d, name: e.target.value }))}
                  className="w-full rounded-xl border border-[color:var(--border)] bg-white px-3 py-2 text-sm"
                  placeholder="masalan, Beshqozon shashlik"
                />
              </Field>
              <Field label="Tavsif">
                <textarea
                  value={draft.description}
                  onChange={(e) => setDraft((d) => ({ ...d, description: e.target.value }))}
                  className="w-full rounded-xl border border-[color:var(--border)] bg-white px-3 py-2 text-sm"
                  rows={2}
                  placeholder="qisqa izoh"
                />
              </Field>
              <div className="grid grid-cols-2 gap-2">
                <Field label="Kategoriya">
                  <input
                    type="text"
                    list="categories"
                    value={draft.category}
                    onChange={(e) => setDraft((d) => ({ ...d, category: e.target.value }))}
                    className="w-full rounded-xl border border-[color:var(--border)] bg-white px-3 py-2 text-sm"
                  />
                  <datalist id="categories">
                    <option value="Asosiy" />
                    <option value="Salat" />
                    <option value="Birinchi taom" />
                    <option value="Ichimliklar" />
                    <option value="Shirinliklar" />
                  </datalist>
                </Field>
                <Field label="Tartib (sort)">
                  <input
                    type="number"
                    value={draft.sortOrder}
                    onChange={(e) => setDraft((d) => ({ ...d, sortOrder: e.target.value }))}
                    className="w-full rounded-xl border border-[color:var(--border)] bg-white px-3 py-2 text-sm"
                  />
                </Field>
              </div>
              <Field label="Narx (so'm) *">
                <input
                  type="number"
                  min="0"
                  step="1000"
                  value={draft.price}
                  onChange={(e) => setDraft((d) => ({ ...d, price: e.target.value }))}
                  className="w-full rounded-xl border border-[color:var(--border)] bg-white px-3 py-2 text-sm"
                  placeholder="50000"
                />
              </Field>
              <Field label="Rasm URL (ixtiyoriy)">
                <input
                  type="text"
                  value={draft.imageUrl}
                  onChange={(e) => setDraft((d) => ({ ...d, imageUrl: e.target.value }))}
                  className="w-full rounded-xl border border-[color:var(--border)] bg-white px-3 py-2 text-sm"
                  placeholder="https://…"
                />
              </Field>
            </div>

            {err ? <p className="mt-3 text-xs text-rose-600">{err}</p> : null}
            {msg ? <p className="mt-3 text-xs text-emerald-700">{msg}</p> : null}

            <div className="mt-4 flex gap-2">
              <button
                type="button"
                onClick={submit}
                disabled={busy}
                className="bq-btn bq-btn-primary h-10 flex-1 text-sm disabled:opacity-50"
              >
                {busy ? "…" : editing ? "Saqlash" : "Qo'shish"}
              </button>
              {editing ? (
                <button
                  type="button"
                  onClick={() => {
                    setEditing(null);
                    setDraft(EMPTY_DRAFT);
                    setErr(null);
                    setMsg(null);
                  }}
                  className="bq-btn bq-btn-ghost h-10 px-3 text-sm"
                >
                  Bekor
                </button>
              ) : null}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="block">
      <span className="block text-[10px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
        {label}
      </span>
      <div className="mt-1">{children}</div>
    </label>
  );
}
