"use client";

import { useCallback, useEffect, useState } from "react";
import { AdminShell } from "@/components/admin/AdminShell";
import { apiFetch } from "@/lib/api";
import { useAdminToken } from "@/lib/adminAuth";

type Branch = {
  id: string;
  name: string;
  address: string;
  phone?: string | null;
  lat: number | null;
  lng: number | null;
  workHours: unknown;
  isActive: boolean;
};

export default function AdminSozlamalarPage() {
  return (
    <AdminShell title="Sozlamalar" subtitle="Filial sozlamalari va admin sessiyasi">
      <Body />
    </AdminShell>
  );
}

function Body() {
  const { token, setToken } = useAdminToken();
  const [branches, setBranches] = useState<Branch[]>([]);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const b = await apiFetch<Branch[]>("/branches");
      setBranches(b);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  return (
    <div className="space-y-4">
      {/* Session */}
      <section className="rounded-2xl border border-[color:var(--border)] bg-white p-5 shadow-[var(--shadow-sm)]">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h2 className="text-sm font-bold text-[color:var(--fg)]">Admin sessiya</h2>
            <p className="text-xs text-[color:var(--muted)]">
              Joriy admin tokeni brauzerda localStorage da saqlanadi
            </p>
          </div>
          <button
            type="button"
            onClick={() => setToken(null)}
            className="bq-btn h-9 rounded-xl border border-rose-300 px-3 text-xs font-bold text-rose-700 transition hover:bg-rose-50"
          >
            Chiqish
          </button>
        </div>
        <div className="mt-3 rounded-xl border border-[color:var(--border)] bg-[color:var(--bg-soft)] p-3 font-mono text-[11px] text-[color:var(--muted)]">
          {token ? `${token.slice(0, 28)}…${token.slice(-10)}` : "—"}
        </div>
      </section>

      {/* Branches */}
      <section>
        <div className="mb-2 flex items-center justify-between">
          <h2 className="text-sm font-bold text-[color:var(--fg)]">Filiallar sozlamalari</h2>
          {loading ? (
            <span className="text-xs text-[color:var(--muted)]">Yuklanmoqda…</span>
          ) : null}
        </div>
        <div className="grid gap-3">
          {branches.map((b) => (
            <BranchEditor
              key={b.id}
              branch={b}
              token={token}
              onSaved={load}
            />
          ))}
        </div>
      </section>
    </div>
  );
}

function BranchEditor({
  branch,
  token,
  onSaved,
}: {
  branch: Branch;
  token: string | null;
  onSaved: () => void;
}) {
  const [name, setName] = useState(branch.name);
  const [address, setAddress] = useState(branch.address);
  const [workHours, setWorkHours] = useState(
    typeof branch.workHours === "string" ? branch.workHours : "10:00-23:00",
  );
  const [lat, setLat] = useState(branch.lat != null ? String(branch.lat) : "");
  const [lng, setLng] = useState(branch.lng != null ? String(branch.lng) : "");
  const [isActive, setIsActive] = useState(branch.isActive);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [geocoding, setGeocoding] = useState(false);

  const geocode = async () => {
    const q = address.trim();
    if (!q) return;
    setGeocoding(true);
    setErr(null);
    try {
      const res = await fetch(
        `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(q)}&format=json&limit=1`,
        { headers: { "Accept-Language": "uz,ru" } },
      );
      const data = (await res.json()) as Array<{ lat: string; lon: string; display_name: string }>;
      if (data[0]) {
        setLat(parseFloat(data[0].lat).toFixed(6));
        setLng(parseFloat(data[0].lon).toFixed(6));
        setMsg(`Topildi: ${data[0].display_name.slice(0, 60)}…`);
      } else {
        setErr("Manzil topilmadi. Aniqroq manzil kiriting.");
      }
    } catch {
      setErr("Geokodlashda xatolik. Internet ulanishini tekshiring.");
    } finally {
      setGeocoding(false);
    }
  };

  const save = async () => {
    if (!token) return;
    setBusy(true);
    setMsg(null);
    setErr(null);
    try {
      const payload: Record<string, unknown> = {
        name: name.trim(),
        address: address.trim(),
        workHours: workHours.trim() || undefined,
        isActive,
      };
      if (lat.trim() === "") payload.lat = null;
      else if (!isNaN(Number(lat))) payload.lat = Number(lat);
      if (lng.trim() === "") payload.lng = null;
      else if (!isNaN(Number(lng))) payload.lng = Number(lng);

      await apiFetch(`/admin/branches/${branch.id}`, {
        method: "PATCH",
        token,
        body: JSON.stringify(payload),
      });
      setMsg("Saqlandi");
      onSaved();
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)]">
      <div className="flex items-start justify-between gap-2">
        <h3 className="text-base font-extrabold text-[color:var(--fg)]">{branch.name}</h3>
        <label className="inline-flex items-center gap-2 text-xs font-semibold">
          <input
            type="checkbox"
            checked={isActive}
            onChange={(e) => setIsActive(e.target.checked)}
            className="h-4 w-4 accent-[color:var(--brand)]"
          />
          <span className={isActive ? "text-emerald-700" : "text-[color:var(--muted)]"}>
            {isActive ? "Faol" : "Yopiq"}
          </span>
        </label>
      </div>

      <div className="mt-3 grid gap-2 sm:grid-cols-2">
        <L label="Nomi">
          <input
            value={name}
            onChange={(e) => setName(e.target.value)}
            className="w-full rounded-xl border border-[color:var(--border)] bg-white px-3 py-2 text-sm"
          />
        </L>
        <L label="Ish vaqti">
          <input
            value={workHours}
            onChange={(e) => setWorkHours(e.target.value)}
            placeholder="10:00-23:00"
            className="w-full rounded-xl border border-[color:var(--border)] bg-white px-3 py-2 text-sm"
          />
        </L>
        <L label="Manzil">
          <input
            value={address}
            onChange={(e) => setAddress(e.target.value)}
            className="w-full rounded-xl border border-[color:var(--border)] bg-white px-3 py-2 text-sm"
          />
        </L>
        <div className="space-y-2">
          <div className="grid grid-cols-2 gap-2">
            <L label="Latitude">
              <input
                value={lat}
                onChange={(e) => setLat(e.target.value)}
                placeholder="41.311"
                className="w-full rounded-xl border border-[color:var(--border)] bg-white px-3 py-2 text-sm"
              />
            </L>
            <L label="Longitude">
              <input
                value={lng}
                onChange={(e) => setLng(e.target.value)}
                placeholder="69.279"
                className="w-full rounded-xl border border-[color:var(--border)] bg-white px-3 py-2 text-sm"
              />
            </L>
          </div>
          <button
            type="button"
            onClick={() => void geocode()}
            disabled={geocoding || !address.trim()}
            className="inline-flex items-center gap-1.5 rounded-xl border border-[color:var(--border)] bg-[color:var(--bg-soft)] px-3 py-1.5 text-xs font-semibold text-[color:var(--fg)] transition hover:border-[color:var(--brand)] hover:text-[color:var(--brand)] disabled:cursor-not-allowed disabled:opacity-50"
          >
            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="11" cy="11" r="8" /><path d="M21 21l-4.35-4.35" />
            </svg>
            {geocoding ? "Qidirmoqda…" : "Manzildan koordinata topish"}
          </button>
        </div>
      </div>

      <div className="mt-3 flex items-center justify-between">
        <div className="text-xs">
          {msg ? <span className="text-emerald-700">{msg}</span> : null}
          {err ? <span className="text-rose-600">{err}</span> : null}
        </div>
        <button
          type="button"
          onClick={save}
          disabled={busy}
          className="bq-btn bq-btn-primary h-9 px-4 text-xs disabled:opacity-50"
        >
          {busy ? "…" : "Saqlash"}
        </button>
      </div>
    </div>
  );
}

function L({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="block">
      <span className="block text-[10px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
        {label}
      </span>
      <div className="mt-1">{children}</div>
    </label>
  );
}
