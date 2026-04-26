"use client";

import { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import { AdminShell } from "@/components/admin/AdminShell";
import { apiFetch } from "@/lib/api";

type Branch = {
  id: string;
  name: string;
  address?: string | null;
  phone?: string | null;
  openingTime?: string | null;
  closingTime?: string | null;
  totalTables?: number;
  availableTables?: number;
  isActive?: boolean;
};

export default function AdminFiliallarPage() {
  return (
    <AdminShell
      title="Filiallar"
      subtitle="Filiallar ro'yxati va har bir filial bo'yicha statistika"
    >
      <BranchesBody />
    </AdminShell>
  );
}

function BranchesBody() {
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
      <div className="rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)]">
        <p className="text-xs text-[color:var(--muted)]">
          Filial tahrirlash va qo'shish (soatlar, manzil, status) keyingi bosqichda qo'shiladi. Hozircha —
          umumiy ko'rinish va bandlik statistikasi.
        </p>
      </div>

      {loading && !branches.length ? (
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {[0, 1, 2].map((i) => (
            <div key={i} className="h-40 animate-pulse rounded-2xl bg-[color:var(--bg-soft)]" />
          ))}
        </div>
      ) : (
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {branches.map((b) => {
            const total = b.totalTables ?? 0;
            const free = b.availableTables ?? 0;
            const occupied = Math.max(0, total - free);
            const pct = total ? Math.round((occupied / total) * 100) : 0;
            return (
              <div
                key={b.id}
                className="rounded-2xl border border-[color:var(--border)] bg-white p-5 shadow-[var(--shadow-sm)] transition hover:shadow-[var(--shadow-md)]"
              >
                <div className="flex items-start justify-between gap-2">
                  <div className="min-w-0">
                    <h3 className="truncate text-base font-extrabold text-[color:var(--fg)]">
                      {b.name}
                    </h3>
                    {b.address ? (
                      <p className="mt-0.5 line-clamp-2 text-xs text-[color:var(--muted)]">
                        {b.address}
                      </p>
                    ) : null}
                  </div>
                  <span
                    className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-bold ${
                      b.isActive !== false
                        ? "bg-[#e8f5ee] text-[#15803d]"
                        : "bg-[#f3f4f6] text-[#4b5563]"
                    }`}
                  >
                    {b.isActive !== false ? "Faol" : "Yopiq"}
                  </span>
                </div>

                {b.openingTime && b.closingTime ? (
                  <p className="mt-2 text-xs text-[color:var(--muted)]">
                    {b.openingTime?.slice(0, 5)} – {b.closingTime?.slice(0, 5)}
                  </p>
                ) : null}

                <div className="mt-3">
                  <div className="flex items-center justify-between text-xs">
                    <span className="font-semibold text-[color:var(--fg)]">Bandlik</span>
                    <span className="text-[color:var(--muted)]">
                      {occupied}/{total} band · {free} bo'sh
                    </span>
                  </div>
                  <div className="mt-1.5 h-2 overflow-hidden rounded-full bg-[color:var(--bg-soft)]">
                    <div
                      className="h-full rounded-full bg-[color:var(--brand)] transition-all"
                      style={{ width: `${pct}%` }}
                    />
                  </div>
                </div>

                <div className="mt-4 flex gap-2">
                  <Link
                    href={`/admin/zonalar`}
                    className="bq-btn bq-btn-ghost h-9 flex-1 text-xs"
                  >
                    Stollar
                  </Link>
                  <Link
                    href={`/admin/bronlar`}
                    className="bq-btn bq-btn-primary h-9 flex-1 text-xs"
                  >
                    Bronlar
                  </Link>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
