"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { apiFetch } from "@/lib/api";
import { BranchCard } from "@/components/BranchCard";
import { MapLinkButton } from "@/components/MapLinkButton";

type Branch = {
  id: string;
  name: string;
  address?: string | null;
  workHours?: unknown;
  phone?: string | null;
  lat?: number | null;
  lng?: number | null;
  _count?: { zones?: number };
  totalTables?: number;
  availableTables?: number;
};

export default function FiliallarPage() {
  const [branches, setBranches] = useState<Branch[]>([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const data = await apiFetch<Branch[]>("/branches");
        if (!cancelled) setBranches(data);
      } catch (e) {
        if (!cancelled) setErr((e as Error).message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <div className="bq-container py-14">
      <div>
        <span className="bq-chip">Filiallar</span>
        <h1 className="mt-3 text-3xl font-extrabold tracking-tight text-[color:var(--fg)] md:text-4xl">
          Bizning filiallarimiz
        </h1>
        <p className="mt-2 max-w-xl text-[color:var(--muted)]">
          Filialni tanlang va bir necha daqiqada stol band qiling.
        </p>
      </div>

      <div className="mt-8 grid gap-6 sm:grid-cols-2 lg:grid-cols-3">
        {loading ? (
          Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="bq-card overflow-hidden">
              <div className="aspect-[4/3] w-full animate-pulse bg-[color:var(--bg-soft)]" />
              <div className="space-y-2 p-4">
                <div className="h-4 w-2/3 animate-pulse rounded bg-[color:var(--bg-soft)]" />
                <div className="h-3 w-1/2 animate-pulse rounded bg-[color:var(--bg-soft)]" />
              </div>
            </div>
          ))
        ) : err ? (
          <div className="col-span-full rounded-xl border border-[color:var(--border)] bg-[color:var(--bg-soft)] p-6 text-sm text-[color:var(--muted)]">
            Filiallarni yuklab bo&apos;lmadi: {err}
          </div>
        ) : branches.length === 0 ? (
          <div className="col-span-full rounded-xl border border-[color:var(--border)] bg-[color:var(--bg-soft)] p-6 text-sm text-[color:var(--muted)]">
            Hozircha filiallar yo&apos;q.{" "}
            <Link href="/admin" className="text-[color:var(--brand-700)] underline">
              Admin panelidan qo&apos;shing
            </Link>
            .
          </div>
        ) : (
          branches.map((b, i) => (
            <div key={b.id} className="flex flex-col gap-2">
              <BranchCard
                branch={b}
                index={i}
                variant="link"
                href={`/bron?branch=${b.id}`}
              />
              <div className="flex items-center justify-between">
                <MapLinkButton
                  name={b.name}
                  address={b.address}
                  lat={b.lat}
                  lng={b.lng}
                  variant="pill"
                />
                {b.phone ? (
                  <a
                    href={`tel:${b.phone}`}
                    className="inline-flex items-center gap-1.5 rounded-full border border-[color:var(--border)] bg-white px-3 py-1 text-xs font-semibold text-[color:var(--fg)] hover:border-[color:var(--brand)] hover:text-[color:var(--brand-700)]"
                  >
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6A19.79 19.79 0 0 1 2.12 4.18 2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72c.13.96.36 1.9.7 2.81a2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45c.91.34 1.85.57 2.81.7A2 2 0 0 1 22 16.92z" />
                    </svg>
                    Qo&apos;ng&apos;iroq
                  </a>
                ) : null}
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
