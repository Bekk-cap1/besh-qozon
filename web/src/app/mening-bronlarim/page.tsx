"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { apiFetch } from "@/lib/api";
import { useAuth, type LoyaltyInfo } from "@/context/AuthContext";
import { MapLinkButton } from "@/components/MapLinkButton";
import { shortBookingCode } from "@/components/ReservationTicket";
import { LoyaltyCard } from "@/components/LoyaltyCard";
import { ReviewForm } from "@/components/ReviewForm";

type Reservation = {
  id: string;
  branchId: string;
  branch?: { id: string; name: string; address?: string | null; lat?: number | null; lng?: number | null } | null;
  tableId: string;
  table?: {
    id: string;
    number: string;
    seats?: number;
    type?: string;
    zone?: { id: string; name: string } | null;
  } | null;
  startAt: string;
  endAt: string;
  guestsCount: number;
  status: string;
  depositAmount?: number | string | null;
  preorderItems?: Array<{
    id: string;
    name: string;
    quantity: number;
    unitPrice: number | string;
  }>;
  createdAt: string;
};

type Tab = "upcoming" | "past" | "all";

export default function MyReservationsPage() {
  const { user, token } = useAuth();
  const [items, setItems] = useState<Reservation[]>([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);
  const [tab, setTab] = useState<Tab>("upcoming");
  const [cancellingId, setCancellingId] = useState<string | null>(null);
  const [actionMsg, setActionMsg] = useState<string | null>(null);
  const [loyalty, setLoyalty] = useState<LoyaltyInfo | null>(null);

  const reload = async () => {
    if (!token) return;
    try {
      const [data, profile] = await Promise.all([
        apiFetch<Reservation[]>("/user/reservations", { token }),
        apiFetch<{ loyalty?: LoyaltyInfo }>("/user/profile", { token }).catch(() => null),
      ]);
      setItems(data);
      if (profile?.loyalty) setLoyalty(profile.loyalty);
      setErr(null);
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!token) {
      setLoading(false);
      return;
    }
    void reload();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [token]);

  const { upcoming, past } = useMemo(() => {
    const now = Date.now();
    const up: Reservation[] = [];
    const ps: Reservation[] = [];
    for (const r of items) {
      const t = new Date(r.startAt).getTime();
      const isActive =
        r.status === "PENDING_PAYMENT" || r.status === "CONFIRMED";
      if (isActive && t > now - 30 * 60 * 1000) up.push(r);
      else ps.push(r);
    }
    up.sort((a, b) => new Date(a.startAt).getTime() - new Date(b.startAt).getTime());
    ps.sort((a, b) => new Date(b.startAt).getTime() - new Date(a.startAt).getTime());
    return { upcoming: up, past: ps };
  }, [items]);

  const visible = tab === "upcoming" ? upcoming : tab === "past" ? past : items;

  const cancel = async (id: string) => {
    if (!token) return;
    if (!confirm("Broningizni bekor qilishni xohlaysizmi?")) return;
    setCancellingId(id);
    setActionMsg(null);
    try {
      await apiFetch(`/reservations/${id}`, { method: "DELETE", token });
      setActionMsg("Bron bekor qilindi");
      await reload();
    } catch (e) {
      setActionMsg((e as Error).message);
    } finally {
      setCancellingId(null);
    }
  };

  if (!user) {
    return (
      <div className="bq-container py-16">
        <div className="mx-auto max-w-md text-center">
          <span className="bq-chip">Kirish kerak</span>
          <h1 className="mt-3 text-3xl font-extrabold text-[color:var(--fg)]">
            Bronlaringizni ko&apos;rish uchun kiring
          </h1>
          <p className="mt-3 text-[color:var(--muted)]">
            Telefon raqamingizni SMS yoki Telegram orqali tasdiqlang.
          </p>
          <div className="mt-6 flex justify-center gap-3">
            <Link href="/bron" className="bq-btn bq-btn-primary px-7">
              Tizimga kirish
            </Link>
            <Link href="/" className="bq-btn bq-btn-ghost px-7">
              Bosh sahifa
            </Link>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="bq-container py-10 md:py-14">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <span className="bq-chip">Mening bronlarim</span>
          <h1 className="mt-3 text-3xl font-extrabold tracking-tight text-[color:var(--fg)] md:text-4xl">
            Bronlaringiz tarixi
          </h1>
          <p className="mt-2 max-w-xl text-[color:var(--muted)]">
            Kelayotgan bronlaringizni boshqaring va bir bosishda qayta bron qiling.
          </p>
        </div>
        <Link href="/bron" className="bq-btn bq-btn-primary">
          + Yangi bron
        </Link>
      </div>

      <div className="mt-6 flex flex-wrap items-center gap-2">
        <TabButton active={tab === "upcoming"} onClick={() => setTab("upcoming")}>
          Kelayotgan{" "}
          <span className="ml-1 rounded-full bg-white/60 px-1.5 text-[10px]">
            {upcoming.length}
          </span>
        </TabButton>
        <TabButton active={tab === "past"} onClick={() => setTab("past")}>
          O&apos;tgan <span className="ml-1 rounded-full bg-white/60 px-1.5 text-[10px]">{past.length}</span>
        </TabButton>
        <TabButton active={tab === "all"} onClick={() => setTab("all")}>
          Barchasi <span className="ml-1 rounded-full bg-white/60 px-1.5 text-[10px]">{items.length}</span>
        </TabButton>
      </div>

      {loyalty ? <LoyaltyCard loyalty={loyalty} className="mt-6" /> : null}

      {actionMsg ? (
        <div className="mt-4 rounded-xl border border-amber-200 bg-amber-50 px-4 py-2 text-sm text-amber-900">
          {actionMsg}
        </div>
      ) : null}

      <div className="mt-6 space-y-4">
        {loading ? (
          Array.from({ length: 3 }).map((_, i) => (
            <div key={i} className="bq-card p-5">
              <div className="h-4 w-1/3 animate-pulse rounded bg-[color:var(--bg-soft)]" />
              <div className="mt-3 h-3 w-2/3 animate-pulse rounded bg-[color:var(--bg-soft)]" />
            </div>
          ))
        ) : err ? (
          <div className="rounded-xl border border-[color:var(--border)] bg-[color:var(--bg-soft)] p-6 text-sm text-[color:var(--muted)]">
            Bronlarni yuklab bo&apos;lmadi: {err}
          </div>
        ) : visible.length === 0 ? (
          <EmptyState tab={tab} />
        ) : (
          visible.map((r) => (
            <ReservationCard
              key={r.id}
              r={r}
              busy={cancellingId === r.id}
              onCancel={() => cancel(r.id)}
              token={token}
            />
          ))
        )}
      </div>
    </div>
  );
}

function TabButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={[
        "inline-flex items-center gap-1 rounded-full px-4 py-2 text-sm font-semibold transition",
        active
          ? "bg-[color:var(--brand)] text-white shadow-[0_4px_12px_rgba(27,122,78,0.25)]"
          : "border border-[color:var(--border)] bg-white text-[color:var(--fg)] hover:border-[color:var(--brand)]",
      ].join(" ")}
    >
      {children}
    </button>
  );
}

function EmptyState({ tab }: { tab: Tab }) {
  return (
    <div className="rounded-xl border border-[color:var(--border)] bg-[color:var(--bg-soft)] p-8 text-center">
      <div className="text-3xl">🪑</div>
      <div className="mt-3 text-lg font-bold text-[color:var(--fg)]">
        {tab === "upcoming"
          ? "Kelayotgan bronlaringiz yo'q"
          : tab === "past"
            ? "O'tgan bronlar yo'q"
            : "Bronlar yo'q"}
      </div>
      <p className="mt-1 text-sm text-[color:var(--muted)]">
        Birinchi bronni hoziroq yarating.
      </p>
      <Link href="/bron" className="bq-btn bq-btn-primary mt-5 px-7">
        Bron qilish
      </Link>
    </div>
  );
}

const STATUS_BADGE: Record<string, { bg: string; fg: string; label: string; dot: string }> = {
  PENDING_PAYMENT: { bg: "#fef3c7", fg: "#92400e", dot: "#f59e0b", label: "To'lov kutilmoqda" },
  CONFIRMED: { bg: "#dcfce7", fg: "#166534", dot: "#22c55e", label: "Tasdiqlangan" },
  COMPLETED: { bg: "#e0e7ff", fg: "#3730a3", dot: "#6366f1", label: "Tugallangan" },
  NO_SHOW: { bg: "#fee2e2", fg: "#991b1b", dot: "#ef4444", label: "Kelmadi" },
  CANCELLED_BY_USER: { bg: "#f3f4f6", fg: "#4b5563", dot: "#9ca3af", label: "Bekor qilindi" },
  CANCELLED_BY_RESTAURANT: { bg: "#fee2e2", fg: "#991b1b", dot: "#ef4444", label: "Restoran bekor qildi" },
};

function ReservationCard({
  r,
  busy,
  onCancel,
  token,
}: {
  r: Reservation;
  busy: boolean;
  onCancel: () => void;
  token: string | null;
}) {
  const s = STATUS_BADGE[r.status] ?? {
    bg: "#f3f4f6",
    fg: "#4b5563",
    dot: "#9ca3af",
    label: r.status,
  };
  const when = new Date(r.startAt);
  const code = shortBookingCode(r.id);
  const canCancel = r.status === "PENDING_PAYMENT" || r.status === "CONFIRMED";
  const canShowQR = r.status === "CONFIRMED" || r.status === "COMPLETED";

  // Ссылка повтора: /bron?branch=<id>&guests=<n>&zone=<zoneId>
  const repeatHref = `/bron?branch=${r.branchId}&guests=${r.guestsCount}${
    r.table?.zone?.id ? `&zone=${r.table.zone.id}` : ""
  }`;

  return (
    <div className="bq-card overflow-hidden transition hover:shadow-[var(--shadow-md)]">
      <div className="grid gap-4 p-5 md:grid-cols-[1fr_auto]">
        <div className="flex min-w-0 flex-col gap-2">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-lg font-extrabold text-[color:var(--fg)]">
              {r.branch?.name ?? "Filial"}
            </span>
            <span
              className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[11px] font-bold"
              style={{ background: s.bg, color: s.fg }}
            >
              <span className="h-1.5 w-1.5 rounded-full" style={{ background: s.dot }} />
              {s.label}
            </span>
            <span className="rounded-full bg-[color:var(--bg-soft)] px-2 py-0.5 font-mono text-[11px] text-[color:var(--muted)]">
              {code}
            </span>
          </div>
          {r.branch?.address ? (
            <div className="text-sm text-[color:var(--muted)]">{r.branch.address}</div>
          ) : null}

          <div className="mt-1 flex flex-wrap items-center gap-x-5 gap-y-1 text-sm">
            <span className="inline-flex items-center gap-1.5">
              <span aria-hidden>📅</span>
              <b className="text-[color:var(--fg)]">
                {when.toLocaleString("uz-UZ", {
                  day: "2-digit",
                  month: "long",
                  hour: "2-digit",
                  minute: "2-digit",
                })}
              </b>
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span aria-hidden>👥</span>
              {r.guestsCount} kishi
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span aria-hidden>🪑</span>
              T-{r.table?.number ?? "—"}
              {r.table?.zone?.name ? (
                <span className="text-[color:var(--muted)]"> · {r.table.zone.name}</span>
              ) : null}
            </span>
          </div>

          {r.preorderItems && r.preorderItems.length > 0 ? (
            <div className="mt-2 rounded-xl border border-[color:var(--border)] bg-[color:var(--bg-soft)] p-3">
              <div className="text-[10px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
                Oldindan buyurtma · {r.preorderItems.length} ta pozitsiya
              </div>
              <ul className="mt-1 space-y-0.5 text-xs">
                {r.preorderItems.slice(0, 4).map((it) => (
                  <li key={it.id} className="flex justify-between gap-3">
                    <span className="truncate">
                      <b>{it.quantity}×</b> {it.name}
                    </span>
                    <span className="whitespace-nowrap text-[color:var(--muted)]">
                      {(Number(it.unitPrice) * it.quantity).toLocaleString("ru-RU")} so&apos;m
                    </span>
                  </li>
                ))}
                {r.preorderItems.length > 4 ? (
                  <li className="text-[color:var(--muted)]">
                    + yana {r.preorderItems.length - 4}
                  </li>
                ) : null}
              </ul>
              <div className="mt-1 flex justify-between gap-3 border-t border-[color:var(--border)] pt-1 text-xs font-extrabold">
                <span>Jami:</span>
                <span className="text-[color:var(--brand-700)]">
                  {r.preorderItems
                    .reduce((s, it) => s + Number(it.unitPrice) * it.quantity, 0)
                    .toLocaleString("ru-RU")}{" "}
                  so&apos;m
                </span>
              </div>
            </div>
          ) : null}

          <div className="mt-2 flex flex-wrap gap-2">
            <Link
              href={repeatHref}
              className="inline-flex h-9 items-center gap-1.5 rounded-full border border-[color:var(--border)] bg-white px-3 text-xs font-semibold text-[color:var(--fg)] hover:border-[color:var(--brand)] hover:text-[color:var(--brand-700)]"
            >
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <polyline points="23 4 23 10 17 10" />
                <path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10" />
              </svg>
              Qayta bron qilish
            </Link>
            <MapLinkButton
              name={r.branch?.name}
              address={r.branch?.address}
              lat={r.branch?.lat}
              lng={r.branch?.lng}
              variant="pill"
            />
            {canCancel ? (
              <button
                type="button"
                onClick={onCancel}
                disabled={busy}
                className="inline-flex h-9 items-center gap-1.5 rounded-full border border-rose-200 bg-white px-3 text-xs font-semibold text-rose-700 hover:border-rose-400 hover:bg-rose-50 disabled:opacity-50"
              >
                {busy ? "…" : "Bekor qilish"}
              </button>
            ) : null}
          </div>
        </div>

        {/* Right: QR или стрелка */}
        {canShowQR ? (
          <div className="flex flex-col items-center gap-1.5 md:w-[140px]">
            <div className="rounded-xl border border-[color:var(--border)] bg-white p-1.5 shadow-sm">
              {/* eslint-disable-next-line @next/next/no-img-element */}
              <img
                src={`https://api.qrserver.com/v1/create-qr-code/?size=140x140&margin=6&data=${encodeURIComponent(r.id)}`}
                alt="QR"
                width={120}
                height={120}
              />
            </div>
            <a
              href={`https://api.qrserver.com/v1/create-qr-code/?size=600x600&margin=16&data=${encodeURIComponent(r.id)}`}
              download={`beshqozon-${code}.png`}
              className="text-[11px] font-semibold text-[color:var(--brand-700)] hover:underline"
            >
              ⬇ Yuklash
            </a>
          </div>
        ) : null}
      </div>

      {r.status === "COMPLETED" && token ? (
        <div className="border-t border-[color:var(--border)] bg-[color:var(--bg-soft)] p-5">
          <ReviewForm reservationId={r.id} token={token} />
        </div>
      ) : null}
    </div>
  );
}
