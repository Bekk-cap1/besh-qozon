"use client";

/**
 * Карточка-билет с QR-кодом после успешной оплаты.
 * QR содержит reservationId — админ может сверить на входе (скан → admin/bronlar).
 */

type Props = {
  reservationId: string;
  branchName?: string | null;
  address?: string | null;
  tableNumber?: string | null;
  startAt?: string | null;
  guests?: number | null;
  userName?: string | null;
  userPhone?: string | null;
};

export function shortBookingCode(uuid: string): string {
  const clean = uuid.replace(/-/g, "").toUpperCase();
  return `BQ-${clean.slice(0, 6)}`;
}

export function ReservationTicket({
  reservationId,
  branchName,
  address,
  tableNumber,
  startAt,
  guests,
  userName,
  userPhone,
}: Props) {
  const qrSrc = `https://api.qrserver.com/v1/create-qr-code/?size=280x280&margin=12&data=${encodeURIComponent(reservationId)}`;
  const code = shortBookingCode(reservationId);
  const when = startAt ? new Date(startAt).toLocaleString("uz-UZ", {
    day: "2-digit",
    month: "long",
    hour: "2-digit",
    minute: "2-digit",
  }) : null;

  return (
    <div className="overflow-hidden rounded-2xl border-2 border-[color:var(--brand)] bg-gradient-to-br from-[color:var(--brand-50)] to-white shadow-[0_20px_50px_rgba(27,122,78,0.18)]">
      {/* Header */}
      <div className="flex items-center gap-3 bg-[color:var(--brand)] px-5 py-3 text-white">
        <span className="flex h-9 w-9 items-center justify-center rounded-full bg-white/20">
          <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
            <path d="M5 12l4.5 4.5L19 7" />
          </svg>
        </span>
        <div>
          <div className="text-[11px] font-bold uppercase tracking-wider opacity-80">
            Bron tasdiqlandi
          </div>
          <div className="text-lg font-extrabold leading-tight">
            Kelishingizni kutamiz!
          </div>
        </div>
      </div>

      {/* Body */}
      <div className="grid gap-4 p-5 sm:grid-cols-[auto_1fr]">
        <div className="flex flex-col items-center gap-2">
          <div className="rounded-xl border border-[color:var(--border)] bg-white p-2 shadow-sm">
            {/* eslint-disable-next-line @next/next/no-img-element */}
            <img src={qrSrc} alt="QR" width={200} height={200} className="block" />
          </div>
          <div className="rounded-full bg-white px-3 py-1 font-mono text-sm font-bold text-[color:var(--fg)] shadow-sm">
            {code}
          </div>
        </div>

        <div className="flex flex-col gap-2">
          <TicketRow icon="📍" label="Filial" value={branchName ?? "—"} sub={address ?? undefined} />
          <TicketRow icon="🕒" label="Sana va vaqt" value={when ?? "—"} />
          <TicketRow icon="🪑" label="Stol" value={tableNumber ? `T-${tableNumber}` : "—"} />
          <TicketRow icon="👥" label="Mehmonlar" value={guests ? `${guests} kishi` : "—"} />
          <TicketRow icon="👤" label="Mijoz" value={userName || userPhone || "—"} sub={userName ? userPhone ?? undefined : undefined} />
        </div>
      </div>

      {/* Footer */}
      <div className="flex flex-wrap items-center justify-between gap-3 border-t border-dashed border-[color:var(--brand)]/30 px-5 py-3 text-xs text-[color:var(--muted)]">
        <div className="flex items-center gap-1.5">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
            <path d="M12 9v4M12 17h.01" />
            <circle cx="12" cy="12" r="10" />
          </svg>
          Xodimga QR yoki <b className="font-mono">{code}</b> kodini ko&apos;rsating.
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => {
              if (typeof window !== "undefined") window.print();
            }}
            className="rounded-full border border-[color:var(--border)] bg-white px-3 py-1 font-semibold text-[color:var(--fg)] hover:border-[color:var(--brand)]"
          >
            🖨 Chop etish
          </button>
          <a
            href={qrSrc}
            download={`beshqozon-${code}.png`}
            className="rounded-full border border-[color:var(--border)] bg-white px-3 py-1 font-semibold text-[color:var(--fg)] hover:border-[color:var(--brand)]"
          >
            ⬇ QR yuklash
          </a>
        </div>
      </div>
    </div>
  );
}

function TicketRow({
  icon,
  label,
  value,
  sub,
}: {
  icon: string;
  label: string;
  value: string;
  sub?: string;
}) {
  return (
    <div className="flex items-start gap-2.5">
      <span className="mt-0.5 inline-block text-base">{icon}</span>
      <div className="flex-1">
        <div className="text-[10px] font-bold uppercase tracking-wider text-[color:var(--muted-2)]">
          {label}
        </div>
        <div className="text-sm font-semibold text-[color:var(--fg)]">{value}</div>
        {sub ? <div className="text-xs text-[color:var(--muted)]">{sub}</div> : null}
      </div>
    </div>
  );
}
