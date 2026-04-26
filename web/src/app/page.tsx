"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { apiFetch } from "@/lib/api";
import { BranchCard } from "@/components/BranchCard";

type Branch = {
  id: string;
  name: string;
  address?: string | null;
  workHours?: unknown;
  description?: string | null;
  phone?: string | null;
  isActive?: boolean;
  _count?: { zones?: number };
  totalTables?: number;
  availableTables?: number;
};

export default function Home() {
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
    <>
      <HeroSection />
      <HowItWorks />
      <BranchesSection branches={branches} loading={loading} err={err} />
      <CallToAction />
    </>
  );
}

/* ────────────────────────────── HERO ────────────────────────────── */

function HeroSection() {
  return (
    <section className="relative overflow-hidden">
      {/* decorative background */}
      <div
        aria-hidden
        className="absolute inset-0 -z-10"
        style={{
          background:
            "radial-gradient(1200px 600px at 80% -10%, rgba(27,122,78,0.18), transparent 60%), radial-gradient(900px 500px at -10% 60%, rgba(233,185,73,0.12), transparent 60%), linear-gradient(180deg, #f7fbf8 0%, #ffffff 65%)",
        }}
      />
      <div
        aria-hidden
        className="absolute inset-0 -z-10 opacity-[0.06]"
        style={{
          backgroundImage:
            "radial-gradient(circle at 1px 1px, #1b7a4e 1px, transparent 0)",
          backgroundSize: "24px 24px",
        }}
      />

      <div className="bq-container grid gap-10 py-16 md:grid-cols-[1.1fr_1fr] md:py-24">
        <div className="flex flex-col justify-center">
          <span className="bq-chip w-fit">
            <span className="h-1.5 w-1.5 rounded-full bg-[color:var(--brand)]" />
            Onlayn bron · har safar o&apos;zingizga qulay joy
          </span>
          <h1 className="mt-5 text-4xl font-black leading-[1.1] tracking-tight text-[color:var(--fg)] md:text-5xl lg:text-6xl">
            Stolni bron qiling —
            <br />
            <span className="text-[color:var(--brand)]">o&apos;zingizga qulay joyni</span>
            {" "}tanlang
          </h1>
          <p className="mt-5 max-w-xl text-base leading-relaxed text-[color:var(--muted)] md:text-lg">
            Beshqozon restoranlarida bir necha daqiqada stol band qiling.
            Zalni xaritadan ko&apos;ring, oynalar oldi yoki VIP xonani tanlang,
            ertangi plov uchun joyingizni avvaldan ta&apos;minlang.
          </p>
          <div className="mt-8 flex flex-wrap gap-3">
            <Link href="/bron" className="bq-btn bq-btn-primary px-7 text-[15px]">
              Hozir bron qilish
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" aria-hidden>
                <path d="M5 12h14" />
                <path d="M13 6l6 6-6 6" />
              </svg>
            </Link>
            <Link href="#filiallar" className="bq-btn bq-btn-ghost px-7">
              Filiallarni ko&apos;rish
            </Link>
          </div>

          <dl className="mt-10 grid max-w-md grid-cols-3 gap-6">
            <Stat label="Filial" value="4+" />
            <Stat label="Kunlik bron" value="200+" />
            <Stat label="Reyting" value="4.8" unit="/5" />
          </dl>
        </div>

        {/* illustrative card */}
        <div className="relative">
          <div
            aria-hidden
            className="absolute -right-4 -top-4 h-40 w-40 rounded-full bg-[color:var(--brand-100)] blur-2xl"
          />
          <div className="relative rounded-[24px] border border-[color:var(--border)] bg-white/70 p-5 shadow-[0_20px_60px_rgba(17,24,39,0.12)] backdrop-blur">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-xs font-semibold uppercase tracking-widest text-[color:var(--brand-700)]">
                  Beshqozon · Glinka
                </div>
                <div className="mt-1 text-lg font-bold text-[color:var(--fg)]">
                  Bugun · 19:30
                </div>
              </div>
              <span className="inline-flex items-center gap-1.5 rounded-full bg-[color:var(--brand)] px-3 py-1 text-xs font-bold text-white">
                <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-white" />
                LIVE
              </span>
            </div>

            {/* mini floor plan preview */}
            <div className="mt-4 aspect-[4/3] w-full overflow-hidden rounded-2xl border border-[color:var(--border)] bg-[color:var(--bg-soft)] p-4">
              <MiniFloorPlan />
            </div>

            <div className="mt-4 flex flex-wrap items-center gap-2 text-xs">
              <LegendDot color="#22c55e" label="Bo'sh" />
              <LegendDot color="#ef4444" label="Band" />
              <LegendDot color="#3b82f6" label="Tanlangan" />
              <LegendDot color="#f59e0b" label="Kutilmoqda" />
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function Stat({ label, value, unit }: { label: string; value: string; unit?: string }) {
  return (
    <div>
      <dt className="text-xs font-semibold uppercase tracking-wider text-[color:var(--muted)]">
        {label}
      </dt>
      <dd className="mt-1 text-2xl font-extrabold text-[color:var(--fg)]">
        {value}
        {unit ? <span className="text-sm font-bold text-[color:var(--muted)]">{unit}</span> : null}
      </dd>
    </div>
  );
}

function LegendDot({ color, label }: { color: string; label: string }) {
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full border border-[color:var(--border)] bg-white px-2 py-0.5 text-[11px] font-medium text-[color:var(--fg)]">
      <span className="h-2 w-2 rounded-full" style={{ background: color }} />
      {label}
    </span>
  );
}

function MiniFloorPlan() {
  // Small decorative svg "floor plan" preview
  const tables = [
    { x: 10, y: 18, w: 18, h: 14, fill: "#22c55e" },
    { x: 36, y: 12, w: 14, h: 14, fill: "#22c55e" },
    { x: 58, y: 14, w: 20, h: 14, fill: "#ef4444" },
    { x: 14, y: 46, w: 14, h: 14, fill: "#22c55e" },
    { x: 36, y: 48, w: 22, h: 14, fill: "#3b82f6", stroke: "#1d4ed8" },
    { x: 66, y: 46, w: 16, h: 14, fill: "#f59e0b" },
    { x: 20, y: 74, w: 20, h: 14, fill: "#22c55e" },
    { x: 50, y: 76, w: 26, h: 12, fill: "#22c55e" },
  ];
  return (
    <svg viewBox="0 0 100 100" className="h-full w-full">
      {/* walls */}
      <rect x="2" y="2" width="96" height="96" rx="3" fill="#ffffff" stroke="#d1d5db" strokeWidth="0.8" />
      {/* kitchen strip */}
      <rect x="2" y="2" width="96" height="8" fill="#e5e7eb" />
      <text x="50" y="7.5" fontSize="3.2" textAnchor="middle" fill="#6b7280" fontWeight="700">
        OSHXONA
      </text>
      {/* bar */}
      <rect x="84" y="38" width="14" height="28" fill="#f3f4f6" stroke="#d1d5db" strokeWidth="0.5" />
      <text x="91" y="54" fontSize="2.6" textAnchor="middle" fill="#6b7280" fontWeight="700">
        BAR
      </text>
      {/* door */}
      <rect x="2" y="46" width="3" height="14" fill="#1b7a4e" opacity="0.2" />
      {/* tables */}
      {tables.map((t, i) => (
        <rect
          key={i}
          x={t.x}
          y={t.y}
          width={t.w}
          height={t.h}
          rx="2"
          fill={t.fill}
          stroke={t.stroke ?? "rgba(0,0,0,0.08)"}
          strokeWidth={t.stroke ? 1 : 0.4}
        />
      ))}
    </svg>
  );
}

/* ────────────────────────────── HOW IT WORKS ────────────────────────────── */

function HowItWorks() {
  const steps = [
    {
      icon: IconBranch,
      title: "Filialni tanlang",
      desc: "Eng yaqin yoki sevimli filialingizni tanlang.",
    },
    {
      icon: IconCalendar,
      title: "Sana va vaqtni tanlang",
      desc: "Qulay sanani va bo‘sh vaqt oralig‘ini belgilang.",
    },
    {
      icon: IconTable,
      title: "Stolni xaritadan tanlang",
      desc: "Zalning real xaritasida aniq stolni tanlang.",
    },
    {
      icon: IconPay,
      title: "Depozitni to‘lang",
      desc: "Payme yoki Click orqali depozit — bron tasdiqlanadi.",
    },
  ];

  return (
    <section className="bq-container py-16 md:py-20">
      <div className="mx-auto max-w-2xl text-center">
        <span className="bq-chip">Qanday ishlaydi?</span>
        <h2 className="mt-3 text-3xl font-extrabold tracking-tight text-[color:var(--fg)] md:text-4xl">
          4 qadamda — stolingiz tayyor
        </h2>
        <p className="mt-3 text-[color:var(--muted)]">
          Jarayon oddiy va aniq. Navbatga turish shart emas.
        </p>
      </div>
      <ol className="mt-12 grid gap-6 md:grid-cols-2 lg:grid-cols-4">
        {steps.map((s, i) => (
          <li key={s.title} className="bq-card bq-card-hover relative flex flex-col p-6">
            <span className="absolute right-4 top-4 inline-flex h-7 w-7 items-center justify-center rounded-full bg-[color:var(--brand-50)] text-xs font-bold text-[color:var(--brand-700)]">
              {i + 1}
            </span>
            <div className="inline-flex h-12 w-12 items-center justify-center rounded-xl bg-[color:var(--brand)] text-white shadow-[var(--shadow-sm)]">
              <s.icon />
            </div>
            <h3 className="mt-4 text-lg font-bold text-[color:var(--fg)]">{s.title}</h3>
            <p className="mt-1 text-sm text-[color:var(--muted)]">{s.desc}</p>
          </li>
        ))}
      </ol>
    </section>
  );
}

/* ────────────────────────────── BRANCHES ────────────────────────────── */

function BranchesSection({
  branches,
  loading,
  err,
}: {
  branches: Branch[];
  loading: boolean;
  err: string | null;
}) {
  return (
    <section id="filiallar" className="bq-container py-14 md:py-20">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <span className="bq-chip">Filiallar</span>
          <h2 className="mt-3 text-3xl font-extrabold tracking-tight text-[color:var(--fg)] md:text-4xl">
            Bizning joylarimiz
          </h2>
          <p className="mt-2 max-w-xl text-[color:var(--muted)]">
            Toshkent bo&apos;ylab 4 ta filialda sizni kutamiz. Har birida o&apos;ziga xos zonalar va qulayliklar.
          </p>
        </div>
        <Link href="/filiallar" className="bq-btn bq-btn-ghost">
          Barchasi
        </Link>
      </div>

      <div className="mt-8 grid gap-6 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
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
            Hozircha filiallar yo&apos;q. Admin panelidan qo&apos;shishingiz mumkin.
          </div>
        ) : (
          branches.map((b, i) => (
            <BranchCard
              key={b.id}
              branch={b}
              index={i}
              variant="link"
              href={`/bron?branch=${b.id}`}
            />
          ))
        )}
      </div>
    </section>
  );
}

/* ────────────────────────────── CTA ────────────────────────────── */

function CallToAction() {
  return (
    <section className="bq-container pb-20">
      <div
        className="relative overflow-hidden rounded-[28px] p-10 md:p-14"
        style={{
          background:
            "linear-gradient(135deg, #114f33 0%, #1b7a4e 55%, #22a06b 100%)",
        }}
      >
        <div
          aria-hidden
          className="absolute -right-10 -top-10 h-64 w-64 rounded-full bg-white/10 blur-2xl"
        />
        <div
          aria-hidden
          className="absolute -left-16 bottom-0 h-64 w-64 rounded-full bg-[color:var(--accent-warm)]/20 blur-2xl"
        />
        <div className="relative grid gap-6 md:grid-cols-[1.3fr_1fr] md:items-center">
          <div>
            <h3 className="text-3xl font-extrabold leading-tight text-white md:text-4xl">
              Ertangi oqshomni hozirdanoq rejalashtiring
            </h3>
            <p className="mt-3 max-w-lg text-white/85">
              3 daqiqada bron qiling, depozit ketganda stolingiz kafolatlangan. Keyingi tashvishlar yo&apos;q.
            </p>
          </div>
          <div className="flex flex-col items-start gap-3 md:items-end">
            <Link href="/bron" className="bq-btn bq-btn-primary bg-white text-[color:var(--brand-700)] hover:bg-white/90 px-7 text-[15px]">
              Bron qilish
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" aria-hidden>
                <path d="M5 12h14" />
                <path d="M13 6l6 6-6 6" />
              </svg>
            </Link>
            <span className="text-xs text-white/80">SMS orqali tasdiqlash · xavfsiz to&apos;lov</span>
          </div>
        </div>
      </div>
    </section>
  );
}

/* ────────────────────────────── ICONS ────────────────────────────── */

function IconBranch() {
  return (
    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
      <path d="M3 10.5L12 3l9 7.5" />
      <path d="M5 9.5V21h14V9.5" />
      <path d="M10 21v-6h4v6" />
    </svg>
  );
}
function IconCalendar() {
  return (
    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
      <rect x="3" y="5" width="18" height="16" rx="2" />
      <path d="M3 10h18" />
      <path d="M8 3v4M16 3v4" />
    </svg>
  );
}
function IconTable() {
  return (
    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
      <rect x="3" y="5" width="18" height="10" rx="1.5" />
      <path d="M6 19v-4M18 19v-4M3 9h18" />
    </svg>
  );
}
function IconPay() {
  return (
    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
      <rect x="3" y="6" width="18" height="12" rx="2" />
      <path d="M3 10h18" />
      <path d="M7 15h4" />
    </svg>
  );
}
