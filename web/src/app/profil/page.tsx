"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { apiFetch } from "@/lib/api";
import { useAuth, type LoyaltyInfo } from "@/context/AuthContext";
import { LoyaltyCard } from "@/components/LoyaltyCard";

type ProfileResponse = {
  id: string;
  phone: string;
  name: string | null;
  role: string;
  completedBookings: number;
  loyaltyBonuses: number;
  telegramUsername: string | null;
  telegramLinkedAt: string | null;
  noShowStreak: number;
  bookingBlockedUntil: string | null;
  loyalty: LoyaltyInfo;
};

export default function ProfilePage() {
  const { token, user, logout, setSession } = useAuth();
  const [profile, setProfile] = useState<ProfileResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);

  const [editing, setEditing] = useState(false);
  const [name, setName] = useState("");
  const [savingMsg, setSavingMsg] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const loadProfile = async () => {
    if (!token) {
      setLoading(false);
      return;
    }
    try {
      const data = await apiFetch<ProfileResponse>("/user/profile", { token });
      setProfile(data);
      setName(data.name ?? "");
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadProfile();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [token]);

  const saveName = async () => {
    if (!token) return;
    setBusy(true);
    setSavingMsg(null);
    setErr(null);
    try {
      const data = await apiFetch<ProfileResponse>("/user/profile", {
        method: "PATCH",
        token,
        body: JSON.stringify({ name: name.trim() }),
      });
      setProfile(data);
      if (user) {
        setSession(token, { ...user, name: data.name });
      }
      setEditing(false);
      setSavingMsg("Saqlandi");
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  if (!token) {
    return (
      <main className="mx-auto max-w-2xl px-4 py-10">
        <div className="rounded-2xl border border-[color:var(--border)] bg-white p-8 text-center shadow-[var(--shadow-sm)]">
          <div className="text-4xl">👋</div>
          <h1 className="mt-3 text-2xl font-extrabold text-[color:var(--fg)]">Profilingizga kirish</h1>
          <p className="mt-2 text-sm text-[color:var(--muted)]">
            Profilni ochish uchun tizimga kiring
          </p>
          <Link href="/bron" className="bq-btn bq-btn-primary mt-5 px-8">
            Kirish
          </Link>
        </div>
      </main>
    );
  }

  if (loading) {
    return (
      <main className="mx-auto max-w-3xl px-4 py-10">
        <div className="h-40 animate-pulse rounded-2xl bg-[color:var(--bg-soft)]" />
      </main>
    );
  }

  if (!profile) {
    return (
      <main className="mx-auto max-w-3xl px-4 py-10">
        <div className="rounded-2xl border border-rose-200 bg-rose-50 p-6 text-sm text-rose-700">
          {err ?? "Profilni yuklab bo'lmadi"}
        </div>
      </main>
    );
  }

  return (
    <main className="mx-auto max-w-3xl px-4 py-8">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-extrabold text-[color:var(--fg)] sm:text-3xl">
          Profilim
        </h1>
        <button
          type="button"
          onClick={logout}
          className="inline-flex h-9 items-center gap-1.5 rounded-full border border-[color:var(--border)] bg-white px-3 text-xs font-semibold text-[color:var(--muted)] hover:border-rose-400 hover:text-rose-700"
        >
          Chiqish
        </button>
      </div>

      {/* Name / phone */}
      <div className="mt-5 rounded-2xl border border-[color:var(--border)] bg-white p-5 shadow-[var(--shadow-sm)]">
        <div className="flex items-center gap-4">
          <div className="flex h-14 w-14 items-center justify-center rounded-full bg-[color:var(--brand)] text-xl font-extrabold text-white">
            {(profile.name || profile.phone).charAt(0).toUpperCase()}
          </div>
          <div className="min-w-0 flex-1">
            {editing ? (
              <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
                <input
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="Ismingiz"
                  className="flex-1 rounded-xl border border-[color:var(--border)] bg-white px-3 py-2 text-sm"
                  maxLength={80}
                />
                <div className="flex gap-2">
                  <button
                    type="button"
                    onClick={saveName}
                    disabled={busy}
                    className="bq-btn bq-btn-primary h-9 px-4 text-xs disabled:opacity-50"
                  >
                    {busy ? "…" : "Saqlash"}
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      setEditing(false);
                      setName(profile.name ?? "");
                    }}
                    className="bq-btn bq-btn-ghost h-9 px-4 text-xs"
                  >
                    Bekor
                  </button>
                </div>
              </div>
            ) : (
              <>
                <div className="flex items-center gap-2">
                  <span className="text-lg font-extrabold text-[color:var(--fg)]">
                    {profile.name || "Ismingiz kiritilmagan"}
                  </span>
                  <button
                    type="button"
                    onClick={() => setEditing(true)}
                    className="text-xs font-semibold text-[color:var(--brand-700)] hover:underline"
                  >
                    Tahrirlash
                  </button>
                </div>
                <div className="mt-0.5 text-sm text-[color:var(--muted)]">
                  {profile.phone}
                </div>
                {savingMsg ? (
                  <div className="mt-1 text-xs text-emerald-700">{savingMsg}</div>
                ) : null}
                {err ? <div className="mt-1 text-xs text-rose-600">{err}</div> : null}
              </>
            )}
          </div>
        </div>
      </div>

      {/* Stats */}
      <div className="mt-4 grid gap-3 sm:grid-cols-3">
        <StatCard
          icon="📝"
          label="Jami bronlar"
          value={profile.completedBookings}
        />
        <StatCard icon="🎁" label="Bonuslar" value={profile.loyaltyBonuses} />
        <StatCard
          icon="⚠️"
          label="No-show"
          value={profile.noShowStreak}
          muted={profile.noShowStreak === 0}
        />
      </div>

      {/* Loyalty */}
      <div className="mt-4">
        <LoyaltyCard loyalty={profile.loyalty} />
      </div>

      {/* Telegram */}
      <div className="mt-4 rounded-2xl border border-[color:var(--border)] bg-white p-5 shadow-[var(--shadow-sm)]">
        <div className="flex items-start gap-3">
          <div className="inline-flex h-10 w-10 items-center justify-center rounded-xl bg-[#e0f2fe] text-[#0284c7]">
            <svg width="22" height="22" viewBox="0 0 24 24" fill="currentColor">
              <path d="M22 3L2 11l6 2 2 6 3-4 5 4z" />
            </svg>
          </div>
          <div className="min-w-0 flex-1">
            <div className="text-sm font-bold text-[color:var(--fg)]">Telegram</div>
            {profile.telegramLinkedAt ? (
              <div className="mt-0.5 text-xs text-[color:var(--muted)]">
                {profile.telegramUsername ? `@${profile.telegramUsername} · ` : ""}
                {new Date(profile.telegramLinkedAt).toLocaleDateString("uz-UZ")} dan buyon
                ulangan
              </div>
            ) : (
              <div className="mt-0.5 text-xs text-[color:var(--muted)]">
                Ulanmagan — bronlar haqida xabar olmaysiz
              </div>
            )}
          </div>
          {!profile.telegramLinkedAt ? (
            <Link
              href="/bron"
              className="bq-btn bq-btn-primary h-9 px-3 text-xs"
            >
              Ulash
            </Link>
          ) : (
            <span className="inline-flex items-center gap-1 rounded-full bg-emerald-100 px-2.5 py-1 text-xs font-semibold text-emerald-800">
              ✓ Ulangan
            </span>
          )}
        </div>
      </div>

      {/* Blocked */}
      {profile.bookingBlockedUntil ? (
        <div className="mt-4 rounded-2xl border border-rose-200 bg-rose-50 p-4 text-sm text-rose-800">
          ⚠️ No-showlar ko&apos;payganligi sababli bron qilish{" "}
          <b>{new Date(profile.bookingBlockedUntil).toLocaleString("uz-UZ")}</b> gacha
          bloklangan.
        </div>
      ) : null}

      <div className="mt-4 grid gap-3 sm:grid-cols-2">
        <Link
          href="/mening-bronlarim"
          className="rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)] transition hover:border-[color:var(--brand)]"
        >
          <div className="text-lg">📋</div>
          <div className="mt-1 text-sm font-bold text-[color:var(--fg)]">Mening bronlarim</div>
          <div className="text-xs text-[color:var(--muted)]">Bronlar tarixi</div>
        </Link>
        <Link
          href="/bron"
          className="rounded-2xl border border-[color:var(--brand)] bg-[color:var(--brand)] p-4 text-white shadow-[0_8px_20px_rgba(27,122,78,0.24)] transition hover:brightness-110"
        >
          <div className="text-lg">🍽</div>
          <div className="mt-1 text-sm font-bold">Yangi bron</div>
          <div className="text-xs opacity-90">Stol band qilish</div>
        </Link>
      </div>
    </main>
  );
}

function StatCard({
  icon,
  label,
  value,
  muted,
}: {
  icon: string;
  label: string;
  value: number;
  muted?: boolean;
}) {
  return (
    <div className="rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)]">
      <div className="text-xl">{icon}</div>
      <div className="mt-1 text-[10px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
        {label}
      </div>
      <div
        className={`mt-0.5 text-2xl font-extrabold ${
          muted ? "text-[color:var(--muted)]" : "text-[color:var(--fg)]"
        }`}
      >
        {value}
      </div>
    </div>
  );
}
