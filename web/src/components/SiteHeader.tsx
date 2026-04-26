"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";
import { useAuth } from "@/context/AuthContext";
import { useI18n } from "@/i18n/I18nContext";
import { BeshqozonLogo } from "./BeshqozonLogo";

export function SiteHeader() {
  const pathname = usePathname() ?? "/";
  const { user, logout } = useAuth();
  const { lang, setLang, t } = useI18n();
  const [open, setOpen] = useState(false);

  const NAV_ITEMS = [
    { href: "/menyu", label: t("nav.menu") },
    { href: "/bron", label: t("nav.book") },
    { href: "/filiallar", label: t("nav.branches") },
  ];

  // close mobile menu on route change
  useEffect(() => {
    setOpen(false);
  }, [pathname]);

  // hide header chrome inside admin (admin has its own shell)
  const isAdmin = pathname.startsWith("/admin");
  if (isAdmin) return null;

  const isActive = (href: string) =>
    href === "/" ? pathname === "/" : pathname.startsWith(href);

  const initials = (user?.name || user?.phone || "")
    .replace(/[^A-Za-zА-Яа-я0-9]/g, "")
    .slice(0, 2)
    .toUpperCase();

  return (
    <header className="sticky top-0 z-30 border-b border-[color:var(--border)] bg-[color:var(--bg)]/85 backdrop-blur supports-[backdrop-filter]:bg-[color:var(--bg)]/70">
      <div className="bq-container flex h-16 items-center gap-4">
        <Link href="/" aria-label="Beshqozon — bosh sahifa" className="shrink-0">
          <BeshqozonLogo />
        </Link>

        {/* center nav (desktop) */}
        <nav className="mx-auto hidden items-center gap-8 md:flex" aria-label="Asosiy navigatsiya">
          {NAV_ITEMS.map((it) => (
            <Link
              key={it.href}
              href={it.href}
              aria-current={isActive(it.href) ? "page" : undefined}
              className="bq-nav-link text-sm"
            >
              {it.label}
            </Link>
          ))}
        </nav>

        {/* right side */}
        <div className="ml-auto hidden items-center gap-3 md:flex">
          <LangSwitcher lang={lang} setLang={setLang} />
          {user ? (
            <>
              <Link
                href="/mening-bronlarim"
                className="text-sm font-medium text-[color:var(--muted)] hover:text-[color:var(--fg)]"
              >
                {t("nav.my")}
              </Link>
              <Link
                href="/profil"
                className="flex items-center gap-2 rounded-full border border-[color:var(--border)] px-3 py-1.5 text-sm text-[color:var(--fg)] hover:bg-[color:var(--bg-soft)]"
                title="Profil"
              >
                <span className="inline-flex h-7 w-7 items-center justify-center rounded-full bg-[color:var(--brand-50)] text-[11px] font-bold text-[color:var(--brand-700)]">
                  {initials || "U"}
                </span>
                <span className="max-w-[140px] truncate">
                  {user.name || user.phone}
                </span>
              </Link>
              <button
                type="button"
                onClick={logout}
                className="text-sm font-medium text-[color:var(--muted)] hover:text-[color:var(--fg)]"
              >
                {t("nav.logout")}
              </button>
            </>
          ) : (
            <>
              <Link
                href="/admin"
                className="text-sm font-medium text-[color:var(--muted)] hover:text-[color:var(--fg)]"
              >
                {t("nav.admin")}
              </Link>
              <Link href="/bron" className="bq-btn bq-btn-primary h-10 px-5">
                {t("nav.book")}
              </Link>
            </>
          )}
        </div>

        {/* mobile burger */}
        <button
          type="button"
          aria-label={open ? "Menyuni yopish" : "Menyuni ochish"}
          aria-expanded={open}
          onClick={() => setOpen((v) => !v)}
          className="ml-auto inline-flex h-10 w-10 items-center justify-center rounded-lg border border-[color:var(--border)] md:hidden"
        >
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
            {open ? (
              <>
                <path d="M6 6l12 12" />
                <path d="M18 6L6 18" />
              </>
            ) : (
              <>
                <path d="M4 7h16" />
                <path d="M4 12h16" />
                <path d="M4 17h16" />
              </>
            )}
          </svg>
        </button>
      </div>

      {/* mobile dropdown */}
      {open ? (
        <div className="border-t border-[color:var(--border)] bg-[color:var(--bg)] md:hidden">
          <div className="bq-container flex flex-col gap-1 py-3">
            {NAV_ITEMS.map((it) => (
              <Link
                key={it.href}
                href={it.href}
                aria-current={isActive(it.href) ? "page" : undefined}
                className="rounded-lg px-3 py-2 text-sm font-medium text-[color:var(--fg)] hover:bg-[color:var(--bg-soft)] aria-[current=page]:bg-[color:var(--brand-50)] aria-[current=page]:text-[color:var(--brand-700)]"
              >
                {it.label}
              </Link>
            ))}
            <div className="my-2 h-px bg-[color:var(--border)]" />
            <div className="flex items-center justify-between px-3 py-1">
              <span className="text-[10px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
                Til · Язык
              </span>
              <LangSwitcher lang={lang} setLang={setLang} />
            </div>
            <div className="my-2 h-px bg-[color:var(--border)]" />
            {user ? (
              <>
                <Link
                  href="/profil"
                  className="flex items-center gap-2 rounded-lg px-3 py-2 text-sm text-[color:var(--fg)] hover:bg-[color:var(--bg-soft)]"
                >
                  <span className="inline-flex h-7 w-7 items-center justify-center rounded-full bg-[color:var(--brand-50)] text-[11px] font-bold text-[color:var(--brand-700)]">
                    {initials || "U"}
                  </span>
                  {user.name || user.phone}
                </Link>
                <Link
                  href="/mening-bronlarim"
                  className="rounded-lg px-3 py-2 text-sm text-[color:var(--fg)] hover:bg-[color:var(--bg-soft)]"
                >
                  Mening bronlarim
                </Link>
                <button
                  type="button"
                  onClick={logout}
                  className="rounded-lg px-3 py-2 text-left text-sm font-medium text-[color:var(--muted)] hover:bg-[color:var(--bg-soft)]"
                >
                  Chiqish
                </button>
              </>
            ) : (
              <>
                <Link
                  href="/admin"
                  className="rounded-lg px-3 py-2 text-sm text-[color:var(--muted)] hover:bg-[color:var(--bg-soft)]"
                >
                  Admin
                </Link>
                <Link href="/bron" className="bq-btn bq-btn-primary mt-1">
                  Bron qilish
                </Link>
              </>
            )}
          </div>
        </div>
      ) : null}
    </header>
  );
}

function LangSwitcher({
  lang,
  setLang,
}: {
  lang: "uz" | "ru";
  setLang: (l: "uz" | "ru") => void;
}) {
  return (
    <div className="inline-flex items-center overflow-hidden rounded-full border border-[color:var(--border)] bg-white">
      {(["uz", "ru"] as const).map((l) => (
        <button
          key={l}
          type="button"
          onClick={() => setLang(l)}
          className={`px-2.5 py-1 text-[11px] font-bold uppercase tracking-wider transition ${
            lang === l
              ? "bg-[color:var(--brand)] text-white"
              : "text-[color:var(--muted)] hover:text-[color:var(--fg)]"
          }`}
          aria-pressed={lang === l}
        >
          {l}
        </button>
      ))}
    </div>
  );
}
