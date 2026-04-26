"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import type { ReactNode } from "react";
import { BeshqozonLogo } from "@/components/BeshqozonLogo";

type NavItem = {
  href: string;
  label: string;
  icon: ReactNode;
  exact?: boolean;
};

const IconDash = (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <rect x="3" y="3" width="7" height="9" rx="1.5" />
    <rect x="14" y="3" width="7" height="5" rx="1.5" />
    <rect x="14" y="12" width="7" height="9" rx="1.5" />
    <rect x="3" y="16" width="7" height="5" rx="1.5" />
  </svg>
);
const IconCalendar = (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <rect x="3" y="5" width="18" height="16" rx="2" />
    <path d="M3 9h18M8 3v4M16 3v4" />
  </svg>
);
const IconTables = (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <rect x="3" y="7" width="8" height="8" rx="1" />
    <rect x="13" y="4" width="8" height="5" rx="1" />
    <rect x="13" y="13" width="8" height="8" rx="1" />
  </svg>
);
const IconBranches = (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M3 21h18" />
    <path d="M5 21V8l7-5 7 5v13" />
    <path d="M10 21v-6h4v6" />
  </svg>
);
const IconSettings = (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="12" cy="12" r="3" />
    <path d="M19.4 15a1.6 1.6 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.6 1.6 0 0 0-1.8-.3 1.6 1.6 0 0 0-1 1.5V21a2 2 0 1 1-4 0v-.1a1.6 1.6 0 0 0-1-1.5 1.6 1.6 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.6 1.6 0 0 0 .3-1.8 1.6 1.6 0 0 0-1.5-1H3a2 2 0 1 1 0-4h.1a1.6 1.6 0 0 0 1.5-1 1.6 1.6 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.6 1.6 0 0 0 1.8.3h0a1.6 1.6 0 0 0 1-1.5V3a2 2 0 1 1 4 0v.1a1.6 1.6 0 0 0 1 1.5 1.6 1.6 0 0 0 1.8-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.6 1.6 0 0 0-.3 1.8v0a1.6 1.6 0 0 0 1.5 1H21a2 2 0 1 1 0 4h-.1a1.6 1.6 0 0 0-1.5 1z" />
  </svg>
);
const IconReports = (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M3 3v18h18" />
    <path d="M7 16l4-6 4 3 6-8" />
  </svg>
);
const IconMenu = (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M11 9V3l5 3-5 3z" />
    <path d="M3 9c0 4.5 3 7 5 7v5" />
    <path d="M21 9c0 4.5-3 7-5 7v5" />
  </svg>
);
const IconReviews = (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M12 2l3 6 7 1-5 5 1 7-6-3-6 3 1-7-5-5 7-1z" />
  </svg>
);
const IconUsers = (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" />
    <circle cx="9" cy="7" r="4" />
    <path d="M23 21v-2a4 4 0 0 0-3-3.87" />
    <path d="M16 3.13a4 4 0 0 1 0 7.75" />
  </svg>
);

const NAV: NavItem[] = [
  { href: "/admin", label: "Dashboard", icon: IconDash, exact: true },
  { href: "/admin/bronlar", label: "Bronlar", icon: IconCalendar },
  { href: "/admin/zonalar", label: "Zonalar va stollar", icon: IconTables },
  { href: "/admin/menyu", label: "Menyu", icon: IconMenu },
  { href: "/admin/filiallar", label: "Filiallar", icon: IconBranches },
  { href: "/admin/foydalanuvchilar", label: "Foydalanuvchilar", icon: IconUsers },
  { href: "/admin/sharhlar", label: "Sharhlar", icon: IconReviews },
  { href: "/admin/hisobotlar", label: "Hisobotlar", icon: IconReports },
  { href: "/admin/sozlamalar", label: "Sozlamalar", icon: IconSettings },
];

type Props = {
  open: boolean;
  onClose: () => void;
  onLogout: () => void;
  phone: string | null;
};

export function AdminSidebar({ open, onClose, onLogout, phone }: Props) {
  const pathname = usePathname();

  const isActive = (item: NavItem) => {
    if (!pathname) return false;
    if (item.exact) return pathname === item.href;
    return pathname === item.href || pathname.startsWith(item.href + "/");
  };

  return (
    <>
      {/* Мобильный оверлей */}
      {open ? (
        <div
          className="fixed inset-0 z-30 bg-black/40 lg:hidden"
          onClick={onClose}
          aria-hidden
        />
      ) : null}

      <aside
        className={`fixed inset-y-0 left-0 z-40 flex w-64 flex-col border-r border-[color:var(--border)] bg-white transition-transform lg:sticky lg:top-0 lg:h-screen lg:translate-x-0 ${
          open ? "translate-x-0" : "-translate-x-full lg:translate-x-0"
        }`}
      >
        {/* Лого/заголовок */}
        <div className="flex items-center justify-between gap-2 border-b border-[color:var(--border)] px-4 py-4">
          <Link href="/admin" className="flex items-center" onClick={onClose}>
            <BeshqozonLogo />
          </Link>
          <button
            type="button"
            onClick={onClose}
            className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-[color:var(--border)] text-[color:var(--muted)] lg:hidden"
            aria-label="Yopish"
          >
            ✕
          </button>
        </div>

        {/* Метка */}
        <div className="px-4 pt-4">
          <div className="text-[10px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
            Admin panel
          </div>
        </div>

        {/* Навигация */}
        <nav className="min-h-0 flex-1 overflow-y-auto space-y-1 p-3 pt-2">
          {NAV.map((item) => {
            const active = isActive(item);
            return (
              <Link
                key={item.href}
                href={item.href}
                onClick={onClose}
                className={`flex items-center gap-3 rounded-xl px-3 py-2.5 text-sm font-semibold transition ${
                  active
                    ? "bg-[color:var(--brand)] text-white shadow-[0_4px_14px_rgba(27,122,78,0.28)]"
                    : "text-[color:var(--fg)] hover:bg-[color:var(--bg-soft)]"
                }`}
              >
                <span className={active ? "text-white" : "text-[color:var(--muted)]"}>
                  {item.icon}
                </span>
                {item.label}
              </Link>
            );
          })}
        </nav>

        {/* Профиль/выход */}
        <div className="border-t border-[color:var(--border)] p-3">
          <Link
            href="/"
            className="mb-2 flex items-center gap-2 rounded-xl px-3 py-2 text-xs font-semibold text-[color:var(--muted)] transition hover:bg-[color:var(--bg-soft)]"
          >
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M19 12H5" />
              <path d="M12 5l-7 7 7 7" />
            </svg>
            Saytga qaytish
          </Link>
          <div className="flex items-center gap-2 rounded-xl bg-[color:var(--bg-soft)] p-2.5">
            <span className="inline-flex h-8 w-8 items-center justify-center rounded-full bg-[color:var(--brand)] text-xs font-bold text-white">
              A
            </span>
            <div className="min-w-0 flex-1 text-xs">
              <div className="truncate font-semibold text-[color:var(--fg)]">Administrator</div>
              <div className="truncate text-[color:var(--muted)]">{phone ?? "—"}</div>
            </div>
            <button
              type="button"
              onClick={onLogout}
              className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-[color:var(--border)] text-[color:var(--muted)] transition hover:border-[color:var(--danger)] hover:text-[color:var(--danger)]"
              aria-label="Chiqish"
              title="Chiqish"
            >
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
                <path d="M16 17l5-5-5-5" />
                <path d="M21 12H9" />
              </svg>
            </button>
          </div>
        </div>
      </aside>
    </>
  );
}
