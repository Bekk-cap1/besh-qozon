"use client";

import { type ReactNode } from "react";

type Props = {
  title?: string;
  subtitle?: string;
  actions?: ReactNode;
  children: ReactNode;
};

/**
 * AdminShell — топбар с заголовком страницы и область контента.
 * Sidebar, login-gate и внешний каркас вынесены в `app/admin/layout.tsx`,
 * поэтому при переходах между admin-маршрутами sidebar остаётся неизменным,
 * а Next.js подменяет только main + header (через `loading.tsx`).
 */
export function AdminShell({ title, subtitle, actions, children }: Props) {
  return (
    <>
      <header className="sticky top-0 z-20 flex items-center gap-3 border-b border-[color:var(--border)] bg-white/90 px-4 py-3 backdrop-blur lg:px-8">
        <AdminMobileMenuButton />
        <div className="min-w-0 flex-1">
          {title ? (
            <h1 className="truncate text-[17px] font-extrabold tracking-tight text-[color:var(--fg)] lg:text-xl">
              {title}
            </h1>
          ) : null}
          {subtitle ? (
            <p className="truncate text-xs text-[color:var(--muted)]">{subtitle}</p>
          ) : null}
        </div>
        {actions ? <div className="flex items-center gap-2">{actions}</div> : null}
      </header>

      <main className="flex-1 px-4 py-6 lg:px-8 lg:py-8">{children}</main>
    </>
  );
}

/**
 * Мобильная бургер-кнопка показывается только на <lg.
 * Сам сайдбар-мобильный drawer управляется в layout, поэтому здесь — заглушка,
 * которая просто диспатчит событие. Layout слушает его и открывает sidebar.
 */
function AdminMobileMenuButton() {
  return (
    <button
      type="button"
      onClick={() => {
        if (typeof window !== "undefined") {
          window.dispatchEvent(new CustomEvent("admin-open-sidebar"));
        }
      }}
      className="inline-flex h-10 w-10 items-center justify-center rounded-xl border border-[color:var(--border)] text-[color:var(--muted)] lg:hidden"
      aria-label="Menyu"
    >
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
        <path d="M3 6h18M3 12h18M3 18h18" />
      </svg>
    </button>
  );
}
