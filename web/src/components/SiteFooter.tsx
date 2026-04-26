"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { BeshqozonLogo } from "./BeshqozonLogo";

export function SiteFooter() {
  const pathname = usePathname() ?? "/";
  if (pathname.startsWith("/admin")) return null;

  return (
    <footer className="mt-20 border-t border-[color:var(--border)] bg-[color:var(--bg-soft)]">
      <div className="bq-container grid gap-8 py-10 md:grid-cols-4">
        <div>
          <BeshqozonLogo />
          <p className="mt-3 max-w-xs text-sm text-[color:var(--muted)]">
            Onlayn stol bron qilish — milliy oshxona, shinam muhit, qulay tizim.
          </p>
        </div>
        <div>
          <h4 className="text-sm font-semibold text-[color:var(--fg)]">Menyu</h4>
          <ul className="mt-3 space-y-2 text-sm text-[color:var(--muted)]">
            <li><Link href="/menyu" className="hover:text-[color:var(--fg)]">Asosiy taomlar</Link></li>
            <li><Link href="/menyu" className="hover:text-[color:var(--fg)]">Ichimliklar</Link></li>
            <li><Link href="/filiallar" className="hover:text-[color:var(--fg)]">Filiallar</Link></li>
          </ul>
        </div>
        <div>
          <h4 className="text-sm font-semibold text-[color:var(--fg)]">Tizim</h4>
          <ul className="mt-3 space-y-2 text-sm text-[color:var(--muted)]">
            <li><Link href="/bron" className="hover:text-[color:var(--fg)]">Bron qilish</Link></li>
            <li><Link href="/mening-bronlarim" className="hover:text-[color:var(--fg)]">Mening bronlarim</Link></li>
            <li><Link href="/admin" className="hover:text-[color:var(--fg)]">Admin</Link></li>
          </ul>
        </div>
        <div>
          <h4 className="text-sm font-semibold text-[color:var(--fg)]">Bog&apos;lanish</h4>
          <ul className="mt-3 space-y-2 text-sm text-[color:var(--muted)]">
            <li>+998 (90) 000-00-00</li>
            <li>info@beshqozon.uz</li>
            <li>Toshkent, O&apos;zbekiston</li>
          </ul>
        </div>
      </div>
      <div className="border-t border-[color:var(--border)]">
        <div className="bq-container flex flex-col items-center justify-between gap-2 py-4 text-xs text-[color:var(--muted)] md:flex-row">
          <span>© {new Date().getFullYear()} Beshqozon. Barcha huquqlar himoyalangan.</span>
          <span>Made in Uzbekistan</span>
        </div>
      </div>
    </footer>
  );
}
