"use client";

import { useState } from "react";
import { buildMapLinks } from "@/lib/maps";

type Props = {
  name?: string | null;
  address?: string | null;
  lat?: number | null;
  lng?: number | null;
  className?: string;
  /** "button" — с текстом кнопки, "pill" — компактная ссылка, "icon" — круглая иконка */
  variant?: "button" | "pill" | "icon";
};

export function MapLinkButton({
  name,
  address,
  lat,
  lng,
  className,
  variant = "button",
}: Props) {
  const [open, setOpen] = useState(false);
  const links = buildMapLinks({ name, address, lat, lng });
  if (!links) return null;

  const onClick = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setOpen((v) => !v);
  };

  const shared = "inline-flex items-center gap-1.5 font-semibold transition";

  const trigger = (() => {
    if (variant === "pill") {
      return (
        <button
          type="button"
          onClick={onClick}
          className={`${shared} rounded-full border border-[color:var(--border)] bg-white px-3 py-1 text-xs text-[color:var(--fg)] hover:border-[color:var(--brand)] hover:text-[color:var(--brand-700)] ${className ?? ""}`}
        >
          <PinIcon />
          {links.label}
        </button>
      );
    }
    if (variant === "icon") {
      return (
        <button
          type="button"
          onClick={onClick}
          aria-label={links.label}
          title={links.label}
          className={`${shared} h-9 w-9 justify-center rounded-full border border-[color:var(--border)] bg-white text-[color:var(--fg)] hover:border-[color:var(--brand)] hover:text-[color:var(--brand-700)] ${className ?? ""}`}
        >
          <PinIcon />
        </button>
      );
    }
    return (
      <button
        type="button"
        onClick={onClick}
        className={`${shared} h-10 w-full rounded-xl border border-[color:var(--border)] bg-white px-3 text-sm text-[color:var(--fg)] hover:border-[color:var(--brand)] hover:bg-[color:var(--brand-50)] hover:text-[color:var(--brand-700)] ${className ?? ""}`}
      >
        <PinIcon />
        {links.label}
      </button>
    );
  })();

  return (
    <div className="relative inline-block">
      {trigger}
      {open ? (
        <>
          <div
            aria-hidden
            className="fixed inset-0 z-[55]"
            onClick={() => setOpen(false)}
          />
          <div
            role="menu"
            className="absolute right-0 z-[56] mt-2 w-56 overflow-hidden rounded-xl border border-[color:var(--border)] bg-white shadow-lg"
          >
            <a
              href={links.yandex}
              target="_blank"
              rel="noreferrer"
              className="flex items-center gap-2 px-3 py-2.5 text-sm hover:bg-[color:var(--brand-50)]"
              onClick={() => setOpen(false)}
            >
              <span className="flex h-7 w-7 items-center justify-center rounded-full bg-[#fc3f1d] text-[10px] font-bold text-white">
                Я
              </span>
              Yandex Maps
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="ml-auto opacity-50">
                <path d="M7 17L17 7M17 7H7M17 7v10" />
              </svg>
            </a>
            <a
              href={links.google}
              target="_blank"
              rel="noreferrer"
              className="flex items-center gap-2 border-t border-[color:var(--border)] px-3 py-2.5 text-sm hover:bg-[color:var(--brand-50)]"
              onClick={() => setOpen(false)}
            >
              <span className="flex h-7 w-7 items-center justify-center rounded-full bg-[#4285F4] text-[10px] font-bold text-white">
                G
              </span>
              Google Maps
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="ml-auto opacity-50">
                <path d="M7 17L17 7M17 7H7M17 7v10" />
              </svg>
            </a>
          </div>
        </>
      ) : null}
    </div>
  );
}

function PinIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
      <path d="M12 21s-7-6.5-7-12a7 7 0 1 1 14 0c0 5.5-7 12-7 12z" />
      <circle cx="12" cy="9" r="2.5" />
    </svg>
  );
}
