import React from "react";

/**
 * Заглушка логотипа Beshqozon.
 * Когда появится настоящий логотип — замените SVG или добавьте <img src="/logo.svg" />.
 */
export function BeshqozonLogo({ size = 36 }: { size?: number }) {
  return (
    <span className="inline-flex items-center gap-2.5">
      <svg
        width={size}
        height={size}
        viewBox="0 0 48 48"
        xmlns="http://www.w3.org/2000/svg"
        aria-hidden="true"
      >
        <defs>
          <linearGradient id="bqGrad" x1="0" x2="1" y1="0" y2="1">
            <stop offset="0%" stopColor="#22a06b" />
            <stop offset="100%" stopColor="#114f33" />
          </linearGradient>
        </defs>
        <circle cx="24" cy="24" r="22" fill="url(#bqGrad)" />
        {/* stylized qozon (cauldron) */}
        <path
          d="M13 22c0-1.1.9-2 2-2h18c1.1 0 2 .9 2 2v4c0 5-4 9-9 9h-4c-5 0-9-4-9-9v-4z"
          fill="#ffffff"
          opacity="0.95"
        />
        <rect x="10" y="19" width="28" height="3" rx="1.5" fill="#ffffff" opacity="0.85" />
        {/* steam */}
        <path
          d="M19 14c1-2 0-3 0-5M24 12c1-2 0-3 0-5M29 14c1-2 0-3 0-5"
          stroke="#ffffff"
          strokeWidth="1.6"
          strokeLinecap="round"
          fill="none"
          opacity="0.9"
        />
      </svg>
      <span className="flex flex-col leading-none">
        <span className="text-[17px] font-extrabold tracking-tight text-[color:var(--fg)]">
          Beshqozon
        </span>
        <span className="text-[10px] font-medium uppercase tracking-[0.18em] text-[color:var(--muted)]">
          Restoran · bron
        </span>
      </span>
    </span>
  );
}
