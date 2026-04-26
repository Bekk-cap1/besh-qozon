"use client";

import { useState } from "react";

type Props = {
  value: number;
  onChange?: (v: number) => void;
  readonly?: boolean;
  size?: number;
};

export function StarRating({ value, onChange, readonly = false, size = 28 }: Props) {
  const [hover, setHover] = useState<number | null>(null);
  const displayed = hover ?? value;
  return (
    <div className="inline-flex items-center gap-1" role="radiogroup" aria-label="Rating">
      {[1, 2, 3, 4, 5].map((i) => {
        const active = i <= displayed;
        return (
          <button
            key={i}
            type="button"
            disabled={readonly}
            onClick={() => onChange?.(i)}
            onMouseEnter={() => !readonly && setHover(i)}
            onMouseLeave={() => !readonly && setHover(null)}
            className={`transition ${readonly ? "cursor-default" : "cursor-pointer hover:scale-110"}`}
            aria-label={`${i} yulduz`}
            role="radio"
            aria-checked={i === value}
          >
            <svg
              width={size}
              height={size}
              viewBox="0 0 24 24"
              fill={active ? "#f59e0b" : "none"}
              stroke={active ? "#f59e0b" : "#d1d5db"}
              strokeWidth={1.5}
            >
              <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z" />
            </svg>
          </button>
        );
      })}
    </div>
  );
}
