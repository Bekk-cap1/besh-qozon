"use client";

import { useEffect, useMemo, useState } from "react";
import { apiFetch } from "@/lib/api";

export type MenuItem = {
  id: string;
  branchId: string;
  name: string;
  description?: string | null;
  category: string;
  price: number;
  imageUrl?: string | null;
};

export type Cart = Record<string, number>;

type Props = {
  branchId: string | null;
  cart: Cart;
  onChange: (cart: Cart) => void;
};

export function cartToItems(cart: Cart): { menuItemId: string; quantity: number }[] {
  return Object.entries(cart)
    .filter(([, q]) => q > 0)
    .map(([menuItemId, quantity]) => ({ menuItemId, quantity }));
}

export function cartTotal(cart: Cart, menu: MenuItem[]): number {
  let sum = 0;
  for (const m of menu) {
    const q = cart[m.id] ?? 0;
    if (q > 0) sum += m.price * q;
  }
  return sum;
}

export function PreorderMenu({ branchId, cart, onChange }: Props) {
  const [menu, setMenu] = useState<MenuItem[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false);
  const [activeCategory, setActiveCategory] = useState<string | null>(null);

  useEffect(() => {
    if (!branchId) {
      setMenu(null);
      return;
    }
    let cancelled = false;
    setLoading(true);
    apiFetch<MenuItem[]>(`/branches/${branchId}/menu`)
      .then((data) => {
        if (cancelled) return;
        setMenu(data);
        setActiveCategory(data[0]?.category ?? null);
      })
      .catch(() => {
        if (!cancelled) setMenu([]);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [branchId]);

  const grouped = useMemo(() => {
    const map = new Map<string, MenuItem[]>();
    for (const m of menu ?? []) {
      const arr = map.get(m.category) ?? [];
      arr.push(m);
      map.set(m.category, arr);
    }
    return Array.from(map.entries());
  }, [menu]);

  const total = cartTotal(cart, menu ?? []);
  const totalCount = Object.values(cart).reduce((a, b) => a + b, 0);

  if (loading) {
    return (
      <div className="rounded-2xl border border-[color:var(--border)] bg-white p-4">
        <div className="h-4 w-1/3 animate-pulse rounded bg-[color:var(--bg-soft)]" />
      </div>
    );
  }
  if (!menu || menu.length === 0) return null;

  return (
    <div className="overflow-hidden rounded-2xl border border-[color:var(--border)] bg-white">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left transition hover:bg-[color:var(--brand-50)]"
      >
        <div className="flex items-center gap-3">
          <span className="flex h-10 w-10 items-center justify-center rounded-xl bg-[color:var(--brand-50)] text-xl">
            🍽
          </span>
          <div>
            <div className="text-sm font-extrabold text-[color:var(--fg)]">
              Oldindan ovqat buyurtma qilish
            </div>
            <div className="text-xs text-[color:var(--muted)]">
              {totalCount > 0
                ? `${totalCount} ta · ${total.toLocaleString("ru-RU")} so'm`
                : "Ixtiyoriy — restoranda to'laysiz"}
            </div>
          </div>
        </div>
        <svg
          width="20"
          height="20"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          className={`text-[color:var(--muted)] transition-transform ${open ? "rotate-180" : ""}`}
        >
          <polyline points="6 9 12 15 18 9" />
        </svg>
      </button>

      {open ? (
        <div className="border-t border-[color:var(--border)]">
          {/* категории */}
          {grouped.length > 1 ? (
            <div className="flex flex-wrap gap-1.5 overflow-x-auto px-4 pt-3">
              {grouped.map(([cat]) => (
                <button
                  type="button"
                  key={cat}
                  onClick={() => setActiveCategory(cat)}
                  className={[
                    "whitespace-nowrap rounded-full px-3 py-1 text-xs font-semibold transition",
                    activeCategory === cat
                      ? "bg-[color:var(--brand)] text-white"
                      : "bg-[color:var(--bg-soft)] text-[color:var(--fg)] hover:bg-[color:var(--brand-50)]",
                  ].join(" ")}
                >
                  {cat}
                </button>
              ))}
            </div>
          ) : null}

          <div className="max-h-[360px] space-y-2 overflow-y-auto p-3">
            {(grouped.find(([c]) => c === activeCategory)?.[1] ?? menu).map((m) => {
              const q = cart[m.id] ?? 0;
              return (
                <div
                  key={m.id}
                  className="flex items-center gap-3 rounded-xl border border-[color:var(--border)] bg-white p-3"
                >
                  <div className="flex-1 min-w-0">
                    <div className="truncate text-sm font-bold text-[color:var(--fg)]">
                      {m.name}
                    </div>
                    {m.description ? (
                      <div className="line-clamp-1 text-xs text-[color:var(--muted)]">
                        {m.description}
                      </div>
                    ) : null}
                    <div className="mt-0.5 text-sm font-extrabold text-[color:var(--brand-700)]">
                      {m.price.toLocaleString("ru-RU")} so&apos;m
                    </div>
                  </div>
                  {q > 0 ? (
                    <div className="flex items-center gap-1">
                      <Btn onClick={() => onChange({ ...cart, [m.id]: Math.max(0, q - 1) })}>−</Btn>
                      <span className="min-w-[24px] text-center text-sm font-bold">{q}</span>
                      <Btn onClick={() => onChange({ ...cart, [m.id]: Math.min(50, q + 1) })}>+</Btn>
                    </div>
                  ) : (
                    <button
                      type="button"
                      onClick={() => onChange({ ...cart, [m.id]: 1 })}
                      className="rounded-full border border-[color:var(--brand)] bg-white px-3 py-1 text-xs font-bold text-[color:var(--brand-700)] hover:bg-[color:var(--brand-50)]"
                    >
                      + Qo&apos;shish
                    </button>
                  )}
                </div>
              );
            })}
          </div>

          {totalCount > 0 ? (
            <div className="flex items-center justify-between gap-3 border-t border-[color:var(--border)] bg-[color:var(--brand-50)] px-4 py-3">
              <div className="text-xs text-[color:var(--muted)]">
                Restoranda to&apos;lanadi
              </div>
              <div className="text-sm font-extrabold text-[color:var(--brand-700)]">
                Jami: {total.toLocaleString("ru-RU")} so&apos;m
              </div>
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

function Btn({ onClick, children }: { onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="flex h-7 w-7 items-center justify-center rounded-full border border-[color:var(--border)] bg-white text-sm font-extrabold text-[color:var(--fg)] hover:border-[color:var(--brand)] hover:text-[color:var(--brand-700)]"
    >
      {children}
    </button>
  );
}
