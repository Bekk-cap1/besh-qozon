"use client";

import { useCallback, useEffect, useState } from "react";
import { AdminShell } from "@/components/admin/AdminShell";
import { apiFetch } from "@/lib/api";
import { useAdminToken } from "@/lib/adminAuth";
import { StarRating } from "@/components/StarRating";

type Review = {
  id: string;
  rating: number;
  comment: string | null;
  isPublished: boolean;
  createdAt: string;
  branch?: { name: string } | null;
  user?: { name: string | null; phone: string } | null;
  reservation?: { id: string; startAt: string } | null;
};

type Branch = { id: string; name: string };

export default function AdminReviewsPage() {
  return (
    <AdminShell title="Sharhlar" subtitle="Mijozlar baholashlari va fikrlari">
      <ReviewsBody />
    </AdminShell>
  );
}

function ReviewsBody() {
  const { token } = useAdminToken();
  const [reviews, setReviews] = useState<Review[]>([]);
  const [branches, setBranches] = useState<Branch[]>([]);
  const [branchId, setBranchId] = useState<string>("");
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    apiFetch<Branch[]>("/branches").then(setBranches);
  }, []);

  const load = useCallback(async () => {
    if (!token) return;
    setLoading(true);
    setErr(null);
    try {
      const q = branchId ? `?branchId=${branchId}` : "";
      const data = await apiFetch<Review[]>(`/admin/reviews${q}`, { token });
      setReviews(data);
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setLoading(false);
    }
  }, [token, branchId]);

  useEffect(() => {
    void load();
  }, [load]);

  const publish = async (id: string, publish: boolean) => {
    if (!token) return;
    try {
      await apiFetch(`/admin/reviews/${id}/${publish ? "publish" : "unpublish"}`, {
        method: "PATCH",
        token,
      });
      await load();
    } catch (e) {
      setErr((e as Error).message);
    }
  };

  const remove = async (id: string) => {
    if (!token) return;
    if (!confirm("Sharhni o'chirasizmi?")) return;
    try {
      await apiFetch(`/admin/reviews/${id}`, { method: "DELETE", token });
      await load();
    } catch (e) {
      setErr((e as Error).message);
    }
  };

  const avg =
    reviews.length > 0
      ? (reviews.reduce((s, r) => s + r.rating, 0) / reviews.length).toFixed(2)
      : "—";

  return (
    <div className="space-y-4">
      <div className="grid gap-3 sm:grid-cols-3">
        <StatCard label="Sharhlar" value={String(reviews.length)} />
        <StatCard label="O'rtacha baho" value={avg} suffix="/5" />
        <StatCard
          label="Yashirin"
          value={String(reviews.filter((r) => !r.isPublished).length)}
        />
      </div>

      <div className="rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)]">
        <label className="block text-xs font-bold uppercase tracking-wider text-[color:var(--muted)]">
          Filial
        </label>
        <select
          value={branchId}
          onChange={(e) => setBranchId(e.target.value)}
          className="mt-2 w-full rounded-xl border border-[color:var(--border)] bg-white px-3 py-2 text-sm sm:max-w-xs"
        >
          <option value="">Barchasi</option>
          {branches.map((b) => (
            <option key={b.id} value={b.id}>
              {b.name}
            </option>
          ))}
        </select>
      </div>

      {err ? (
        <div className="rounded-xl border border-rose-200 bg-rose-50 p-3 text-sm text-rose-700">
          {err}
        </div>
      ) : null}

      <div className="space-y-3">
        {loading && reviews.length === 0 ? (
          <div className="rounded-2xl border border-[color:var(--border)] bg-white p-8 text-center text-sm text-[color:var(--muted)]">
            Yuklanmoqda…
          </div>
        ) : reviews.length === 0 ? (
          <div className="rounded-2xl border border-dashed border-[color:var(--border)] bg-white p-8 text-center text-sm text-[color:var(--muted)]">
            Sharhlar yo&apos;q
          </div>
        ) : (
          reviews.map((r) => (
            <div
              key={r.id}
              className={`rounded-2xl border bg-white p-4 shadow-[var(--shadow-sm)] ${
                r.isPublished ? "border-[color:var(--border)]" : "border-amber-200 bg-amber-50/30"
              }`}
            >
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0 flex-1">
                  <div className="flex flex-wrap items-center gap-2">
                    <StarRating value={r.rating} readonly size={16} />
                    <span className="font-bold text-[color:var(--fg)]">{r.rating}/5</span>
                    {r.branch ? (
                      <span className="rounded-full bg-[color:var(--bg-soft)] px-2 py-0.5 text-xs font-semibold text-[color:var(--muted)]">
                        {r.branch.name}
                      </span>
                    ) : null}
                    {!r.isPublished ? (
                      <span className="rounded-full bg-amber-200 px-2 py-0.5 text-[10px] font-bold text-amber-900">
                        YASHIRIN
                      </span>
                    ) : null}
                  </div>
                  <div className="mt-1 text-xs text-[color:var(--muted)]">
                    {r.user?.name || r.user?.phone} ·{" "}
                    {new Date(r.createdAt).toLocaleString("uz-UZ")}
                  </div>
                  {r.comment ? (
                    <p className="mt-2 text-sm text-[color:var(--fg)]">{r.comment}</p>
                  ) : (
                    <p className="mt-2 text-xs italic text-[color:var(--muted)]">
                      (matnsiz sharh)
                    </p>
                  )}
                </div>
                <div className="flex shrink-0 flex-col gap-1">
                  <button
                    type="button"
                    onClick={() => publish(r.id, !r.isPublished)}
                    className="inline-flex h-8 items-center justify-center rounded-lg border border-[color:var(--border)] px-2 text-xs font-semibold text-[color:var(--muted)] hover:border-[color:var(--brand)] hover:text-[color:var(--brand-700)]"
                  >
                    {r.isPublished ? "Yashirish" : "Ko'rsatish"}
                  </button>
                  <button
                    type="button"
                    onClick={() => remove(r.id)}
                    className="inline-flex h-8 items-center justify-center rounded-lg border border-rose-200 px-2 text-xs font-semibold text-rose-700 hover:bg-rose-50"
                  >
                    O&apos;chirish
                  </button>
                </div>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}

function StatCard({ label, value, suffix }: { label: string; value: string; suffix?: string }) {
  return (
    <div className="rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)]">
      <div className="text-[10px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
        {label}
      </div>
      <div className="mt-1 text-2xl font-extrabold text-[color:var(--fg)]">
        {value}
        {suffix ? (
          <span className="text-sm font-normal text-[color:var(--muted)]">{suffix}</span>
        ) : null}
      </div>
    </div>
  );
}
