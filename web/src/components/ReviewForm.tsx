"use client";

import { useState } from "react";
import { apiFetch } from "@/lib/api";
import { StarRating } from "./StarRating";

type Props = {
  reservationId: string;
  token: string;
  onDone?: () => void;
};

export function ReviewForm({ reservationId, token, onDone }: Props) {
  const [rating, setRating] = useState(5);
  const [comment, setComment] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [done, setDone] = useState(false);

  const submit = async () => {
    setBusy(true);
    setErr(null);
    try {
      await apiFetch("/reviews", {
        method: "POST",
        token,
        body: JSON.stringify({
          reservationId,
          rating,
          comment: comment.trim() || undefined,
        }),
      });
      setDone(true);
      onDone?.();
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  if (done) {
    return (
      <div className="rounded-2xl border border-emerald-200 bg-emerald-50 p-4 text-sm text-emerald-800">
        ✓ Rahmat! Sharhingiz qabul qilindi.
      </div>
    );
  }

  return (
    <div className="rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)]">
      <div className="text-xs font-bold uppercase tracking-wider text-[color:var(--muted)]">
        Tashrifingizni baholang
      </div>
      <div className="mt-2 flex items-center justify-between">
        <StarRating value={rating} onChange={setRating} />
        <span className="text-sm font-bold text-[color:var(--fg)]">{rating}/5</span>
      </div>
      <textarea
        value={comment}
        onChange={(e) => setComment(e.target.value)}
        placeholder="Fikringizni yozing (ixtiyoriy)…"
        rows={3}
        className="mt-3 w-full rounded-xl border border-[color:var(--border)] bg-white px-3 py-2 text-sm"
        maxLength={500}
      />
      {err ? <p className="mt-2 text-xs text-rose-600">{err}</p> : null}
      <button
        type="button"
        onClick={submit}
        disabled={busy}
        className="bq-btn bq-btn-primary mt-3 h-10 w-full text-sm disabled:opacity-50"
      >
        {busy ? "Yuborilmoqda…" : "Sharhni yuborish"}
      </button>
    </div>
  );
}
