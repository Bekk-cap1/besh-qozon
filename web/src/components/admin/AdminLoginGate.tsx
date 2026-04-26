"use client";

import { useState } from "react";
import { fetchAdminToken } from "@/context/AuthContext";
import { BeshqozonLogo } from "@/components/BeshqozonLogo";

export function AdminLoginGate({ onToken }: { onToken: (t: string) => void }) {
  const [login, setLogin] = useState("");
  const [password, setPassword] = useState("");
  const [showPass, setShowPass] = useState(false);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const submit = async () => {
    if (!login.trim() || !password) return;
    setLoading(true);
    setErr(null);
    try {
      const r = await fetchAdminToken(login.trim(), password);
      onToken(r.accessToken);
    } catch (e) {
      setErr((e as Error).message ?? "Login yoki parol noto'g'ri");
    } finally {
      setLoading(false);
    }
  };

  const onKey = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") void submit();
  };

  return (
    <div className="flex min-h-screen items-center justify-center bg-[color:var(--bg-soft)] p-4">
      <div className="w-full max-w-sm rounded-2xl border border-[color:var(--border)] bg-white p-8 shadow-[var(--shadow-md)]">
        {/* Logo */}
        <div className="mb-6 flex justify-center">
          <BeshqozonLogo />
        </div>

        <h1 className="text-center text-xl font-extrabold tracking-tight text-[color:var(--fg)]">
          Admin panel
        </h1>
        <p className="mt-1 text-center text-sm text-[color:var(--muted)]">
          Login va parolingizni kiriting
        </p>

        <div className="mt-7 space-y-4">
          {/* Login */}
          <div>
            <label className="mb-1.5 block text-xs font-bold uppercase tracking-wider text-[color:var(--muted)]">
              Login
            </label>
            <div className="relative">
              <span className="pointer-events-none absolute inset-y-0 left-3 flex items-center text-[color:var(--muted)]">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" />
                  <circle cx="12" cy="7" r="4" />
                </svg>
              </span>
              <input
                className="bq-input h-12 pl-9"
                value={login}
                onChange={(e) => setLogin(e.target.value)}
                onKeyDown={onKey}
                placeholder="admin"
                autoComplete="username"
                autoFocus
              />
            </div>
          </div>

          {/* Password */}
          <div>
            <label className="mb-1.5 block text-xs font-bold uppercase tracking-wider text-[color:var(--muted)]">
              Parol
            </label>
            <div className="relative">
              <span className="pointer-events-none absolute inset-y-0 left-3 flex items-center text-[color:var(--muted)]">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <rect x="3" y="11" width="18" height="11" rx="2" />
                  <path d="M7 11V7a5 5 0 0 1 10 0v4" />
                </svg>
              </span>
              <input
                className="bq-input h-12 pl-9 pr-11"
                type={showPass ? "text" : "password"}
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                onKeyDown={onKey}
                placeholder="••••••••"
                autoComplete="current-password"
              />
              <button
                type="button"
                tabIndex={-1}
                onClick={() => setShowPass((v) => !v)}
                className="absolute inset-y-0 right-3 flex items-center text-[color:var(--muted)] hover:text-[color:var(--fg)]"
                aria-label={showPass ? "Yashirish" : "Ko'rsatish"}
              >
                {showPass ? (
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94" />
                    <path d="M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19" />
                    <line x1="1" y1="1" x2="23" y2="23" />
                  </svg>
                ) : (
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" />
                    <circle cx="12" cy="12" r="3" />
                  </svg>
                )}
              </button>
            </div>
          </div>

          {/* Submit */}
          <button
            type="button"
            onClick={() => void submit()}
            disabled={loading || !login.trim() || !password}
            className="bq-btn bq-btn-primary h-12 w-full text-base"
          >
            {loading ? (
              <span className="inline-flex items-center gap-2">
                <svg className="animate-spin" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <path d="M21 12a9 9 0 1 1-6.219-8.56" />
                </svg>
                Kirish…
              </span>
            ) : (
              "Kirish →"
            )}
          </button>

          {/* Error */}
          {err ? (
            <div className="flex items-start gap-2 rounded-xl border border-red-200 bg-red-50 px-3 py-2.5 text-sm text-red-800">
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="mt-0.5 shrink-0">
                <circle cx="12" cy="12" r="10" />
                <line x1="12" y1="8" x2="12" y2="12" />
                <line x1="12" y1="16" x2="12.01" y2="16" />
              </svg>
              {err}
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
