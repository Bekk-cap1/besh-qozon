"use client";

import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import { apiFetch } from "@/lib/api";

type User = { id: string; phone: string; name?: string | null; role: string };

type AuthState = {
  token: string | null;
  user: User | null;
  setSession: (token: string, user: User) => void;
  logout: () => void;
  requestOtp: (phone: string) => Promise<{ devCode?: string }>;
  verifyOtp: (phone: string, code: string, name?: string) => Promise<void>;
  telegramInit: () => Promise<TelegramInitResult>;
  telegramStatus: (loginToken: string) => Promise<TelegramStatusResult>;
};

export type TelegramInitResult =
  | { enabled: false; message: string }
  | { enabled: true; loginToken: string; deepLink: string; botUsername: string; ttl: number };

export type TelegramStatusResult =
  | { status: "waiting" }
  | { status: "started" }
  | { status: "expired" }
  | { status: "linked"; accessToken: string; user: User };

export type LoyaltyInfo = {
  threshold: number;
  completed: number;
  inCycle: number;
  toNext: number;
  bonuses: number;
  bonusValueUzs: number;
};

const Ctx = createContext<AuthState | null>(null);

const STORAGE_KEY = "beshqozon_token";
const USER_KEY = "beshqozon_user";

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [token, setToken] = useState<string | null>(null);
  const [user, setUser] = useState<User | null>(null);

  useEffect(() => {
    setToken(localStorage.getItem(STORAGE_KEY));
    const u = localStorage.getItem(USER_KEY);
    if (u) {
      try {
        setUser(JSON.parse(u) as User);
      } catch {
        /* ignore */
      }
    }
  }, []);

  const setSession = useCallback((t: string, u: User) => {
    localStorage.setItem(STORAGE_KEY, t);
    localStorage.setItem(USER_KEY, JSON.stringify(u));
    setToken(t);
    setUser(u);
  }, []);

  const logout = useCallback(() => {
    localStorage.removeItem(STORAGE_KEY);
    localStorage.removeItem(USER_KEY);
    setToken(null);
    setUser(null);
  }, []);

  const requestOtp = useCallback(async (phone: string) => {
    return apiFetch<{ devCode?: string }>("/auth/request-otp", {
      method: "POST",
      body: JSON.stringify({ phone }),
    });
  }, []);

  const verifyOtp = useCallback(
    async (phone: string, code: string, name?: string) => {
      const res = await apiFetch<{ accessToken: string; user: User }>("/auth/verify-otp", {
        method: "POST",
        body: JSON.stringify({ phone, code, name }),
      });
      setSession(res.accessToken, res.user);
    },
    [setSession],
  );

  const telegramInit = useCallback(async () => {
    return apiFetch<TelegramInitResult>("/auth/telegram/init", { method: "POST" });
  }, []);

  const telegramStatus = useCallback(async (loginToken: string) => {
    const r = await apiFetch<TelegramStatusResult>(
      `/auth/telegram/status?loginToken=${encodeURIComponent(loginToken)}`,
    );
    if (r.status === "linked") {
      setSession(r.accessToken, r.user);
    }
    return r;
  }, [setSession]);

  const value = useMemo(
    () => ({
      token,
      user,
      setSession,
      logout,
      requestOtp,
      verifyOtp,
      telegramInit,
      telegramStatus,
    }),
    [token, user, setSession, logout, requestOtp, verifyOtp, telegramInit, telegramStatus],
  );

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useAuth() {
  const v = useContext(Ctx);
  if (!v) throw new Error("useAuth outside AuthProvider");
  return v;
}

export async function fetchDevAdminToken(phone: string): Promise<{ accessToken: string; user: User }> {
  return apiFetch("/auth/dev-admin-token", {
    method: "POST",
    body: JSON.stringify({ phone }),
  });
}

export async function fetchAdminToken(login: string, password: string): Promise<{ accessToken: string; user: User }> {
  return apiFetch("/auth/admin-login", {
    method: "POST",
    body: JSON.stringify({ login, password }),
  });
}
