"use client";

import { useEffect, useState } from "react";

export const ADMIN_TOKEN_KEY = "beshqozon_admin_token";

/** React-хук для работы с админским JWT в localStorage. */
export function useAdminToken() {
  const [token, setTokenState] = useState<string | null>(null);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    try {
      const v = localStorage.getItem(ADMIN_TOKEN_KEY);
      setTokenState(v);
    } catch {
      /* ignore */
    }
    setReady(true);
  }, []);

  useEffect(() => {
    const onStorage = (e: StorageEvent) => {
      if (e.key === ADMIN_TOKEN_KEY) setTokenState(e.newValue);
    };
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, []);

  const setToken = (t: string | null) => {
    if (t) localStorage.setItem(ADMIN_TOKEN_KEY, t);
    else localStorage.removeItem(ADMIN_TOKEN_KEY);
    setTokenState(t);
  };

  return { token, ready, setToken };
}
