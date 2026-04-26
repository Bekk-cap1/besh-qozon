"use client";

import { useEffect, useMemo, useState } from "react";
import { AdminSidebar } from "@/components/admin/AdminSidebar";
import { AdminLoginGate } from "@/components/admin/AdminLoginGate";
import { useAdminToken } from "@/lib/adminAuth";

function decodeJwtPayload(token: string): { phone?: string; sub?: string } | null {
  try {
    const [, payload] = token.split(".");
    if (!payload) return null;
    const json = JSON.parse(
      typeof window === "undefined"
        ? Buffer.from(payload, "base64").toString("utf-8")
        : atob(payload.replace(/-/g, "+").replace(/_/g, "/")),
    );
    return json;
  } catch {
    return null;
  }
}

export default function AdminLayout({ children }: { children: React.ReactNode }) {
  const { token, ready, setToken } = useAdminToken();
  const [open, setOpen] = useState(false);

  const phone = useMemo(() => {
    if (!token) return null;
    return decodeJwtPayload(token)?.phone ?? null;
  }, [token]);

  // Мобильный drawer: открывается из AdminShell через кастомное событие
  useEffect(() => {
    const openHandler = () => setOpen(true);
    window.addEventListener("admin-open-sidebar", openHandler);
    return () => window.removeEventListener("admin-open-sidebar", openHandler);
  }, []);

  if (!ready) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-[color:var(--bg-soft)]">
        <div className="text-sm text-[color:var(--muted)]">Yuklanmoqda…</div>
      </div>
    );
  }

  if (!token) {
    return <AdminLoginGate onToken={setToken} />;
  }

  return (
    <div className="min-h-screen bg-[color:var(--bg-soft)] text-[color:var(--fg)]">
      <div className="flex min-h-screen">
        <AdminSidebar
          open={open}
          onClose={() => setOpen(false)}
          onLogout={() => setToken(null)}
          phone={phone}
        />

        <div className="flex min-w-0 flex-1 flex-col">{children}</div>
      </div>
    </div>
  );
}
