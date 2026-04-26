"use client";

import { useEffect, useRef } from "react";
import { io, Socket } from "socket.io-client";
import { wsOrigin } from "./api";

type Handlers = {
  onTablesRefresh?: (payload: { zoneId?: string }) => void;
  onAdminRefresh?: (payload: { scope?: string; zoneId?: string; reservationId?: string }) => void;
};

type Options = Handlers & {
  /** Zone ID to subscribe to (joinZone/leaveZone on change). */
  zoneId?: string | null;
  /** Register as admin (room "admin") to receive admin:refresh. */
  admin?: boolean;
  /** Turn off the whole subscription (e.g. until token is known). */
  enabled?: boolean;
};

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function useBeshSocket({ zoneId, admin, enabled = true, onTablesRefresh, onAdminRefresh }: Options) {
  const socketRef = useRef<Socket | null>(null);
  const zoneRef = useRef<string | null>(null);
  const handlersRef = useRef<Handlers>({});
  handlersRef.current = { onTablesRefresh, onAdminRefresh };

  useEffect(() => {
    if (!enabled) return;
    const s = io(wsOrigin(), {
      transports: ["websocket", "polling"],
      reconnection: true,
      reconnectionAttempts: Infinity,
      reconnectionDelay: 750,
      reconnectionDelayMax: 5_000,
      timeout: 10_000,
    });
    socketRef.current = s;

    const joinCurrent = () => {
      if (admin) s.emit("joinAdmin");
      const z = zoneRef.current;
      if (z && UUID_RE.test(z)) s.emit("joinZone", z);
    };

    s.on("connect", joinCurrent);
    s.io.on("reconnect", joinCurrent);

    s.on("tables:refresh", (p: { zoneId?: string }) => {
      handlersRef.current.onTablesRefresh?.(p ?? {});
    });
    s.on("admin:refresh", (p: { scope?: string; zoneId?: string; reservationId?: string }) => {
      handlersRef.current.onAdminRefresh?.(p ?? {});
    });

    return () => {
      s.removeAllListeners();
      s.io.removeAllListeners();
      s.close();
      socketRef.current = null;
    };
  }, [admin, enabled]);

  useEffect(() => {
    const s = socketRef.current;
    const prev = zoneRef.current;
    const next = zoneId && UUID_RE.test(zoneId) ? zoneId : null;
    zoneRef.current = next;
    if (!s || !s.connected) return;
    if (prev && prev !== next) s.emit("leaveZone", prev);
    if (next && next !== prev) s.emit("joinZone", next);
  }, [zoneId]);
}
