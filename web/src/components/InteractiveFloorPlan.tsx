"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useBeshSocket } from "@/lib/useBeshSocket";

export type PlanTable = {
  id: string;
  number: string;
  seats: number;
  type: string;
  xPos: number;
  yPos: number;
  slotState: "free" | "held" | "booked" | "maintenance";
  maintenance: boolean;
};

type State = "free" | "held" | "booked" | "maintenance" | "selected";

const STATE_COLORS: Record<State, string> = {
  free: "#1b7a4e",
  held: "#f59e0b",
  booked: "#ef4444",
  maintenance: "#6b7280",
  selected: "#0ea5e9",
};

const STATE_BG: Record<State, string> = {
  free: "#dcfce7",
  held: "#fef3c7",
  booked: "#fee2e2",
  maintenance: "#e5e7eb",
  selected: "#e0f2fe",
};

const STATE_LABELS: Record<Exclude<State, "selected">, string> = {
  free: "Bo'sh",
  held: "Kutishda",
  booked: "Band",
  maintenance: "Texnik",
};

function stateOf(t: PlanTable, selectedId: string | null): State {
  if (t.id === selectedId) return "selected";
  if (t.slotState === "maintenance" || t.maintenance) return "maintenance";
  if (t.slotState === "booked") return "booked";
  if (t.slotState === "held") return "held";
  return "free";
}

export type FloorDecoration =
  | { kind: "door"; x: number; y: number } // relative 0-1 of floor dimensions
  | { kind: "label"; text: string; style: "bar" | "kitchen" | "default"; x: number; y: number };

export type FloorConfig = { doors?: FloorDecoration[]; labels?: FloorDecoration[] };

function parseFloorConfig(raw: string | null | undefined): FloorConfig | null {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    if (typeof parsed === "object" && parsed !== null) return parsed as FloorConfig;
  } catch {
    /* ignore */
  }
  return null;
}

type Props = {
  zoneId: string;
  zoneType?: string;
  tables: PlanTable[];
  selectedId: string | null;
  floorConfig?: string | null;
  onSelect: (id: string) => void;
  onRefresh?: () => void;
};

const TABLE_W = 96;
const TABLE_H = 68;
const PADDING = 40;
const WALL_THICKNESS = 8;

const MIN_ZOOM = 0.4;
const MAX_ZOOM = 2.5;

export function InteractiveFloorPlan({
  zoneId,
  zoneType = "INDOOR",
  tables,
  selectedId,
  floorConfig: floorConfigRaw,
  onSelect,
  onRefresh,
}: Props) {
  const floorConfig = useMemo(() => parseFloorConfig(floorConfigRaw), [floorConfigRaw]);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const viewportRef = useRef<HTMLDivElement | null>(null);
  const [containerWidth, setContainerWidth] = useState(720);
  const [hover, setHover] = useState<{ id: string; x: number; y: number } | null>(null);
  const [userZoom, setUserZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const dragState = useRef<{ startX: number; startY: number; panX: number; panY: number } | null>(null);

  const bounds = useMemo(() => {
    if (!tables.length) {
      return { minX: 0, minY: 0, width: 640, height: 360 };
    }
    let minX = Infinity;
    let minY = Infinity;
    let maxX = 0;
    let maxY = 0;
    for (const t of tables) {
      minX = Math.min(minX, t.xPos);
      minY = Math.min(minY, t.yPos);
      maxX = Math.max(maxX, t.xPos + TABLE_W);
      maxY = Math.max(maxY, t.yPos + TABLE_H);
    }
    // минимальные размеры чтобы фон красиво смотрелся
    const w = Math.max(maxX - minX + PADDING * 2, 600);
    const h = Math.max(maxY - minY + PADDING * 2, 360);
    return {
      minX: minX - PADDING,
      minY: minY - PADDING,
      width: w,
      height: h,
    };
  }, [tables]);

  useEffect(() => {
    if (!containerRef.current) return;
    const el = containerRef.current;
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) setContainerWidth(Math.max(320, entry.contentRect.width));
    });
    observer.observe(el);
    setContainerWidth(Math.max(320, el.clientWidth));
    return () => observer.disconnect();
  }, []);

  const baseScale = useMemo(() => {
    const s = containerWidth / bounds.width;
    return Math.min(Math.max(s, 0.5), 1.4);
  }, [containerWidth, bounds.width]);

  const scale = Math.min(Math.max(baseScale * userZoom, MIN_ZOOM * 0.5), MAX_ZOOM * 1.6);

  const viewportHeight = Math.max(360, Math.round(bounds.height * baseScale));

  const zoomIn = useCallback(() => setUserZoom((z) => Math.min(MAX_ZOOM, +(z + 0.2).toFixed(2))), []);
  const zoomOut = useCallback(() => setUserZoom((z) => Math.max(MIN_ZOOM, +(z - 0.2).toFixed(2))), []);
  const zoomReset = useCallback(() => {
    setUserZoom(1);
    setPan({ x: 0, y: 0 });
  }, []);

  // Wheel zoom (desktop)
  useEffect(() => {
    const el = viewportRef.current;
    if (!el) return;
    const onWheel = (e: WheelEvent) => {
      if (e.ctrlKey || e.metaKey || Math.abs(e.deltaY) > 10) {
        // shift+scroll или обычный scroll с Ctrl — масштабирование
        e.preventDefault();
        setUserZoom((z) => {
          const delta = e.deltaY < 0 ? 0.1 : -0.1;
          return Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, +(z + delta).toFixed(2)));
        });
      }
    };
    el.addEventListener("wheel", onWheel, { passive: false });
    return () => el.removeEventListener("wheel", onWheel);
  }, []);

  // Pan (drag)
  const onPointerDown = (e: React.PointerEvent) => {
    if ((e.target as HTMLElement).dataset.tableId) return;
    (e.target as HTMLElement).setPointerCapture(e.pointerId);
    dragState.current = { startX: e.clientX, startY: e.clientY, panX: pan.x, panY: pan.y };
  };
  const onPointerMove = (e: React.PointerEvent) => {
    if (!dragState.current) return;
    const dx = e.clientX - dragState.current.startX;
    const dy = e.clientY - dragState.current.startY;
    setPan({ x: dragState.current.panX + dx, y: dragState.current.panY + dy });
  };
  const onPointerUp = () => {
    dragState.current = null;
  };

  useBeshSocket({
    zoneId,
    enabled: Boolean(onRefresh),
    onTablesRefresh: (payload) => {
      if (!payload.zoneId || payload.zoneId === zoneId) onRefresh?.();
    },
  });

  const counts = useMemo(() => {
    const acc = { free: 0, held: 0, booked: 0, maintenance: 0 };
    for (const t of tables) {
      const s = stateOf(t, null);
      if (s === "selected") continue;
      acc[s] += 1;
    }
    return acc;
  }, [tables]);

  const contentWidth = Math.round(bounds.width * scale);
  const contentHeight = Math.round(bounds.height * scale);

  return (
    <div className="w-full space-y-3" ref={containerRef}>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <Legend counts={counts} total={tables.length} />
        <div className="flex items-center gap-1">
          <button
            type="button"
            onClick={zoomOut}
            aria-label="Kichraytirish"
            className="h-9 w-9 rounded-lg border border-[color:var(--border)] bg-white text-lg font-bold text-[color:var(--fg)] transition hover:border-[color:var(--brand)]"
          >
            −
          </button>
          <button
            type="button"
            onClick={zoomReset}
            className="h-9 rounded-lg border border-[color:var(--border)] bg-white px-2.5 text-xs font-semibold text-[color:var(--fg)] transition hover:border-[color:var(--brand)]"
          >
            {Math.round(userZoom * 100)}%
          </button>
          <button
            type="button"
            onClick={zoomIn}
            aria-label="Kattalashtirish"
            className="h-9 w-9 rounded-lg border border-[color:var(--border)] bg-white text-lg font-bold text-[color:var(--fg)] transition hover:border-[color:var(--brand)]"
          >
            +
          </button>
        </div>
      </div>

      <div
        ref={viewportRef}
        className="relative w-full overflow-hidden rounded-2xl border border-[color:var(--border)] bg-[#f3f6f4] shadow-inner"
        style={{ height: viewportHeight, touchAction: "none" }}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
        onPointerLeave={onPointerUp}
      >
        {/* Движимый контейнер с планом */}
        <div
          className="absolute left-1/2 top-1/2 origin-center"
          style={{
            width: contentWidth,
            height: contentHeight,
            transform: `translate(-50%, -50%) translate(${pan.x}px, ${pan.y}px)`,
            transition: dragState.current ? "none" : "transform 150ms ease-out",
          }}
        >
          {/* Фон зала */}
          <FloorPlanBackground
            width={contentWidth}
            height={contentHeight}
            zoneType={zoneType}
            config={floorConfig}
          />

          {/* Столы */}
          {tables.map((t) => {
            const s = stateOf(t, selectedId);
            const clickable = t.slotState === "free" && !t.maintenance;
            const left = (t.xPos - bounds.minX) * scale;
            const top = (t.yPos - bounds.minY) * scale;
            const w = TABLE_W * scale;
            const h = TABLE_H * scale;
            return (
              <button
                key={t.id}
                type="button"
                data-table-id={t.id}
                disabled={!clickable}
                onClick={() => clickable && onSelect(t.id)}
                onMouseEnter={(e) => {
                  const r = (e.currentTarget as HTMLButtonElement).getBoundingClientRect();
                  const vp = viewportRef.current?.getBoundingClientRect();
                  if (!vp) return;
                  setHover({ id: t.id, x: r.left - vp.left + r.width / 2, y: r.top - vp.top });
                }}
                onMouseLeave={() => setHover((h) => (h?.id === t.id ? null : h))}
                className={`absolute flex flex-col items-center justify-center overflow-hidden rounded-xl border-2 text-white transition-all ${
                  clickable ? "cursor-pointer hover:-translate-y-0.5 hover:shadow-lg" : "cursor-not-allowed"
                } ${s === "selected" ? "ring-4 ring-[color:var(--brand)]/40" : ""}`}
                style={{
                  left,
                  top,
                  width: w,
                  height: h,
                  background: STATE_COLORS[s],
                  borderColor: s === "selected" ? "#0369a1" : "rgba(0,0,0,0.12)",
                  boxShadow:
                    s === "selected"
                      ? "0 8px 24px rgba(14, 165, 233, 0.35)"
                      : "0 2px 6px rgba(0,0,0,0.15)",
                  opacity: clickable ? 1 : 0.85,
                }}
              >
                <span
                  className="pointer-events-none font-extrabold leading-none"
                  style={{ fontSize: `${Math.max(9, Math.min(15, 13 * scale))}px` }}
                >
                  {t.number}
                </span>
                {scale > 0.55 ? (
                  <span
                    className="pointer-events-none font-bold opacity-95"
                    style={{ fontSize: `${Math.max(8, Math.min(11, 9 * scale))}px`, marginTop: 2 }}
                  >
                    {t.seats}k
                  </span>
                ) : null}
                {s !== "free" && s !== "selected" ? (
                  <span className="pointer-events-none mt-1 rounded-full bg-black/20 px-1.5 py-0.5 text-[9px] font-bold">
                    {STATE_LABELS[s]}
                  </span>
                ) : s === "selected" ? (
                  <span className="pointer-events-none mt-1 rounded-full bg-white/90 px-1.5 py-0.5 text-[9px] font-bold text-[#0369a1]">
                    Tanlandi ✓
                  </span>
                ) : null}
              </button>
            );
          })}

          {tables.length === 0 ? (
            <div
              className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 rounded-xl border border-dashed border-[color:var(--border-strong)] bg-white/80 px-4 py-3 text-sm text-[color:var(--muted)]"
            >
              Bu zonada stollar hali qo&apos;yilmagan.
            </div>
          ) : null}
        </div>

        {/* Hover tooltip */}
        {hover ? (() => {
          const t = tables.find((x) => x.id === hover.id);
          if (!t) return null;
          const s = stateOf(t, selectedId);
          const bg = STATE_BG[s];
          const fg = STATE_COLORS[s];
          return (
            <div
              className="pointer-events-none absolute z-10 -translate-x-1/2 -translate-y-full rounded-xl border border-[color:var(--border)] bg-white px-3 py-2 text-xs shadow-lg"
              style={{ left: hover.x, top: hover.y - 6 }}
            >
              <div className="flex items-center gap-2">
                <span
                  className="inline-block h-2.5 w-2.5 rounded-full"
                  style={{ background: fg }}
                  aria-hidden
                />
                <b className="text-sm text-[color:var(--fg)]">T-{t.number}</b>
                <span
                  className="rounded-full px-1.5 py-0.5 text-[10px] font-bold"
                  style={{ background: bg, color: fg }}
                >
                  {s === "selected" ? "Tanlandi" : STATE_LABELS[s as Exclude<State, "selected">]}
                </span>
              </div>
              <div className="mt-1 text-[color:var(--muted)]">
                {t.seats} kishilik · {t.type === "VIP" ? "VIP" : t.type === "FAMILY" ? "Oilaviy" : "Standart"}
              </div>
            </div>
          );
        })() : null}

        {/* Подсказка */}
        <div className="pointer-events-none absolute bottom-2 left-1/2 -translate-x-1/2 rounded-full bg-white/90 px-3 py-1 text-[10px] text-[color:var(--muted)] shadow-sm">
          Yashil stolni bosing · Sichqoncha g&apos;ildiragi yoki +/− bilan zoom · Sichqoncha bilan suring
        </div>
      </div>
    </div>
  );
}

function DoorIcon({
  width,
  height,
  floorColor,
  door,
}: {
  width: number;
  height: number;
  floorColor: string;
  door: { x: number; y: number };
}) {
  const cx = door.x * width;
  const cy = door.y * height;
  const dw = 36;
  const dh = 52;
  return (
    <g>
      {/* Clear opening */}
      <rect x={cx - dw / 2} y={cy - dh / 2} width={dw} height={dh} fill={floorColor} />
      {/* Door frame */}
      <rect x={cx - dw / 2} y={cy - dh / 2} width={dw} height={dh} fill="none" stroke="#475569" strokeWidth="2" rx={3} />
      {/* Door leaf (half-open) */}
      <rect x={cx - dw / 2 + 2} y={cy - dh / 2 + 3} width={dw / 2} height={dh - 6} fill="#94a3b8" opacity="0.55" rx={2} />
      {/* Door knob */}
      <circle cx={cx + dw / 2 - 8} cy={cy} r={3.5} fill="#64748b" />
      {/* Swing arc */}
      <path
        d={`M ${cx - dw / 2 + 2} ${cy + dh / 2 - 3} A ${dh - 6} ${dh - 6} 0 0 1 ${cx + dw / 2 - 2} ${cy - dh / 2 + 3}`}
        fill="none"
        stroke="#94a3b8"
        strokeWidth="1"
        strokeDasharray="4 3"
      />
      {/* Label */}
      <text x={cx} y={cy + dh / 2 + 13} textAnchor="middle" fontSize="10" fill="#64748b" fontWeight="bold">
        ESHIK
      </text>
    </g>
  );
}

function FloorPlanBackground({
  width,
  height,
  zoneType,
  config,
}: {
  width: number;
  height: number;
  zoneType: string;
  config: FloorConfig | null;
}) {
  const isOutdoor = zoneType === "OUTDOOR";
  const isVip = zoneType === "VIP";
  const wallColor = isOutdoor ? "#86c8a4" : isVip ? "#9333ea" : "#cbd5e1";
  const floorColor = isOutdoor ? "#eaf6ef" : isVip ? "#f3e8ff" : "#f8fafc";

  const doors: Array<{ x: number; y: number }> =
    config?.doors && config.doors.length > 0
      ? (config.doors.filter((d) => d.kind === "door") as Array<{ kind: "door"; x: number; y: number }>)
      : [{ x: 0.5, y: 0.92 }];

  const labels: Array<{ text: string; style: string; x: number; y: number }> =
    config?.labels
      ? (config.labels.filter((d) => d.kind === "label") as Array<{ kind: "label"; text: string; style: string; x: number; y: number }>)
      : isOutdoor
        ? []
        : [
            { text: "BAR", style: "bar", x: 18, y: 18 },
            { text: "OSHXONA", style: "kitchen", x: width - width * 0.22 - 18, y: height - 52 },
          ];

  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      className="absolute inset-0"
      aria-hidden
    >
      <rect x="0" y="0" width={width} height={height} fill={floorColor} rx={16} />
      <defs>
        <pattern id="floor-grid" x="0" y="0" width="40" height="40" patternUnits="userSpaceOnUse">
          <path d="M 40 0 L 0 0 0 40" fill="none" stroke={isVip ? "#e9d5ff" : "#e2e8f0"} strokeWidth="0.5" />
        </pattern>
      </defs>
      <rect x="0" y="0" width={width} height={height} fill="url(#floor-grid)" rx={16} />

      {/* Внешние стены */}
      <rect
        x={WALL_THICKNESS / 2}
        y={WALL_THICKNESS / 2}
        width={width - WALL_THICKNESS}
        height={height - WALL_THICKNESS}
        fill="none"
        stroke={wallColor}
        strokeWidth={WALL_THICKNESS}
        rx={16}
      />

      {/* Окна */}
      {!isOutdoor
        ? Array.from({ length: 3 }).map((_, i) => {
            const x = width * (0.18 + i * 0.24);
            const w = width * 0.12;
            return (
              <g key={`win-${i}`}>
                <rect x={x} y={0} width={w} height={WALL_THICKNESS + 2} fill={floorColor} />
                <rect x={x + 2} y={2} width={w - 4} height={WALL_THICKNESS - 2} fill="#bae6fd" stroke="#0ea5e9" strokeWidth="1" />
                <line x1={x + w / 2} y1={2} x2={x + w / 2} y2={WALL_THICKNESS} stroke="#0ea5e9" strokeWidth="1" />
              </g>
            );
          })
        : null}

      {/* Doors */}
      {doors.map((d, i) => (
        <DoorIcon key={i} width={width} height={height} floorColor={floorColor} door={d} />
      ))}

      {/* Метки (BAR, OSHXONA, custom) */}
      {labels.map((lbl, i) => {
        const lx = config?.labels ? lbl.x * width : lbl.x;
        const ly = config?.labels ? lbl.y * height : lbl.y;
        const lw = config?.labels ? width * 0.18 : width * 0.22;
        const isBar = lbl.style === "bar";
        const isKitchen = lbl.style === "kitchen";
        return (
          <g key={i}>
            <rect
              x={lx}
              y={ly}
              width={lw}
              height={isKitchen ? 34 : 38}
              fill={isBar ? "#422006" : isKitchen ? "#1e293b" : "#374151"}
              stroke={isBar ? "#92400e" : isKitchen ? "#64748b" : "#6b7280"}
              strokeWidth="1.5"
              rx={6}
            />
            <text
              x={lx + lw / 2}
              y={ly + (isKitchen ? 22 : 26)}
              textAnchor="middle"
              fontSize="13"
              fontWeight="bold"
              fill={isBar ? "#fcd34d" : "#fbbf24"}
            >
              {lbl.text}
            </text>
          </g>
        );
      })}

      {/* OUTDOOR — деревья */}
      {isOutdoor ? (
        <g>
          <circle cx={40} cy={40} r={18} fill="#22c55e" opacity="0.7" />
          <circle cx={width - 40} cy={40} r={18} fill="#22c55e" opacity="0.7" />
          <circle cx={40} cy={height - 40} r={16} fill="#22c55e" opacity="0.7" />
          <circle cx={width - 40} cy={height - 40} r={16} fill="#22c55e" opacity="0.7" />
        </g>
      ) : null}
    </svg>
  );
}

function Legend({
  counts,
  total,
}: {
  counts: { free: number; held: number; booked: number; maintenance: number };
  total: number;
}) {
  const items: Array<{ key: Exclude<State, "selected">; count: number }> = [
    { key: "free", count: counts.free },
    { key: "held", count: counts.held },
    { key: "booked", count: counts.booked },
    { key: "maintenance", count: counts.maintenance },
  ];
  return (
    <div className="flex flex-wrap items-center gap-2 text-xs">
      <span className="rounded-full border border-[color:var(--border)] bg-white px-2.5 py-1 font-semibold text-[color:var(--fg)]">
        Jami: <b className="text-[color:var(--brand-700)]">{total}</b>
      </span>
      {items.map((it) => (
        <span
          key={it.key}
          className="inline-flex items-center gap-1.5 rounded-full border border-[color:var(--border)] bg-white px-2.5 py-1 font-medium text-[color:var(--fg)]"
        >
          <span
            className="inline-block h-2.5 w-2.5 rounded-full"
            style={{ backgroundColor: STATE_COLORS[it.key] }}
            aria-hidden
          />
          {STATE_LABELS[it.key]}: <b>{it.count}</b>
        </span>
      ))}
    </div>
  );
}
