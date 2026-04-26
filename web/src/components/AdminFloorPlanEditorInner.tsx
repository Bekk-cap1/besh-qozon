"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { apiFetch } from "@/lib/api";

export type EditorTable = {
  id: string;
  number: string;
  seats: number;
  type: string;
  status: string;
  xPos: number;
  yPos: number;
};

type FloorDoor = { kind: "door"; x: number; y: number }; // relative 0-1
type FloorLabel = { kind: "label"; text: string; style: "bar" | "kitchen" | "default"; x: number; y: number };
type FloorDecorItem = FloorDoor | FloorLabel;

type Props = {
  zoneId: string;
  zoneName: string;
  token: string;
  tables: EditorTable[];
  onSaved: () => void;
  onSelect?: (id: string | null) => void;
  selectedId?: string | null;
  onStatusChange?: (id: string, status: string) => void;
  onDuplicate?: (id: string) => void;
  onDelete?: (id: string) => void;
  floorConfig?: string | null;
};

const TABLE_W = 100;
const TABLE_H = 72;
const DOOR_SIZE = 44;
const PADDING = 40;
const GRID = 20;
const WORLD_W = 1200;
const WORLD_H = 700;
const STAGE_MIN_H = 480;

const BG_KEY = (zoneId: string) => `beshqozon_floor_bg:${zoneId}`;
const BG_OPACITY_KEY = (zoneId: string) => `beshqozon_floor_bg_opacity:${zoneId}`;

const TYPE_COLORS: Record<string, string> = {
  STANDARD: "#2D6A4F",
  VIP: "#7c3aed",
  FAMILY: "#0284c7",
};

const STATUS_BORDER: Record<string, string> = {
  AVAILABLE: "rgba(0,0,0,0.15)",
  OCCUPIED: "#b91c1c",
  RESERVED: "#ea580c",
  MAINTENANCE: "#6b7280",
};

const STATUS_LABEL: Record<string, string> = {
  AVAILABLE: "Bo'sh",
  OCCUPIED: "Band",
  RESERVED: "Zahirada",
  MAINTENANCE: "Ta'mirda",
};

function snap(v: number) {
  return Math.round(v / GRID) * GRID;
}
function toNum(v: unknown, fallback = 0): number {
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : fallback;
}

type Menu = {
  x: number;
  y: number;
  tableId: string;
} | null;

export default function AdminFloorPlanEditorInner({
  zoneId,
  zoneName,
  token,
  tables,
  onSaved,
  onSelect,
  selectedId,
  onStatusChange,
  onDuplicate,
  onDelete,
  floorConfig: floorConfigRaw,
}: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [containerWidth, setContainerWidth] = useState(800);
  const [positions, setPositions] = useState<Record<string, { x: number; y: number }>>({});
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  // Dekoratsiyalar
  const [decorItems, setDecorItems] = useState<FloorDecorItem[]>([]);
  const [decorSaving, setDecorSaving] = useState(false);
  const [decorMsg, setDecorMsg] = useState<string | null>(null);
  const [newLabelText, setNewLabelText] = useState("");
  const [doorMode, setDoorMode] = useState(false);

  const [bgUrl, setBgUrl] = useState<string | null>(null);
  const [bgOpacity, setBgOpacity] = useState<number>(0.7);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const [menu, setMenu] = useState<Menu>(null);

  // table drag state
  const dragRef = useRef<{
    id: string;
    pointerId: number;
    startX: number;
    startY: number;
    origX: number;
    origY: number;
  } | null>(null);
  const [dragging, setDragging] = useState<string | null>(null);

  // door drag state
  const doorDragRef = useRef<{
    idx: number;
    pointerId: number;
    startX: number;
    startY: number;
    origX: number;
    origY: number;
  } | null>(null);

  useEffect(() => {
    const init: Record<string, { x: number; y: number }> = {};
    for (const t of tables) init[t.id] = { x: toNum(t.xPos), y: toNum(t.yPos) };
    setPositions(init);
  }, [tables]);

  useEffect(() => {
    if (floorConfigRaw) {
      try {
        const c = JSON.parse(floorConfigRaw) as { doors?: FloorDoor[]; labels?: FloorLabel[] };
        setDecorItems([...(c.doors ?? []), ...(c.labels ?? [])]);
        return;
      } catch { /* ignore */ }
    }
    apiFetch<{ doors?: FloorDoor[]; labels?: FloorLabel[] }>(`/admin/floor-plan/${zoneId}/config`, { token })
      .then((c) => setDecorItems([...(c.doors ?? []), ...(c.labels ?? [])]))
      .catch(() => setDecorItems([]));
  }, [zoneId, token, floorConfigRaw]);

  const saveDecor = async () => {
    setDecorSaving(true);
    setDecorMsg(null);
    try {
      await apiFetch(`/admin/floor-plan/${zoneId}/config`, {
        method: "PATCH",
        token,
        body: JSON.stringify({
          doors: decorItems.filter((d) => d.kind === "door"),
          labels: decorItems.filter((d) => d.kind === "label"),
        }),
      });
      setDecorMsg("Dekoratsiyalar saqlandi");
    } catch (e) {
      setDecorMsg(`Xatolik: ${(e as Error).message}`);
    } finally {
      setDecorSaving(false);
    }
  };

  useEffect(() => {
    try {
      const saved = localStorage.getItem(BG_KEY(zoneId));
      setBgUrl(saved);
      const op = localStorage.getItem(BG_OPACITY_KEY(zoneId));
      if (op) setBgOpacity(Math.max(0.1, Math.min(1, Number(op))));
      else setBgOpacity(0.7);
    } catch {
      setBgUrl(null);
    }
  }, [zoneId]);

  useEffect(() => {
    if (!containerRef.current) return;
    const el = containerRef.current;
    const update = () => setContainerWidth(Math.max(320, el.clientWidth));
    const obs = new ResizeObserver(update);
    obs.observe(el);
    update();
    return () => obs.disconnect();
  }, []);

  useEffect(() => {
    if (!menu) return;
    const onDown = () => setMenu(null);
    window.addEventListener("pointerdown", onDown);
    window.addEventListener("scroll", onDown, true);
    window.addEventListener("resize", onDown);
    return () => {
      window.removeEventListener("pointerdown", onDown);
      window.removeEventListener("scroll", onDown, true);
      window.removeEventListener("resize", onDown);
    };
  }, [menu]);

  const world = useMemo(() => {
    let w = WORLD_W;
    let h = WORLD_H;
    for (const t of tables) {
      const p = positions[t.id] ?? { x: toNum(t.xPos), y: toNum(t.yPos) };
      w = Math.max(w, p.x + TABLE_W + PADDING);
      h = Math.max(h, p.y + TABLE_H + PADDING);
    }
    return { w, h };
  }, [tables, positions]);

  const scale = useMemo(() => containerWidth / world.w, [containerWidth, world.w]);
  const stageW = containerWidth;
  const stageH = Math.max(STAGE_MIN_H, Math.round(world.h * scale));

  const changed = useMemo(() => {
    return tables.filter((t) => {
      const p = positions[t.id];
      if (!p) return false;
      return p.x !== toNum(t.xPos) || p.y !== toNum(t.yPos);
    });
  }, [tables, positions]);

  const save = async () => {
    if (!changed.length) return;
    setSaving(true);
    setErr(null);
    try {
      await apiFetch(`/admin/floor-plan/${zoneId}`, {
        method: "PUT",
        token,
        body: JSON.stringify({
          tables: changed.map((t) => ({
            id: t.id,
            xPos: positions[t.id].x,
            yPos: positions[t.id].y,
          })),
        }),
      });
      onSaved();
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const reset = () => {
    const init: Record<string, { x: number; y: number }> = {};
    for (const t of tables) init[t.id] = { x: toNum(t.xPos), y: toNum(t.yPos) };
    setPositions(init);
    setErr(null);
  };

  const autoArrange = () => {
    const next: Record<string, { x: number; y: number }> = {};
    const cols = Math.max(1, Math.floor((WORLD_W - PADDING * 2) / (TABLE_W + 20)));
    tables.forEach((t, i) => {
      next[t.id] = {
        x: PADDING + (i % cols) * (TABLE_W + 20),
        y: PADDING + Math.floor(i / cols) * (TABLE_H + 20),
      };
    });
    setPositions(next);
  };

  const moveBy = (id: string, dx: number, dy: number) => {
    setPositions((prev) => {
      const cur = prev[id] ?? { x: 0, y: 0 };
      return {
        ...prev,
        [id]: {
          x: Math.max(0, Math.min(world.w - TABLE_W, cur.x + dx)),
          y: Math.max(0, Math.min(world.h - TABLE_H, cur.y + dy)),
        },
      };
    });
  };

  // ----- Background -----
  const onPickBackground = () => fileInputRef.current?.click();

  const onBackgroundChosen = async (file: File | null) => {
    if (!file) return;
    if (file.size > 5 * 1024 * 1024) {
      setErr("Rasm hajmi 5 MB dan oshmasligi kerak.");
      return;
    }
    if (!/^image\/(png|svg\+xml|jpeg|webp)$/.test(file.type)) {
      setErr("Faqat PNG / SVG / JPG / WEBP qo'llab-quvvatlanadi.");
      return;
    }
    const reader = new FileReader();
    reader.onload = () => {
      const dataUrl = String(reader.result || "");
      try {
        localStorage.setItem(BG_KEY(zoneId), dataUrl);
      } catch {
        setErr("Rasm saqlanmadi (localStorage to'la bo'lishi mumkin).");
      }
      setBgUrl(dataUrl);
    };
    reader.onerror = () => setErr("Rasmni o'qib bo'lmadi.");
    reader.readAsDataURL(file);
  };

  const clearBackground = () => {
    try {
      localStorage.removeItem(BG_KEY(zoneId));
    } catch {
      /* ignore */
    }
    setBgUrl(null);
  };

  const changeBgOpacity = (v: number) => {
    setBgOpacity(v);
    try {
      localStorage.setItem(BG_OPACITY_KEY(zoneId), String(v));
    } catch {
      /* ignore */
    }
  };

  // ----- Table drag handlers -----
  const onPointerDown = useCallback(
    (e: React.PointerEvent<HTMLDivElement>, id: string) => {
      if (e.button === 2) return;
      if (e.button !== 0 && e.pointerType === "mouse") return;
      e.preventDefault();
      e.stopPropagation();
      setMenu(null);
      const cur = positions[id] ?? { x: 0, y: 0 };
      dragRef.current = {
        id,
        pointerId: e.pointerId,
        startX: e.clientX,
        startY: e.clientY,
        origX: cur.x,
        origY: cur.y,
      };
      setDragging(id);
      onSelect?.(id);
      (e.currentTarget as HTMLDivElement).setPointerCapture?.(e.pointerId);
    },
    [positions, onSelect],
  );

  const onPointerMove = useCallback(
    (e: React.PointerEvent<HTMLDivElement>) => {
      const d = dragRef.current;
      if (!d || d.pointerId !== e.pointerId) return;
      const dx = (e.clientX - d.startX) / scale;
      const dy = (e.clientY - d.startY) / scale;
      const nx = Math.max(0, Math.min(world.w - TABLE_W, d.origX + dx));
      const ny = Math.max(0, Math.min(world.h - TABLE_H, d.origY + dy));
      setPositions((prev) => ({ ...prev, [d.id]: { x: nx, y: ny } }));
    },
    [scale, world.w, world.h],
  );

  const onPointerUp = useCallback(
    (e: React.PointerEvent<HTMLDivElement>) => {
      const d = dragRef.current;
      if (!d || d.pointerId !== e.pointerId) return;
      setPositions((prev) => {
        const cur = prev[d.id];
        if (!cur) return prev;
        return {
          ...prev,
          [d.id]: {
            x: Math.max(0, Math.min(world.w - TABLE_W, snap(cur.x))),
            y: Math.max(0, Math.min(world.h - TABLE_H, snap(cur.y))),
          },
        };
      });
      dragRef.current = null;
      setDragging(null);
      try {
        (e.currentTarget as HTMLDivElement).releasePointerCapture?.(e.pointerId);
      } catch {
        /* ignore */
      }
    },
    [world.w, world.h],
  );

  // ----- Door drag handlers -----
  const onDoorPointerDown = useCallback(
    (e: React.PointerEvent<HTMLDivElement>, idx: number) => {
      e.preventDefault();
      e.stopPropagation();
      setDoorMode(false);
      const door = decorItems[idx] as FloorDoor;
      doorDragRef.current = {
        idx,
        pointerId: e.pointerId,
        startX: e.clientX,
        startY: e.clientY,
        origX: door.x,
        origY: door.y,
      };
      (e.currentTarget as HTMLDivElement).setPointerCapture?.(e.pointerId);
    },
    [decorItems],
  );

  const onDoorPointerMove = useCallback(
    (e: React.PointerEvent<HTMLDivElement>) => {
      const d = doorDragRef.current;
      if (!d || d.pointerId !== e.pointerId) return;
      const dx = (e.clientX - d.startX) / scale / world.w;
      const dy = (e.clientY - d.startY) / scale / world.h;
      const nx = Math.max(0, Math.min(1, d.origX + dx));
      const ny = Math.max(0, Math.min(1, d.origY + dy));
      setDecorItems((prev) =>
        prev.map((item, i) => (i === d.idx ? { ...item, x: nx, y: ny } as FloorDoor : item)),
      );
    },
    [scale, world.w, world.h],
  );

  const onDoorPointerUp = useCallback(
    (e: React.PointerEvent<HTMLDivElement>) => {
      if (!doorDragRef.current || doorDragRef.current.pointerId !== e.pointerId) return;
      doorDragRef.current = null;
      try {
        (e.currentTarget as HTMLDivElement).releasePointerCapture?.(e.pointerId);
      } catch {
        /* ignore */
      }
    },
    [],
  );

  // ----- Canvas click for door placement -----
  const onWorldClick = useCallback(
    (e: React.MouseEvent<HTMLDivElement>) => {
      if (e.target !== e.currentTarget && !doorMode) {
        return;
      }
      if (doorMode) {
        const rect = (e.currentTarget as HTMLDivElement).getBoundingClientRect();
        const wx = (e.clientX - rect.left) / scale;
        const wy = (e.clientY - rect.top) / scale;
        setDecorItems((prev) => [
          ...prev,
          { kind: "door", x: wx / world.w, y: wy / world.h },
        ]);
        setDoorMode(false);
        e.stopPropagation();
        return;
      }
      if (e.target === e.currentTarget) {
        onSelect?.(null);
        setMenu(null);
      }
    },
    [doorMode, scale, world.w, world.h, onSelect],
  );

  const openMenu = (e: React.MouseEvent, id: string) => {
    e.preventDefault();
    e.stopPropagation();
    onSelect?.(id);
    setMenu({ x: e.clientX, y: e.clientY, tableId: id });
  };

  const selected = selectedId ? tables.find((t) => t.id === selectedId) ?? null : null;
  const menuTable = menu ? tables.find((t) => t.id === menu.tableId) ?? null : null;

  const gridBg = useMemo(() => {
    const major = GRID * 5;
    return `
      repeating-linear-gradient(to right, #eef2f7 0 1px, transparent 1px ${GRID}px),
      repeating-linear-gradient(to bottom, #eef2f7 0 1px, transparent 1px ${GRID}px),
      repeating-linear-gradient(to right, #cbd5e1 0 1px, transparent 1px ${major}px),
      repeating-linear-gradient(to bottom, #cbd5e1 0 1px, transparent 1px ${major}px)
    `;
  }, []);

  const doorCount = decorItems.filter((d) => d.kind === "door").length;

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h3 className="text-sm font-bold text-[color:var(--fg)]">
            Joylashuv xaritasi —{" "}
            <span className="text-[color:var(--brand)]">{zoneName}</span>
          </h3>
          <p className="text-xs text-[color:var(--muted)]">
            Stolni sichqoncha bilan suring — katakka yopishadi ({GRID}px). O'ng tugma — kontekst menyu.
          </p>
        </div>
        <div className="flex items-center gap-2 text-xs">
          <span className="rounded-full bg-[color:var(--bg-soft)] px-2 py-1 text-[color:var(--fg)]">
            Jami: <b>{tables.length}</b>
          </span>
          {changed.length > 0 ? (
            <span className="rounded-full bg-amber-100 px-2 py-1 text-amber-900">
              Saqlanmagan: <b>{changed.length}</b>
            </span>
          ) : (
            <span className="rounded-full bg-[color:var(--brand)]/10 px-2 py-1 text-[color:var(--brand)]">
              ✓ Saqlangan
            </span>
          )}
        </div>
      </div>

      {/* Toolbar */}
      <div className="flex flex-wrap gap-2">
        <button
          type="button"
          onClick={() => void save()}
          disabled={saving || !changed.length}
          className="bq-btn bq-btn-primary h-9 text-sm"
        >
          {saving ? "Saqlanmoqda…" : `Saqlash${changed.length ? ` (${changed.length})` : ""}`}
        </button>
        <button
          type="button"
          onClick={reset}
          disabled={!changed.length || saving}
          className="bq-btn bq-btn-ghost h-9 text-sm"
        >
          Bekor qilish
        </button>
        <button
          type="button"
          onClick={autoArrange}
          disabled={saving || !tables.length}
          className="bq-btn bq-btn-ghost h-9 text-sm"
        >
          Avto-joylashtirish
        </button>

        {/* Door mode toggle */}
        <button
          type="button"
          onClick={() => setDoorMode((v) => !v)}
          className={`h-9 rounded-xl border px-3 text-sm font-semibold transition ${
            doorMode
              ? "border-blue-500 bg-blue-600 text-white shadow-[0_0_0_3px_rgba(59,130,246,0.35)]"
              : "border-[color:var(--border)] bg-[color:var(--bg-soft)] text-[color:var(--fg)] hover:border-blue-400 hover:text-blue-700"
          }`}
          title="Eshik qo'shish rejimi — canvasga bosing"
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="mr-1.5 inline-block align-[-2px]">
            <path d="M3 21h18" />
            <path d="M5 21V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v16" />
            <circle cx="14" cy="13" r="1" fill="currentColor" />
          </svg>
          {doorMode ? "Canvasga bosing…" : `Eshik qo'shish${doorCount ? ` (${doorCount})` : ""}`}
        </button>

        {/* Background */}
        <input
          ref={fileInputRef}
          type="file"
          accept="image/png,image/svg+xml,image/jpeg,image/webp"
          className="hidden"
          onChange={(e) => void onBackgroundChosen(e.target.files?.[0] ?? null)}
        />
        <button
          type="button"
          onClick={onPickBackground}
          className="bq-btn bq-btn-ghost h-9 text-sm"
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="mr-1 inline-block align-[-2px]">
            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
            <path d="M17 8l-5-5-5 5" />
            <path d="M12 3v12" />
          </svg>
          {bgUrl ? "Tarhi almashtirish" : "Zal tarhi yuklash"}
        </button>
        {bgUrl ? (
          <>
            <div className="flex items-center gap-2 rounded-xl border border-[color:var(--border)] bg-white px-3 py-1.5 text-xs text-[color:var(--muted)]">
              <span>Tiniqligi</span>
              <input
                type="range"
                min={10}
                max={100}
                step={5}
                value={Math.round(bgOpacity * 100)}
                onChange={(e) => changeBgOpacity(Number(e.target.value) / 100)}
                className="h-1 w-24 accent-[color:var(--brand)]"
              />
              <span className="w-8 text-right font-mono text-[color:var(--fg)]">
                {Math.round(bgOpacity * 100)}%
              </span>
            </div>
            <button
              type="button"
              onClick={clearBackground}
              className="bq-btn bq-btn-ghost h-9 text-sm"
            >
              Tarhi olib tashlash
            </button>
          </>
        ) : null}

        {selected ? (
          <div className="ml-auto flex items-center gap-1 rounded-xl border border-[color:var(--brand)]/40 bg-[color:var(--brand)]/10 px-2 py-1 text-xs">
            <span className="text-[color:var(--fg)]">
              Tanlangan: <b>{selected.number}</b>
            </span>
            <button type="button" onClick={() => moveBy(selected.id, -GRID, 0)} className="rounded bg-white px-1.5 py-0.5 shadow-sm">←</button>
            <button type="button" onClick={() => moveBy(selected.id, 0, -GRID)} className="rounded bg-white px-1.5 py-0.5 shadow-sm">↑</button>
            <button type="button" onClick={() => moveBy(selected.id, 0, GRID)} className="rounded bg-white px-1.5 py-0.5 shadow-sm">↓</button>
            <button type="button" onClick={() => moveBy(selected.id, GRID, 0)} className="rounded bg-white px-1.5 py-0.5 shadow-sm">→</button>
            <button type="button" onClick={() => onSelect?.(null)} className="rounded bg-[color:var(--bg-soft)] px-1.5 py-0.5">✕</button>
          </div>
        ) : null}
      </div>

      {/* Door mode hint */}
      {doorMode ? (
        <div className="flex items-center gap-2 rounded-xl border border-blue-300 bg-blue-50 px-3 py-2 text-sm text-blue-800">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="shrink-0">
            <circle cx="12" cy="12" r="10" />
            <line x1="12" y1="8" x2="12" y2="12" />
            <line x1="12" y1="16" x2="12.01" y2="16" />
          </svg>
          Eshik qo&apos;shish rejimi faol — canvasdagi istalgan joyni bosing. Eshikni keyin suring.
          <button type="button" onClick={() => setDoorMode(false)} className="ml-auto rounded-lg border border-blue-300 px-2 py-0.5 text-xs font-semibold hover:bg-blue-100">Bekor</button>
        </div>
      ) : null}

      {err ? (
        <p className="rounded-xl border border-[color:var(--danger)]/40 bg-red-50 px-3 py-2 text-xs text-red-700">
          {err}
        </p>
      ) : null}

      <div className="flex flex-wrap gap-3 text-xs">
        <LegendChip color={TYPE_COLORS.STANDARD} label="STANDARD" />
        <LegendChip color={TYPE_COLORS.VIP} label="VIP" />
        <LegendChip color={TYPE_COLORS.FAMILY} label="FAMILY" />
        <LegendChip color="#6b7280" label="MAINTENANCE" outline />
        <LegendChip color="#1d4ed8" label="Eshik" />
      </div>

      {/* Canvas */}
      <div
        ref={containerRef}
        onContextMenu={(e) => e.preventDefault()}
        className="relative w-full overflow-hidden rounded-2xl border border-[color:var(--border)] shadow-inner"
        style={{
          minHeight: STAGE_MIN_H,
          height: stageH,
          background: "#ffffff",
          touchAction: "none",
          cursor: doorMode ? "crosshair" : "default",
        }}
      >
        {tables.length === 0 && decorItems.filter(d => d.kind === "door").length === 0 ? (
          <div className="flex h-64 items-center justify-center text-sm text-[color:var(--muted)]">
            Bu zonada stollar yo&apos;q. Pastdan qo&apos;shing yoki ommaviy yarating.
          </div>
        ) : (
          <div
            style={{
              position: "absolute",
              inset: 0,
              width: world.w,
              height: world.h,
              transform: `scale(${scale})`,
              transformOrigin: "top left",
              backgroundColor: "#ffffff",
              cursor: doorMode ? "crosshair" : "default",
            }}
            onClick={onWorldClick}
          >
            {/* Grid */}
            <div
              style={{
                position: "absolute",
                inset: 0,
                backgroundImage: gridBg,
                pointerEvents: "none",
              }}
            />
            {/* Background image */}
            {bgUrl ? (
              // eslint-disable-next-line @next/next/no-img-element
              <img
                src={bgUrl}
                alt="Zal tarhi"
                style={{
                  position: "absolute",
                  inset: 0,
                  width: "100%",
                  height: "100%",
                  objectFit: "contain",
                  opacity: bgOpacity,
                  pointerEvents: "none",
                }}
              />
            ) : null}

            {/* Doors (draggable) */}
            {decorItems.map((d, itemIdx) => {
              if (d.kind !== "door") return null;
              const door = d as FloorDoor;
              const wx = door.x * world.w;
              const wy = door.y * world.h;
              return (
                <div
                  key={`door-${itemIdx}`}
                  onPointerDown={(e) => onDoorPointerDown(e, itemIdx)}
                  onPointerMove={onDoorPointerMove}
                  onPointerUp={onDoorPointerUp}
                  onPointerCancel={onDoorPointerUp}
                  onClick={(e) => e.stopPropagation()}
                  title="Eshik — suring yoki olib tashlang"
                  style={{
                    position: "absolute",
                    left: wx - DOOR_SIZE / 2,
                    top: wy - DOOR_SIZE / 2,
                    width: DOOR_SIZE,
                    height: DOOR_SIZE,
                    background: "#1d4ed8",
                    borderRadius: 10,
                    border: "2.5px solid #93c5fd",
                    boxShadow: "0 4px 12px rgba(29,78,216,0.4)",
                    display: "flex",
                    flexDirection: "column",
                    alignItems: "center",
                    justifyContent: "center",
                    cursor: "grab",
                    zIndex: 4,
                    userSelect: "none",
                    color: "white",
                  }}
                >
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M3 21h18" />
                    <path d="M5 21V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v16" />
                    <circle cx="14" cy="13" r="1.2" fill="white" stroke="none" />
                  </svg>
                  <span style={{ fontSize: 8, opacity: 0.85, marginTop: 1 }}>E{itemIdx + 1}</span>
                </div>
              );
            })}

            {/* Tables */}
            {tables.map((t) => {
              const p = positions[t.id] ?? { x: toNum(t.xPos), y: toNum(t.yPos) };
              const isSel = t.id === selectedId;
              const isMoved = p.x !== toNum(t.xPos) || p.y !== toNum(t.yPos);
              const isDrag = dragging === t.id;
              const baseColor = TYPE_COLORS[t.type] ?? TYPE_COLORS.STANDARD;
              const borderColor = isSel
                ? "#f59e0b"
                : isMoved
                ? "#eab308"
                : STATUS_BORDER[t.status] ?? "rgba(0,0,0,0.15)";
              const borderWidth = isSel ? 4 : isMoved || t.status !== "AVAILABLE" ? 3 : 1;
              const borderStyle = t.status !== "AVAILABLE" && !isSel ? "dashed" : "solid";
              return (
                <div
                  key={t.id}
                  onPointerDown={(e) => onPointerDown(e, t.id)}
                  onPointerMove={onPointerMove}
                  onPointerUp={onPointerUp}
                  onPointerCancel={onPointerUp}
                  onContextMenu={(e) => openMenu(e, t.id)}
                  onClick={(e) => {
                    e.stopPropagation();
                    if (!doorMode) onSelect?.(t.id);
                  }}
                  style={{
                    position: "absolute",
                    left: p.x,
                    top: p.y,
                    width: TABLE_W,
                    height: TABLE_H,
                    background: baseColor,
                    color: "#ffffff",
                    borderRadius: 12,
                    border: `${borderWidth}px ${borderStyle} ${borderColor}`,
                    boxShadow: isSel
                      ? "0 8px 24px rgba(245,158,11,0.35)"
                      : isDrag
                      ? "0 10px 20px rgba(0,0,0,0.35)"
                      : "0 4px 10px rgba(0,0,0,0.25)",
                    cursor: isDrag ? "grabbing" : "grab",
                    userSelect: "none",
                    padding: "8px 10px",
                    transition: isDrag ? "none" : "box-shadow 120ms ease",
                    zIndex: isDrag ? 10 : isSel ? 5 : 1,
                  }}
                >
                  <div style={{ fontWeight: 700, fontSize: 16, lineHeight: 1.1 }}>{t.number}</div>
                  <div style={{ fontSize: 11, opacity: 0.95 }}>{t.seats} kishilik</div>
                  <div style={{ fontSize: 10, fontWeight: 700, opacity: 0.9 }}>{t.type}</div>
                  {t.status !== "AVAILABLE" ? (
                    <div
                      style={{
                        position: "absolute",
                        right: 6,
                        bottom: 4,
                        fontSize: 9,
                        background: "rgba(0,0,0,0.35)",
                        padding: "1px 4px",
                        borderRadius: 4,
                      }}
                    >
                      {STATUS_LABEL[t.status] ?? t.status}
                    </div>
                  ) : null}
                  {isMoved ? (
                    <div
                      style={{
                        position: "absolute",
                        top: 4,
                        right: 6,
                        fontSize: 12,
                        color: "#fbbf24",
                      }}
                    >
                      ●
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        )}
      </div>

      <div className="flex flex-wrap gap-3 text-[11px] text-[color:var(--muted)]">
        <span>Stage: <code>{stageW}×{stageH}</code></span>
        <span>World: <code>{world.w}×{world.h}</code></span>
        <span>Scale: <code>{scale.toFixed(3)}</code></span>
        <span>Stollar: <code>{tables.length}</code></span>
      </div>

      {/* Decorations panel (labels only now — doors are on canvas) */}
      <details className="rounded-2xl border border-[color:var(--border)] bg-white">
        <summary className="cursor-pointer select-none px-4 py-3 text-sm font-semibold text-[color:var(--fg)]">
          Dekoratsiyalar — eshiklar va belgilar
          <span className="ml-2 rounded-full bg-[color:var(--bg-soft)] px-2 py-0.5 text-[10px] text-[color:var(--muted)]">
            {decorItems.length} ta
          </span>
        </summary>
        <div className="border-t border-[color:var(--border)] p-4 space-y-4">

          {/* Doors info */}
          <div>
            <div className="mb-2 text-xs font-bold uppercase tracking-wider text-[color:var(--muted)]">Eshiklar</div>
            <p className="mb-2 text-xs text-[color:var(--muted)]">
              Eshiklarni yuqoridagi <b>Eshik qo&apos;shish</b> tugmasi orqali canvasga qo&apos;ying. Suring — pozitsiya yangilanadi.
            </p>
            {decorItems.filter(d => d.kind === "door").length === 0 ? (
              <p className="text-xs text-[color:var(--muted)] italic">Hali eshik qo&apos;shilmagan.</p>
            ) : (
              <div className="flex flex-wrap gap-1.5">
                {decorItems.map((d, i) => {
                  if (d.kind !== "door") return null;
                  const door = d as FloorDoor;
                  return (
                    <div key={i} className="flex items-center gap-2 rounded-xl border border-blue-200 bg-blue-50 px-2.5 py-1.5 text-xs">
                      <span className="font-semibold text-blue-800">
                        Eshik {i + 1} — ({Math.round(door.x * 100)}%, {Math.round(door.y * 100)}%)
                      </span>
                      <button
                        type="button"
                        onClick={() => setDecorItems((prev) => prev.filter((_, j) => j !== i))}
                        className="rounded border border-rose-200 bg-rose-50 px-1.5 py-0.5 text-[10px] font-bold text-rose-700 hover:bg-rose-100"
                      >
                        ✕
                      </button>
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          {/* Labels */}
          <div>
            <div className="mb-2 text-xs font-bold uppercase tracking-wider text-[color:var(--muted)]">Belgilar</div>
            <div className="flex flex-wrap gap-2 mb-2">
              <button
                type="button"
                onClick={() => setDecorItems((prev) => [...prev, { kind: "label", text: "BAR", style: "bar", x: 0.05, y: 0.05 }])}
                className="rounded-xl border border-amber-300 bg-amber-50 px-3 py-1.5 text-xs font-semibold text-amber-800 transition hover:bg-amber-100"
              >
                + BAR
              </button>
              <button
                type="button"
                onClick={() => setDecorItems((prev) => [...prev, { kind: "label", text: "OSHXONA", style: "kitchen", x: 0.75, y: 0.85 }])}
                className="rounded-xl border border-slate-300 bg-slate-50 px-3 py-1.5 text-xs font-semibold text-slate-800 transition hover:bg-slate-100"
              >
                + OSHXONA
              </button>
              <div className="flex items-center gap-1">
                <input
                  value={newLabelText}
                  onChange={(e) => setNewLabelText(e.target.value)}
                  placeholder="Matn…"
                  className="rounded-xl border border-[color:var(--border)] px-3 py-1.5 text-xs w-28"
                  onKeyDown={(e) => {
                    if (e.key === "Enter" && newLabelText.trim()) {
                      setDecorItems((prev) => [...prev, { kind: "label", text: newLabelText.trim(), style: "default", x: 0.4, y: 0.4 }]);
                      setNewLabelText("");
                    }
                  }}
                />
                <button
                  type="button"
                  disabled={!newLabelText.trim()}
                  onClick={() => {
                    if (!newLabelText.trim()) return;
                    setDecorItems((prev) => [...prev, { kind: "label", text: newLabelText.trim(), style: "default", x: 0.4, y: 0.4 }]);
                    setNewLabelText("");
                  }}
                  className="rounded-xl bg-[color:var(--brand)] px-3 py-1.5 text-xs font-semibold text-white disabled:opacity-50"
                >
                  + Qo'shish
                </button>
              </div>
            </div>
          </div>

          {/* Label list */}
          {decorItems.filter(d => d.kind === "label").length > 0 ? (
            <div className="space-y-1.5">
              <div className="text-xs font-bold uppercase tracking-wider text-[color:var(--muted)]">Qo'shilgan belgilar</div>
              {decorItems.map((d, i) => {
                if (d.kind !== "label") return null;
                return (
                  <div key={i} className="flex items-center justify-between gap-3 rounded-xl border border-[color:var(--border)] bg-[color:var(--bg-soft)] px-3 py-2 text-xs">
                    <span className="font-semibold text-[color:var(--fg)]">Belgi: &quot;{(d as FloorLabel).text}&quot;</span>
                    <button
                      type="button"
                      onClick={() => setDecorItems((prev) => prev.filter((_, j) => j !== i))}
                      className="rounded-lg border border-rose-200 bg-rose-50 px-2 py-0.5 text-[10px] font-bold text-rose-700 hover:bg-rose-100"
                    >
                      O'chirish
                    </button>
                  </div>
                );
              })}
            </div>
          ) : null}

          <div className="flex items-center gap-3">
            <button
              type="button"
              onClick={() => void saveDecor()}
              disabled={decorSaving}
              className="bq-btn bq-btn-primary h-9 text-sm"
            >
              {decorSaving ? "Saqlanmoqda…" : "Dekoratsiyalarni saqlash"}
            </button>
            {decorMsg ? (
              <span className={`text-xs ${decorMsg.startsWith("Xatolik") ? "text-rose-600" : "text-emerald-700"}`}>
                {decorMsg}
              </span>
            ) : null}
          </div>
        </div>
      </details>

      {/* Context menu */}
      {menu && menuTable ? (
        <ContextMenu
          x={menu.x}
          y={menu.y}
          table={menuTable}
          onClose={() => setMenu(null)}
          onStatusChange={(s) => {
            onStatusChange?.(menuTable.id, s);
            setMenu(null);
          }}
          onDuplicate={() => {
            onDuplicate?.(menuTable.id);
            setMenu(null);
          }}
          onDelete={() => {
            onDelete?.(menuTable.id);
            setMenu(null);
          }}
        />
      ) : null}
    </div>
  );
}

function ContextMenu({
  x,
  y,
  table,
  onClose,
  onStatusChange,
  onDuplicate,
  onDelete,
}: {
  x: number;
  y: number;
  table: EditorTable;
  onClose: () => void;
  onStatusChange: (s: string) => void;
  onDuplicate: () => void;
  onDelete: () => void;
}) {
  const ref = useRef<HTMLDivElement | null>(null);
  const [pos, setPos] = useState({ x, y });

  useEffect(() => {
    if (!ref.current) return;
    const rect = ref.current.getBoundingClientRect();
    const pad = 8;
    let nx = x;
    let ny = y;
    if (x + rect.width + pad > window.innerWidth) nx = window.innerWidth - rect.width - pad;
    if (y + rect.height + pad > window.innerHeight) ny = window.innerHeight - rect.height - pad;
    setPos({ x: Math.max(pad, nx), y: Math.max(pad, ny) });
  }, [x, y]);

  return (
    <div
      ref={ref}
      onPointerDown={(e) => e.stopPropagation()}
      className="fixed z-50 w-56 overflow-hidden rounded-xl border border-[color:var(--border)] bg-white shadow-[var(--shadow-md)]"
      style={{ left: pos.x, top: pos.y }}
    >
      <div className="border-b border-[color:var(--border)] bg-[color:var(--bg-soft)] px-3 py-2">
        <div className="text-xs text-[color:var(--muted)]">Stol</div>
        <div className="text-sm font-bold text-[color:var(--fg)]">
          {table.number}{" "}
          <span className="font-normal text-[color:var(--muted)]">
            · {table.seats} o'rin
          </span>
        </div>
      </div>
      <div className="px-2 pb-1 pt-2">
        <div className="mb-1 px-1 text-[10px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
          Holat
        </div>
        <MenuItem label="Bo'sh" dot="#15803d" active={table.status === "AVAILABLE"} onClick={() => onStatusChange("AVAILABLE")} />
        <MenuItem label="Band" dot="#b91c1c" active={table.status === "OCCUPIED"} onClick={() => onStatusChange("OCCUPIED")} />
        <MenuItem label="Zahirada" dot="#ea580c" active={table.status === "RESERVED"} onClick={() => onStatusChange("RESERVED")} />
        <MenuItem label="Ta'mirda" dot="#6b7280" active={table.status === "MAINTENANCE"} onClick={() => onStatusChange("MAINTENANCE")} />
      </div>
      <div className="border-t border-[color:var(--border)] py-1">
        <MenuItem label="Dublikat qilish" icon="⎘" onClick={onDuplicate} />
        <MenuItem label="O'chirish" icon="🗑" danger onClick={onDelete} />
      </div>
      <div className="border-t border-[color:var(--border)] py-1">
        <MenuItem label="Yopish" icon="✕" muted onClick={onClose} />
      </div>
    </div>
  );
}

function MenuItem({
  label,
  icon,
  dot,
  active,
  danger,
  muted,
  onClick,
}: {
  label: string;
  icon?: string;
  dot?: string;
  active?: boolean;
  danger?: boolean;
  muted?: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`flex w-full items-center gap-2 rounded-lg px-2 py-1.5 text-left text-sm transition ${
        danger
          ? "text-[color:var(--danger)] hover:bg-red-50"
          : muted
            ? "text-[color:var(--muted)] hover:bg-[color:var(--bg-soft)]"
            : active
              ? "bg-[color:var(--brand)]/10 font-semibold text-[color:var(--brand)]"
              : "text-[color:var(--fg)] hover:bg-[color:var(--bg-soft)]"
      }`}
    >
      {dot ? (
        <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ background: dot }} />
      ) : icon ? (
        <span className="inline-flex w-4 justify-center text-xs">{icon}</span>
      ) : null}
      <span className="flex-1">{label}</span>
      {active ? <span className="text-[color:var(--brand)]">✓</span> : null}
    </button>
  );
}

function LegendChip({ color, label, outline }: { color: string; label: string; outline?: boolean }) {
  return (
    <span className="inline-flex items-center gap-1.5 rounded-md border border-[color:var(--border)] bg-white px-2 py-0.5 text-[color:var(--fg)]">
      <span
        className="inline-block h-3 w-3 rounded-sm"
        style={outline ? { border: `2px dashed ${color || "#6b7280"}`, backgroundColor: "transparent" } : { backgroundColor: color }}
        aria-hidden
      />
      {label}
    </span>
  );
}
