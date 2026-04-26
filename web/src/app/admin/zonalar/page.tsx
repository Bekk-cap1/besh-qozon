"use client";

import { useCallback, useEffect, useState } from "react";
import { AdminShell } from "@/components/admin/AdminShell";
import { apiFetch } from "@/lib/api";
import { useAdminToken } from "@/lib/adminAuth";
import { useBeshSocket } from "@/lib/useBeshSocket";
import { AdminFloorPlanEditor } from "@/components/AdminFloorPlanEditor";

type Branch = { id: string; name: string };
type Zone = { id: string; name: string; type: string; branchId: string; floorPlanSvg?: string | null };
type TableRow = {
  id: string;
  zoneId: string;
  number: string;
  seats: number;
  type: string;
  status: string;
  xPos: number;
  yPos: number;
  shape?: string | null;
};

const TABLE_TYPES = ["STANDARD", "VIP", "FAMILY"] as const;
const TABLE_STATUSES = ["AVAILABLE", "OCCUPIED", "RESERVED", "MAINTENANCE"] as const;
const ZONE_TYPES = ["INDOOR", "OUTDOOR", "VIP"] as const;

export default function AdminZonalarPage() {
  return (
    <AdminShell
      title="Zonalar va stollar"
      subtitle="Zal sxemasi, zonalar va stollarni boshqarish"
    >
      <CatalogBody />
    </AdminShell>
  );
}

function CatalogBody() {
  const { token } = useAdminToken();
  const [err, setErr] = useState<string | null>(null);

  const [branches, setBranches] = useState<Branch[]>([]);
  const [branchId, setBranchId] = useState("");
  const [zones, setZones] = useState<Zone[]>([]);
  const [zoneId, setZoneId] = useState("");
  const [tables, setTables] = useState<TableRow[]>([]);
  const [loading, setLoading] = useState(false);

  const [newZoneName, setNewZoneName] = useState("Ko'cha");
  const [newZoneType, setNewZoneType] = useState<(typeof ZONE_TYPES)[number]>("OUTDOOR");

  const [newNumber, setNewNumber] = useState("");
  const [newSeats, setNewSeats] = useState(4);
  const [newType, setNewType] = useState<(typeof TABLE_TYPES)[number]>("STANDARD");
  const [newX, setNewX] = useState(80);
  const [newY, setNewY] = useState(80);

  const [bulkCount, setBulkCount] = useState(8);
  const [bulkPrefix, setBulkPrefix] = useState("K");
  const [bulkSeats, setBulkSeats] = useState(4);
  const [bulkCols, setBulkCols] = useState(8);
  const [bulkType, setBulkType] = useState<(typeof TABLE_TYPES)[number]>("STANDARD");

  const [selectedTableId, setSelectedTableId] = useState<string | null>(null);
  const [view, setView] = useState<"map" | "table">("map");

  const loadBranches = useCallback(async () => {
    setErr(null);
    try {
      const b = await apiFetch<Branch[]>("/branches");
      setBranches(b);
      setBranchId((cur) => cur || b[0]?.id || "");
    } catch (e) {
      setErr((e as Error).message);
    }
  }, []);

  const loadZones = useCallback(async () => {
    if (!branchId) {
      setZones([]);
      setZoneId("");
      return;
    }
    setErr(null);
    try {
      const z = await apiFetch<Zone[]>(`/branches/${branchId}/zones`);
      setZones(z);
      setZoneId((cur) => (cur && z.some((x) => x.id === cur) ? cur : z[0]?.id ?? ""));
    } catch (e) {
      setErr((e as Error).message);
    }
  }, [branchId]);

  const loadTables = useCallback(async () => {
    if (!zoneId || !token) {
      setTables([]);
      return;
    }
    setLoading(true);
    setErr(null);
    try {
      const t = await apiFetch<TableRow[]>(`/admin/zones/${zoneId}/tables`, { token });
      setTables(t);
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setLoading(false);
    }
  }, [zoneId, token]);

  useEffect(() => {
    void loadBranches();
  }, [loadBranches]);
  useEffect(() => {
    void loadZones();
  }, [loadZones]);
  useEffect(() => {
    void loadTables();
  }, [loadTables]);

  useBeshSocket({
    zoneId,
    onTablesRefresh: (p) => {
      if (!p.zoneId || p.zoneId === zoneId) void loadTables();
    },
  });

  const createZone = async () => {
    if (!branchId || !newZoneName.trim() || !token) return;
    setLoading(true);
    setErr(null);
    try {
      const z = await apiFetch<Zone>("/admin/zones", {
        method: "POST",
        token,
        body: JSON.stringify({ branchId, name: newZoneName.trim(), type: newZoneType }),
      });
      await loadZones();
      setZoneId(z.id);
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setLoading(false);
    }
  };

  const addTable = async () => {
    if (!zoneId || !newNumber.trim() || !token) return;
    setLoading(true);
    setErr(null);
    try {
      await apiFetch("/admin/tables", {
        method: "POST",
        token,
        body: JSON.stringify({
          zoneId,
          number: newNumber.trim(),
          seats: newSeats,
          type: newType,
          xPos: newX,
          yPos: newY,
          shape: "rect",
        }),
      });
      setNewNumber("");
      await loadTables();
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setLoading(false);
    }
  };

  const bulkAdd = async () => {
    if (!zoneId || !token) return;
    setLoading(true);
    setErr(null);
    try {
      await apiFetch(`/admin/zones/${zoneId}/tables/bulk`, {
        method: "POST",
        token,
        body: JSON.stringify({
          count: bulkCount,
          prefix: bulkPrefix,
          seatsPerTable: bulkSeats,
          type: bulkType,
          columnsPerRow: bulkCols,
        }),
      });
      await loadTables();
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setLoading(false);
    }
  };

  const saveRow = async (row: TableRow) => {
    if (!token) return;
    setLoading(true);
    setErr(null);
    try {
      await apiFetch(`/admin/tables/${row.id}`, {
        method: "PUT",
        token,
        body: JSON.stringify({
          number: row.number,
          seats: row.seats,
          type: row.type,
          status: row.status,
          xPos: row.xPos,
          yPos: row.yPos,
        }),
      });
      await loadTables();
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setLoading(false);
    }
  };

  const deleteRow = async (id: string) => {
    if (!token) return;
    if (!confirm("Stolni o'chirish? Bronlar bog'langan bo'lsa, xato chiqishi mumkin.")) return;
    setLoading(true);
    setErr(null);
    try {
      await apiFetch(`/admin/tables/${id}`, { method: "DELETE", token });
      await loadTables();
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setLoading(false);
    }
  };

  const patchRow = (id: string, patch: Partial<TableRow>) => {
    setTables((prev) => prev.map((t) => (t.id === id ? { ...t, ...patch } : t)));
  };

  return (
    <div className="space-y-5">
      {/* Верхний селектор */}
      <div className="flex flex-wrap items-end gap-3 rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)]">
        <div className="min-w-[160px] flex-1">
          <label className="mb-1 block text-[11px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
            Filial
          </label>
          <select
            className="bq-input h-10"
            value={branchId}
            onChange={(e) => setBranchId(e.target.value)}
          >
            {branches.map((b) => (
              <option key={b.id} value={b.id}>
                {b.name}
              </option>
            ))}
          </select>
        </div>
        <div className="min-w-[180px] flex-1">
          <label className="mb-1 block text-[11px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
            Zona
          </label>
          <select
            className="bq-input h-10"
            value={zoneId}
            onChange={(e) => setZoneId(e.target.value)}
          >
            {zones.map((z) => (
              <option key={z.id} value={z.id}>
                {z.name} · {z.type}
              </option>
            ))}
          </select>
        </div>
        {loading ? (
          <span className="pb-2 text-xs text-[color:var(--muted)]">Yuklanmoqda…</span>
        ) : null}
      </div>

      {err ? (
        <div className="rounded-xl border border-[color:var(--danger)]/40 bg-red-50 px-3 py-2 text-sm text-red-700">
          {err}
        </div>
      ) : null}

      {/* Зона управления */}
      <div className="grid gap-4 lg:grid-cols-2">
        <section className="rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)]">
          <h2 className="mb-3 text-sm font-bold text-[color:var(--fg)]">
            Yangi zona qo'shish
          </h2>
          <div className="flex flex-wrap items-end gap-2">
            <div className="min-w-[140px] flex-1">
              <label className="mb-1 block text-[11px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
                Nomi
              </label>
              <input
                className="bq-input h-10"
                value={newZoneName}
                onChange={(e) => setNewZoneName(e.target.value)}
              />
            </div>
            <div className="min-w-[120px]">
              <label className="mb-1 block text-[11px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
                Turi
              </label>
              <select
                className="bq-input h-10"
                value={newZoneType}
                onChange={(e) => setNewZoneType(e.target.value as (typeof ZONE_TYPES)[number])}
              >
                {ZONE_TYPES.map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
            </div>
            <button
              type="button"
              disabled={loading || !branchId}
              className="bq-btn bq-btn-primary h-10 text-sm"
              onClick={() => void createZone()}
            >
              Zona qo'shish
            </button>
          </div>
        </section>

        <section className="rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)]">
          <h2 className="mb-1 text-sm font-bold text-[color:var(--fg)]">
            Ko'p stol yaratish
          </h2>
          <p className="mb-3 text-[11px] text-[color:var(--muted)]">
            prefiks-1, prefiks-2, … avtomatik joylashadi
          </p>
          <div className="flex flex-wrap items-end gap-2">
            <div>
              <label className="mb-1 block text-[11px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
                Soni
              </label>
              <input
                type="number"
                min={1}
                max={80}
                className="bq-input h-10 w-20"
                value={bulkCount}
                onChange={(e) => setBulkCount(Number(e.target.value))}
              />
            </div>
            <div>
              <label className="mb-1 block text-[11px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
                Prefiks
              </label>
              <input
                className="bq-input h-10 w-24"
                value={bulkPrefix}
                maxLength={8}
                onChange={(e) => setBulkPrefix(e.target.value)}
              />
            </div>
            <div>
              <label className="mb-1 block text-[11px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
                O'rinlar
              </label>
              <input
                type="number"
                min={1}
                max={30}
                className="bq-input h-10 w-20"
                value={bulkSeats}
                onChange={(e) => setBulkSeats(Number(e.target.value))}
              />
            </div>
            <div>
              <label className="mb-1 block text-[11px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
                Qator
              </label>
              <input
                type="number"
                min={1}
                max={20}
                className="bq-input h-10 w-20"
                value={bulkCols}
                onChange={(e) => setBulkCols(Number(e.target.value))}
              />
            </div>
            <div>
              <label className="mb-1 block text-[11px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
                Turi
              </label>
              <select
                className="bq-input h-10"
                value={bulkType}
                onChange={(e) => setBulkType(e.target.value as (typeof TABLE_TYPES)[number])}
              >
                {TABLE_TYPES.map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
            </div>
            <button
              type="button"
              disabled={loading || !zoneId}
              className="bq-btn bq-btn-primary h-10 text-sm"
              onClick={() => void bulkAdd()}
            >
              Yaratish
            </button>
          </div>
        </section>
      </div>

      <section className="rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)]">
        <h2 className="mb-3 text-sm font-bold text-[color:var(--fg)]">
          Bitta stol qo'shish
        </h2>
        <div className="flex flex-wrap items-end gap-2">
          <div>
            <label className="mb-1 block text-[11px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
              Raqam
            </label>
            <input
              className="bq-input h-10 w-28"
              value={newNumber}
              onChange={(e) => setNewNumber(e.target.value)}
              placeholder="A-12"
            />
          </div>
          <div>
            <label className="mb-1 block text-[11px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
              O'rinlar
            </label>
            <input
              type="number"
              min={1}
              max={30}
              className="bq-input h-10 w-20"
              value={newSeats}
              onChange={(e) => setNewSeats(Number(e.target.value))}
            />
          </div>
          <div>
            <label className="mb-1 block text-[11px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
              Turi
            </label>
            <select
              className="bq-input h-10"
              value={newType}
              onChange={(e) => setNewType(e.target.value as (typeof TABLE_TYPES)[number])}
            >
              {TABLE_TYPES.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="mb-1 block text-[11px] font-bold uppercase tracking-wider text-[color:var(--muted)]">
              X, Y
            </label>
            <div className="flex gap-1">
              <input
                type="number"
                className="bq-input h-10 w-20"
                value={newX}
                onChange={(e) => setNewX(Number(e.target.value))}
              />
              <input
                type="number"
                className="bq-input h-10 w-20"
                value={newY}
                onChange={(e) => setNewY(Number(e.target.value))}
              />
            </div>
          </div>
          <button
            type="button"
            disabled={loading || !zoneId || !newNumber.trim()}
            className="bq-btn bq-btn-primary h-10 text-sm"
            onClick={() => void addTable()}
          >
            Qo'shish
          </button>
        </div>
      </section>

      {zoneId && zones.length ? (
        <section className="rounded-2xl border border-[color:var(--border)] bg-white p-4 shadow-[var(--shadow-sm)]">
          <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
            <h2 className="text-base font-bold text-[color:var(--fg)]">Zal sxemasi</h2>
            <div className="inline-flex gap-1 rounded-xl border border-[color:var(--border)] bg-[color:var(--bg-soft)] p-1 text-xs">
              <button
                type="button"
                onClick={() => setView("map")}
                className={`rounded-lg px-3 py-1.5 font-semibold transition ${
                  view === "map"
                    ? "bg-[color:var(--brand)] text-white shadow"
                    : "text-[color:var(--muted)] hover:bg-white"
                }`}
              >
                Xarita
              </button>
              <button
                type="button"
                onClick={() => setView("table")}
                className={`rounded-lg px-3 py-1.5 font-semibold transition ${
                  view === "table"
                    ? "bg-[color:var(--brand)] text-white shadow"
                    : "text-[color:var(--muted)] hover:bg-white"
                }`}
              >
                Jadval
              </button>
            </div>
          </div>

          {view === "map" && token ? (
            <AdminFloorPlanEditor
              zoneId={zoneId}
              zoneName={zones.find((z) => z.id === zoneId)?.name ?? "Zona"}
              token={token}
              tables={tables}
              floorConfig={zones.find((z) => z.id === zoneId)?.floorPlanSvg ?? null}
              onSaved={() => void loadTables()}
              selectedId={selectedTableId}
              onSelect={setSelectedTableId}
              onStatusChange={async (id, status) => {
                const row = tables.find((t) => t.id === id);
                if (!row) return;
                await saveRow({ ...row, status });
              }}
              onDuplicate={async (id) => {
                const row = tables.find((t) => t.id === id);
                if (!row || !token) return;
                try {
                  await apiFetch("/admin/tables", {
                    method: "POST",
                    token,
                    body: JSON.stringify({
                      zoneId: row.zoneId,
                      number: `${row.number}-copy`,
                      seats: row.seats,
                      type: row.type,
                      xPos: row.xPos + 40,
                      yPos: row.yPos + 40,
                      shape: row.shape ?? "rect",
                    }),
                  });
                  await loadTables();
                } catch (e) {
                  setErr((e as Error).message);
                }
              }}
              onDelete={(id) => void deleteRow(id)}
            />
          ) : null}

          {view === "map" && selectedTableId ? (
            <InlineTableEditor
              table={tables.find((t) => t.id === selectedTableId) ?? null}
              onChange={(patch) => patchRow(selectedTableId, patch)}
              onSave={() => {
                const row = tables.find((t) => t.id === selectedTableId);
                if (row) void saveRow(row);
              }}
              onDelete={() => {
                if (selectedTableId) {
                  void deleteRow(selectedTableId);
                  setSelectedTableId(null);
                }
              }}
            />
          ) : null}

          {view === "table" ? (
            <div className="overflow-x-auto rounded-xl border border-[color:var(--border)]">
              <table className="min-w-full text-left text-sm">
                <thead className="border-b border-[color:var(--border)] bg-[color:var(--bg-soft)] text-[11px] uppercase tracking-wider text-[color:var(--muted)]">
                  <tr>
                    <th className="px-3 py-2.5">Raqam</th>
                    <th className="px-3 py-2.5">O'rinlar</th>
                    <th className="px-3 py-2.5">Turi</th>
                    <th className="px-3 py-2.5">Holat</th>
                    <th className="px-3 py-2.5">X / Y</th>
                    <th className="px-3 py-2.5" />
                  </tr>
                </thead>
                <tbody>
                  {tables.map((t) => (
                    <tr key={t.id} className="border-b border-[color:var(--border)] last:border-b-0">
                      <td className="px-3 py-1.5">
                        <input
                          className="bq-input h-8 min-w-[72px]"
                          value={t.number}
                          onChange={(e) => patchRow(t.id, { number: e.target.value })}
                        />
                      </td>
                      <td className="px-3 py-1.5">
                        <input
                          type="number"
                          min={1}
                          max={30}
                          className="bq-input h-8 w-16"
                          value={t.seats}
                          onChange={(e) => patchRow(t.id, { seats: Number(e.target.value) })}
                        />
                      </td>
                      <td className="px-3 py-1.5">
                        <select
                          className="bq-input h-8"
                          value={t.type}
                          onChange={(e) => patchRow(t.id, { type: e.target.value })}
                        >
                          {TABLE_TYPES.map((x) => (
                            <option key={x} value={x}>{x}</option>
                          ))}
                        </select>
                      </td>
                      <td className="px-3 py-1.5">
                        <select
                          className="bq-input h-8"
                          value={t.status}
                          onChange={(e) => patchRow(t.id, { status: e.target.value })}
                        >
                          {TABLE_STATUSES.map((x) => (
                            <option key={x} value={x}>{x}</option>
                          ))}
                        </select>
                      </td>
                      <td className="px-3 py-1.5">
                        <div className="flex gap-1">
                          <input
                            type="number"
                            className="bq-input h-8 w-16"
                            value={t.xPos}
                            onChange={(e) => patchRow(t.id, { xPos: Number(e.target.value) })}
                          />
                          <input
                            type="number"
                            className="bq-input h-8 w-16"
                            value={t.yPos}
                            onChange={(e) => patchRow(t.id, { yPos: Number(e.target.value) })}
                          />
                        </div>
                      </td>
                      <td className="whitespace-nowrap px-3 py-1.5 text-right">
                        <button
                          type="button"
                          className="mr-2 text-xs font-bold text-[color:var(--brand)] hover:underline"
                          onClick={() => void saveRow(t)}
                        >
                          Saqlash
                        </button>
                        <button
                          type="button"
                          className="text-xs font-bold text-[color:var(--danger)] hover:underline"
                          onClick={() => void deleteRow(t.id)}
                        >
                          O'chirish
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {!tables.length && zoneId ? (
                <p className="p-4 text-center text-sm text-[color:var(--muted)]">
                  Bu zonada stollar yo'q.
                </p>
              ) : null}
            </div>
          ) : null}
        </section>
      ) : null}
    </div>
  );
}

function InlineTableEditor({
  table,
  onChange,
  onSave,
  onDelete,
}: {
  table: TableRow | null;
  onChange: (patch: Partial<TableRow>) => void;
  onSave: () => void;
  onDelete: () => void;
}) {
  if (!table) return null;
  return (
    <div className="mt-3 rounded-xl border-2 border-[color:var(--brand)]/40 bg-[color:var(--brand)]/5 p-4">
      <div className="mb-3 flex items-center justify-between">
        <div className="text-sm font-bold">
          Stol{" "}
          <span className="text-[color:var(--brand)]">{table.number}</span>{" "}
          parametrlari
        </div>
        <div className="flex gap-1">
          <button
            type="button"
            onClick={onSave}
            className="bq-btn bq-btn-primary h-8 text-xs"
          >
            Saqlash
          </button>
          <button
            type="button"
            onClick={onDelete}
            className="rounded-lg border border-[color:var(--danger)]/50 px-3 py-1 text-xs font-bold text-[color:var(--danger)] hover:bg-red-50"
          >
            O&apos;chirish
          </button>
        </div>
      </div>
      <div className="grid gap-2 sm:grid-cols-5">
        <label className="text-xs">
          <span className="text-[color:var(--muted)]">Raqam</span>
          <input
            className="bq-input mt-0.5 h-9"
            value={table.number}
            onChange={(e) => onChange({ number: e.target.value })}
          />
        </label>
        <label className="text-xs">
          <span className="text-[color:var(--muted)]">O&apos;rinlar</span>
          <input
            type="number"
            min={1}
            max={30}
            className="bq-input mt-0.5 h-9"
            value={table.seats}
            onChange={(e) => onChange({ seats: Number(e.target.value) })}
          />
        </label>
        <label className="text-xs">
          <span className="text-[color:var(--muted)]">Turi</span>
          <select
            className="bq-input mt-0.5 h-9"
            value={table.type}
            onChange={(e) => onChange({ type: e.target.value })}
          >
            {TABLE_TYPES.map((x) => (
              <option key={x} value={x}>{x}</option>
            ))}
          </select>
        </label>
        <label className="text-xs">
          <span className="text-[color:var(--muted)]">Holat</span>
          <select
            className="bq-input mt-0.5 h-9"
            value={table.status}
            onChange={(e) => onChange({ status: e.target.value })}
          >
            {TABLE_STATUSES.map((x) => (
              <option key={x} value={x}>{x}</option>
            ))}
          </select>
        </label>
        <div className="text-xs">
          <span className="text-[color:var(--muted)]">Pozitsiya (X, Y)</span>
          <div className="mt-0.5 rounded-lg border border-[color:var(--border)] bg-[color:var(--bg-soft)] px-3 py-2 text-sm font-semibold">
            {table.xPos} · {table.yPos}
          </div>
        </div>
      </div>
    </div>
  );
}
