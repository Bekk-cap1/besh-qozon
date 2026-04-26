"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { apiFetch } from "@/lib/api";
import { useAuth } from "@/context/AuthContext";
import { InteractiveFloorPlan, type PlanTable } from "@/components/InteractiveFloorPlan";
import { BranchCard } from "@/components/BranchCard";
import { MapLinkButton } from "@/components/MapLinkButton";
import { PhoneOtpStep } from "@/components/PhoneOtpStep";
import { ReservationTicket } from "@/components/ReservationTicket";
import { LoyaltyBonusToggle } from "@/components/LoyaltyCard";
import { PreorderMenu, cartToItems, type Cart } from "@/components/PreorderMenu";
import type { LoyaltyInfo } from "@/context/AuthContext";

type Branch = {
  id: string;
  name: string;
  address: string;
  workHours?: unknown;
  phone?: string | null;
  lat?: number | null;
  lng?: number | null;
  _count?: { zones: number };
  totalTables?: number;
  availableTables?: number;
};
type Zone = {
  id: string;
  name: string;
  type: string;
  floor: number | null;
  isSeasonal?: boolean;
  activeFrom?: string | null;
  activeTo?: string | null;
  floorPlanSvg?: string | null;
  _count?: { tables: number };
  availableTables?: number;
};
type TablesPayload = { tables: PlanTable[]; zoneId: string; date: string; time: string };

const STEP_TITLES = [
  "Filial",
  "Sana va vaqt",
  "Mehmonlar",
  "Zona",
  "Stol",
  "Kirish",
  "Tasdiqlash",
];

const WEEKDAYS_UZ = ["Yak", "Dush", "Sesh", "Chor", "Pay", "Jum", "Shan"];
const MONTHS_UZ = ["yan", "fev", "mar", "apr", "may", "iyn", "iyl", "avg", "sen", "okt", "noy", "dek"];

function addDays(d: Date, n: number) {
  const x = new Date(d);
  x.setDate(x.getDate() + n);
  return x;
}

function isoDate(d: Date) {
  return d.toISOString().slice(0, 10);
}

function formatDatePill(iso: string) {
  const d = new Date(`${iso}T00:00:00`);
  return {
    weekday: WEEKDAYS_UZ[d.getDay()],
    day: d.getDate(),
    month: MONTHS_UZ[d.getMonth()],
  };
}

export default function BronClient() {
  const auth = useAuth();
  const searchParams = useSearchParams();
  const [branches, setBranches] = useState<Branch[]>([]);
  const [loadErr, setLoadErr] = useState<string | null>(null);

  const initBranch = searchParams?.get("branch") ?? null;
  const initZone = searchParams?.get("zone") ?? null;
  const initGuestsRaw = searchParams?.get("guests");
  const initGuests = initGuestsRaw ? Math.min(20, Math.max(1, parseInt(initGuestsRaw, 10) || 2)) : 2;
  const initDate = searchParams?.get("date") ?? null;
  const initTime = searchParams?.get("time") ?? null;

  const [step, setStep] = useState(initBranch ? 2 : 1);
  const [branchId, setBranchId] = useState<string | null>(initBranch);
  const [date, setDate] = useState<string>(initDate ?? isoDate(new Date()));
  const [slots, setSlots] = useState<string[]>([]);
  const [time, setTime] = useState<string | null>(initTime);
  const [guests, setGuests] = useState(initGuests);
  const [zones, setZones] = useState<Zone[]>([]);
  const [zoneId, setZoneId] = useState<string | null>(initZone);
  const [tablesPayload, setTablesPayload] = useState<TablesPayload | null>(null);
  const [tableId, setTableId] = useState<string | null>(null);

  const [otpPhone, setOtpPhone] = useState("+998");
  const [otpCode, setOtpCode] = useState("");
  const [otpName, setOtpName] = useState("");
  const [otpHint, setOtpHint] = useState<string | null>(null);

  // календарная / слотовая загруженность
  const [dayLoad, setDayLoad] = useState<Record<string, { load: number; reservations: number }>>({});
  const [slotInfo, setSlotInfo] = useState<Record<string, { freeTables: number; totalTables: number }>>({});
  const [payMethod, setPayMethod] = useState<"demo" | "payme" | "click" | "uzum">("demo");
  const [loyalty, setLoyalty] = useState<LoyaltyInfo | null>(null);
  const [useBonus, setUseBonus] = useState(false);
  const [preorderCart, setPreorderCart] = useState<Cart>({});

  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [reservation, setReservation] = useState<{
    id: string;
    depositAmount: string;
    status?: string;
    startAt?: string;
  } | null>(null);
  const [checkout, setCheckout] = useState<{
    paymentId: string;
    checkoutUrl: string | null;
    mockFallback?: boolean;
    provider: string;
  } | null>(null);

  const dates = useMemo(() => {
    const today = new Date();
    return Array.from({ length: 15 }, (_, i) => isoDate(addDays(today, i)));
  }, []);

  const reloadBranches = useCallback(() => {
    setLoadErr(null);
    apiFetch<Branch[]>("/branches")
      .then(setBranches)
      .catch((e: Error) => setLoadErr(e.message));
  }, []);

  useEffect(() => {
    reloadBranches();
  }, [reloadBranches]);

  useEffect(() => {
    if (!auth.token) {
      setLoyalty(null);
      setUseBonus(false);
      return;
    }
    apiFetch<{ loyalty?: LoyaltyInfo }>("/user/profile", { token: auth.token })
      .then((p) => setLoyalty(p?.loyalty ?? null))
      .catch(() => setLoyalty(null));
  }, [auth.token]);

  const loadSlots = useCallback(async () => {
    if (!branchId || !date) return;
    setBusy(true);
    setErr(null);
    try {
      const r = await apiFetch<{
        slots: { slot: string; freeTables: number; totalTables: number }[];
      }>(`/branches/${branchId}/slot-load?date=${date}&guests=${guests}`);

      // Bugungi sana uchun o'tgan vaqt slotlarini filtrlash
      const now = new Date();
      const todayIso = isoDate(now);
      const nowMinutes = now.getHours() * 60 + now.getMinutes();
      const filtered = date === todayIso
        ? r.slots.filter((s) => {
            const [h, m] = s.slot.split(":").map(Number);
            return h * 60 + m > nowMinutes;
          })
        : r.slots;

      const labels = filtered.map((s) => s.slot);
      setSlots(labels);
      const info: Record<string, { freeTables: number; totalTables: number }> = {};
      for (const s of filtered) info[s.slot] = { freeTables: s.freeTables, totalTables: s.totalTables };
      setSlotInfo(info);
      setTime((prev) => {
        if (prev && labels.includes(prev) && (info[prev]?.freeTables ?? 0) > 0) return prev;
        const firstFree = filtered.find((s) => s.freeTables > 0)?.slot;
        return firstFree ?? labels[0] ?? null;
      });
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  }, [branchId, date, guests]);

  useEffect(() => {
    if (branchId) void loadSlots();
  }, [branchId, date, guests, loadSlots]);

  // Загруженность по дням для календарных точек
  const loadDayLoad = useCallback(async () => {
    if (!branchId) return;
    try {
      const r = await apiFetch<{
        days: { date: string; load: number; reservations: number }[];
      }>(`/branches/${branchId}/day-load?days=15`);
      const map: Record<string, { load: number; reservations: number }> = {};
      for (const d of r.days) map[d.date] = { load: d.load, reservations: d.reservations };
      setDayLoad(map);
    } catch {
      /* soft-fail — если endpoint недоступен, точки просто не отображаются */
    }
  }, [branchId]);

  useEffect(() => {
    if (branchId) void loadDayLoad();
  }, [branchId, loadDayLoad]);

  const loadZones = useCallback(async () => {
    if (!branchId) return;
    setBusy(true);
    try {
      const z = await apiFetch<Zone[]>(`/branches/${branchId}/zones`);
      setZones(z);
      setZoneId((prev) => (prev && z.some((x) => x.id === prev) ? prev : z[0]?.id ?? null));
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  }, [branchId]);

  useEffect(() => {
    if (branchId && time) void loadZones();
  }, [branchId, time, loadZones]);

  const loadTables = useCallback(async () => {
    if (!zoneId || !date || !time) return;
    setBusy(true);
    try {
      const r = await apiFetch<TablesPayload>(`/zones/${zoneId}/tables?date=${date}&time=${encodeURIComponent(time)}`);
      setTablesPayload(r);
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  }, [zoneId, date, time]);

  useEffect(() => {
    if (zoneId && date && time) void loadTables();
  }, [zoneId, date, time, loadTables]);

  const startAtIso = useMemo(() => {
    if (!date || !time) return "";
    const d = new Date(`${date}T${time}:00`);
    return d.toISOString();
  }, [date, time]);

  const selectedBranch = useMemo(() => branches.find((b) => b.id === branchId) ?? null, [branches, branchId]);
  const selectedZone = useMemo(() => zones.find((z) => z.id === zoneId) ?? null, [zones, zoneId]);
  const selectedTable = useMemo(
    () => tablesPayload?.tables.find((t) => t.id === tableId) ?? null,
    [tablesPayload, tableId],
  );

  const canAdvance = (s: number) => {
    switch (s) {
      case 1:
        return Boolean(branchId);
      case 2:
        return Boolean(date && time);
      case 3:
        return guests >= 1 && guests <= 20;
      case 4:
        return Boolean(zoneId);
      case 5:
        return Boolean(tableId);
      case 6:
        return Boolean(auth.user);
      default:
        return true;
    }
  };

  const submitReservation = async () => {
    if (!auth.token || !branchId || !tableId || !startAtIso) {
      setErr("Barcha maydonlar to‘ldirilmagan");
      return;
    }
    setBusy(true);
    setErr(null);
    setMsg(null);
    try {
      const r = await apiFetch<{ id: string; depositAmount: unknown; status?: string; startAt?: string }>("/reservations", {
        method: "POST",
        token: auth.token,
        body: JSON.stringify({
          branchId,
          tableId,
          startAt: startAtIso,
          guestsCount: guests,
          useBonus: useBonus && (loyalty?.bonuses ?? 0) > 0 ? true : undefined,
          items: cartToItems(preorderCart).length > 0 ? cartToItems(preorderCart) : undefined,
        }),
      });
      setReservation({
        id: r.id,
        depositAmount: String(r.depositAmount),
        status: r.status,
        startAt: r.startAt,
      });
      setCheckout(null);
      if (r.status === "CONFIRMED") {
        setMsg("Bron yaratildi va bonus bilan tasdiqlandi!");
      } else if (payMethod === "demo" && !(useBonus && (loyalty?.bonuses ?? 0) > 0)) {
        // Демо-оплата: сразу проводим mock-платёж после создания брони
        try {
          const paid = await apiFetch<{
            id: string;
            status: string;
            depositAmount: unknown;
            startAt: string;
          }>(`/reservations/${r.id}/pay`, { method: "POST", token: auth.token });
          setReservation({
            id: paid.id,
            depositAmount: String(paid.depositAmount),
            status: paid.status,
            startAt: paid.startAt,
          });
          setMsg("Demo to'lov muvaffaqiyatli. Bron tasdiqlandi!");
        } catch (payErr) {
          setMsg("Bron yaratildi, ammo demo to'lov o'tmadi. Qayta urinib ko'ring.");
          setErr((payErr as Error).message);
        }
      } else {
        setMsg("Bron yaratildi. 10 daqiqa ichida depozitni to'lang.");
      }
    } catch (e) {
      setErr((e as Error).message || "Bron yaratishda xatolik");
    } finally {
      setBusy(false);
    }
  };

  const pay = async () => {
    if (!auth.token || !reservation) return;
    setBusy(true);
    setErr(null);
    try {
      const r = await apiFetch<{
        id: string;
        status: string;
        depositAmount: unknown;
        startAt: string;
      }>(`/reservations/${reservation.id}/pay`, { method: "POST", token: auth.token });
      setReservation({
        id: r.id,
        depositAmount: String(r.depositAmount),
        status: r.status,
        startAt: r.startAt,
      });
      setMsg("To'lov muvaffaqiyatli. Bron tasdiqlandi!");
      setCheckout(null);
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  // После запуска внешнего checkout — пуллим статус резервации, пока не CONFIRMED
  useEffect(() => {
    if (!auth.token || !reservation || reservation.status === "CONFIRMED") return;
    if (!checkout?.checkoutUrl) return; // пуллим только если ушли на внешнего провайдера

    let stopped = false;
    const tick = async () => {
      try {
        const r = await apiFetch<{
          id: string;
          status: string;
          depositAmount: unknown;
          startAt: string;
        }>(`/user/reservations/${reservation.id}`, { token: auth.token });
        if (stopped) return;
        if (r.status === "CONFIRMED") {
          setReservation({
            id: r.id,
            depositAmount: String(r.depositAmount),
            status: r.status,
            startAt: r.startAt,
          });
          setMsg("To'lov muvaffaqiyatli. Bron tasdiqlandi!");
        }
      } catch {
        /* ignore */
      }
    };
    const id = setInterval(tick, 4000);
    return () => {
      stopped = true;
      clearInterval(id);
    };
  }, [auth.token, reservation, checkout?.checkoutUrl]);

  const startCheckout = async (provider: "payme" | "click") => {
    if (!auth.token || !reservation) return;
    setBusy(true);
    setErr(null);
    try {
      const r = await apiFetch<{
        paymentId: string;
        checkoutUrl: string | null;
        mockFallback?: boolean;
        provider: string;
        message?: string;
      }>(`/reservations/${reservation.id}/checkout`, {
        method: "POST",
        token: auth.token,
        body: JSON.stringify({ provider }),
      });
      setCheckout({
        paymentId: r.paymentId,
        checkoutUrl: r.checkoutUrl,
        mockFallback: r.mockFallback,
        provider: r.provider,
      });
      setMsg(r.checkoutUrl ? "To‘lov sahifasi tayyor — yangi oynada oching." : r.message ?? "Checkout sozlanmagan — mock to‘lovni ishlating.");
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const skipAutoAdvanceRef = useRef(false);
  const goNext = () => {
    if (!canAdvance(step)) return;
    setErr(null);
    setStep((s) => Math.min(7, s + 1));
  };
  const goBack = () => {
    setErr(null);
    skipAutoAdvanceRef.current = true;
    setStep((s) => Math.max(1, s - 1));
  };

  useEffect(() => {
    if (skipAutoAdvanceRef.current) {
      skipAutoAdvanceRef.current = false;
      return;
    }
    if (step === 6 && auth.user) setStep(7);
  }, [step, auth.user]);

  return (
    <div className="bg-gradient-to-b from-[color:var(--brand-50)] via-white to-[color:var(--bg-soft)] text-[color:var(--fg)]">
      <div className="mx-auto max-w-3xl space-y-6 px-4 py-8">
        <div className="flex items-end justify-between gap-4">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight sm:text-3xl">Stolni bron qilish</h1>
            <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
              Qadam {step} / 7 — {STEP_TITLES[step - 1]}
            </p>
          </div>
          <div className="hidden text-right text-xs text-zinc-500 sm:block">
            {selectedBranch ? <div>📍 {selectedBranch.name}</div> : null}
            {date && time ? <div>🕒 {date} · {time}</div> : null}
            {selectedTable ? <div>🪑 {selectedTable.number}</div> : null}
          </div>
        </div>

        <Stepper step={step} onJump={(s) => s < step && setStep(s)} />

        {loadErr ? (
          <p className="rounded-lg border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-900 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-100">
            {loadErr}
          </p>
        ) : null}
        {err ? (
          <p className="rounded-lg border border-red-300 bg-red-50 px-3 py-2 text-sm text-red-900 dark:border-red-800 dark:bg-red-950/40 dark:text-red-100">
            {err}
          </p>
        ) : null}
        {msg ? (
          <p className="rounded-lg border border-emerald-300 bg-emerald-50 px-3 py-2 text-sm text-emerald-900 dark:border-emerald-800 dark:bg-emerald-950/40 dark:text-emerald-100">
            {msg}
          </p>
        ) : null}

        <StepCard title={`${step}. ${STEP_TITLES[step - 1]}`} subtitle={subtitleFor(step)}>
          {step === 1 ? (
            <div className="space-y-4">
              {branches.length === 0 ? (
                <div className="rounded-xl border border-[color:var(--border)] bg-[color:var(--bg-soft)] p-6 text-sm text-[color:var(--muted)]">
                  Yuklanmoqda yoki filiallar yo&apos;q…
                </div>
              ) : (
                <div className="grid gap-4 sm:grid-cols-2">
                  {branches.map((b, i) => (
                    <BranchCard
                      key={b.id}
                      branch={b}
                      index={i}
                      selected={branchId === b.id}
                      variant="select"
                      onClick={() => {
                        setBranchId(b.id);
                        setReservation(null);
                        setTableId(null);
                        setErr(null);
                        // Авто-переход к шагу 2 сразу после выбора филиала
                        setStep((s) => (s === 1 ? 2 : s));
                      }}
                    />
                  ))}
                </div>
              )}
              <p className="text-xs text-[color:var(--muted)]">
                Filialni tanlang — keyingi bosqichga avtomatik o&apos;tiladi.
              </p>
            </div>
          ) : null}

          {step === 2 ? (
            <div className="space-y-5">
              <div>
                <div className="mb-2 flex items-center justify-between">
                  <div className="text-xs font-semibold uppercase tracking-wide text-[color:var(--muted)]">
                    Sana
                  </div>
                  <DayLoadLegend />
                </div>
                <div className="flex gap-2 overflow-x-auto pb-1">
                  {dates.map((d) => {
                    const p = formatDatePill(d);
                    const active = date === d;
                    const load = dayLoad[d]?.load;
                    return (
                      <button
                        key={d}
                        type="button"
                        onClick={() => setDate(d)}
                        title={
                          load !== undefined
                            ? `Bandlik: ${Math.round(load * 100)}%`
                            : undefined
                        }
                        className={`relative flex min-w-[68px] flex-col items-center rounded-xl border px-3 py-2 text-center transition ${
                          active
                            ? "border-[color:var(--brand)] bg-[color:var(--brand)] text-white shadow-[var(--shadow-md)]"
                            : "border-[color:var(--border)] bg-white text-[color:var(--fg)] hover:-translate-y-0.5 hover:border-[color:var(--brand)]"
                        }`}
                      >
                        <span className="text-[10px] uppercase opacity-80">{p.weekday}</span>
                        <span className="text-lg font-bold leading-none">{p.day}</span>
                        <span className="text-[10px] opacity-80">{p.month}</span>
                        {load !== undefined ? (
                          <span
                            aria-hidden
                            className="mt-1.5 inline-block h-1.5 w-1.5 rounded-full"
                            style={{ background: loadColor(load) }}
                          />
                        ) : null}
                      </button>
                    );
                  })}
                </div>
              </div>

              <div>
                <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
                  <div className="text-xs font-semibold uppercase tracking-wide text-[color:var(--muted)]">
                    Vaqt · 30 daqiqa oralig&apos;i · {guests} mehmon
                  </div>
                  {time ? (
                    <div className="text-sm font-semibold text-[color:var(--brand-700)]">
                      Tanlangan: {time}
                      {slotInfo[time]?.freeTables !== undefined ? (
                        <span className="ml-1 text-[color:var(--muted)]">
                          · {slotInfo[time].freeTables} ta stol bor
                        </span>
                      ) : null}
                    </div>
                  ) : null}
                </div>
                {busy && !slots.length ? (
                  <p className="text-sm text-[color:var(--muted)]">Yuklanmoqda…</p>
                ) : slots.length === 0 ? (
                  <p className="rounded-xl border border-[color:var(--border)] bg-[color:var(--bg-soft)] px-3 py-4 text-sm text-[color:var(--muted)]">
                    Bu sanada bo&apos;sh vaqt yo&apos;q. Boshqa sanani tanlang.
                  </p>
                ) : (
                  <div className="grid max-h-72 grid-cols-3 gap-2 overflow-y-auto pr-1 sm:grid-cols-5">
                    {slots.map((s) => {
                      const info = slotInfo[s];
                      const free = info?.freeTables ?? null;
                      const disabled = free !== null && free <= 0;
                      const isSel = time === s;
                      return (
                        <button
                          key={s}
                          type="button"
                          disabled={disabled}
                          onClick={() => setTime(s)}
                          className={`flex flex-col items-center rounded-xl border px-2 py-2 text-sm font-semibold transition ${
                            isSel
                              ? "border-[color:var(--brand)] bg-[color:var(--brand)] text-white shadow-[var(--shadow-sm)]"
                              : disabled
                                ? "cursor-not-allowed border-[color:var(--border)] bg-[color:var(--bg-soft)] text-[color:var(--muted-2)]"
                                : "border-[color:var(--border)] bg-white text-[color:var(--fg)] hover:-translate-y-0.5 hover:border-[color:var(--brand)]"
                          }`}
                        >
                          <span className="text-[15px]">{s}</span>
                          {free !== null ? (
                            <span
                              className={`mt-0.5 text-[10px] font-medium ${
                                isSel
                                  ? "text-white/85"
                                  : disabled
                                    ? "text-[color:var(--muted-2)]"
                                    : free < 3
                                      ? "text-[#b45309]"
                                      : "text-[color:var(--brand-700)]"
                              }`}
                            >
                              {disabled ? "To'lgan" : `${free} ta stol`}
                            </span>
                          ) : null}
                        </button>
                      );
                    })}
                  </div>
                )}
                <p className="mt-2 text-[11px] text-[color:var(--muted)]">
                  Stollar soni <b>{guests}</b> kishilik va undan katta stollarni hisobga olgan holda ko&apos;rsatilmoqda.
                </p>
              </div>
            </div>
          ) : null}

          {step === 3 ? (
            <div className="space-y-5">
              <p className="text-sm text-[color:var(--muted)]">
                Necha kishiga stol kerak? Biz sizga eng mos stollarni topib beramiz.
              </p>

              {/* Визуальная группа гостей */}
              <GuestAvatars count={guests} />

              <div className="flex items-center gap-3">
                <button
                  type="button"
                  onClick={() => setGuests((g) => Math.max(1, g - 1))}
                  disabled={guests <= 1}
                  className="h-12 w-12 rounded-full border border-[color:var(--border)] bg-white text-2xl font-bold text-[color:var(--fg)] transition hover:border-[color:var(--brand)] hover:text-[color:var(--brand)] disabled:cursor-not-allowed disabled:opacity-40"
                  aria-label="Kamaytirish"
                >
                  −
                </button>
                <div className="flex h-12 min-w-[6rem] items-center justify-center rounded-xl border border-[color:var(--border)] bg-[color:var(--bg-soft)] text-2xl font-extrabold text-[color:var(--fg)] shadow-inner">
                  {guests}
                </div>
                <button
                  type="button"
                  onClick={() => setGuests((g) => Math.min(20, g + 1))}
                  disabled={guests >= 20}
                  className="h-12 w-12 rounded-full bg-[color:var(--brand)] text-2xl font-bold text-white shadow-[var(--shadow-sm)] transition hover:bg-[color:var(--brand-600)] disabled:cursor-not-allowed disabled:bg-[color:var(--muted-2)]"
                  aria-label="Ko‘paytirish"
                >
                  +
                </button>
                <span className="text-sm font-medium text-[color:var(--muted)]">kishilik</span>
              </div>

              <div>
                <div className="mb-1.5 text-[11px] font-semibold uppercase tracking-wider text-[color:var(--muted)]">
                  Tez tanlash
                </div>
                <div className="flex flex-wrap gap-2">
                  {[2, 4, 6, 8, 10, 12].map((n) => (
                    <button
                      key={n}
                      type="button"
                      onClick={() => setGuests(n)}
                      className={`rounded-full border px-4 py-1.5 text-sm font-semibold transition ${
                        guests === n
                          ? "border-[color:var(--brand)] bg-[color:var(--brand)] text-white shadow-sm"
                          : "border-[color:var(--border)] bg-white text-[color:var(--fg)] hover:border-[color:var(--brand)] hover:text-[color:var(--brand-700)]"
                      }`}
                    >
                      {n}
                    </button>
                  ))}
                </div>
              </div>

              {/* Подсказка «N variant bor» */}
              <GuestHint
                guests={guests}
                totalTables={slotInfo[time ?? ""]?.totalTables ?? null}
              />
            </div>
          ) : null}

          {step === 4 ? (
            <div className="space-y-3">
              {zones.length === 0 ? (
                <p className="rounded-xl border border-[color:var(--border)] bg-[color:var(--bg-soft)] p-4 text-sm text-[color:var(--muted)]">
                  Yuklanmoqda yoki zonalar yo&apos;q.
                </p>
              ) : (
                <div className="grid gap-3 sm:grid-cols-2">
                  {zones.map((z) => {
                    const active = zoneId === z.id;
                    const seasonalClosed = isZoneClosed(z);
                    return (
                      <ZoneCard
                        key={z.id}
                        zone={z}
                        active={active}
                        closed={seasonalClosed}
                        onClick={() => {
                          if (seasonalClosed) return;
                          setZoneId(z.id);
                          setTableId(null);
                        }}
                      />
                    );
                  })}
                </div>
              )}
            </div>
          ) : null}

          {step === 5 ? (
            <div className="space-y-3">
              {tablesPayload && zoneId ? (
                <InteractiveFloorPlan
                  zoneId={zoneId}
                  zoneType={selectedZone?.type}
                  tables={tablesPayload.tables}
                  selectedId={tableId}
                  floorConfig={selectedZone?.floorPlanSvg ?? null}
                  onSelect={(id) => {
                    setTableId(id);
                    setReservation(null);
                  }}
                  onRefresh={loadTables}
                />
              ) : (
                <p className="rounded-xl border border-[color:var(--border)] bg-[color:var(--bg-soft)] p-4 text-sm text-[color:var(--muted)]">
                  Stollar yuklanmoqda…
                </p>
              )}
              {selectedTable ? (
                <div className="flex items-center gap-2 rounded-xl border border-[color:var(--brand-100)] bg-[color:var(--brand-50)] p-3 text-sm text-[color:var(--brand-700)]">
                  <span className="inline-flex h-6 w-6 items-center justify-center rounded-full bg-[color:var(--brand)] text-white">
                    ✓
                  </span>
                  <span>
                    Tanlandi: <b>T-{selectedTable.number}</b> · {selectedTable.seats} kishilik ·{" "}
                    {selectedTable.type === "VIP"
                      ? "VIP"
                      : selectedTable.type === "FAMILY"
                        ? "Oilaviy"
                        : "Standart"}
                  </span>
                </div>
              ) : null}
            </div>
          ) : null}

          {step === 6 ? (
            <PhoneOtpStep
              phone={otpPhone}
              onPhoneChange={setOtpPhone}
              code={otpCode}
              onCodeChange={setOtpCode}
              name={otpName}
              onNameChange={setOtpName}
              onRequestOtp={async (phone) => {
                setBusy(true);
                setErr(null);
                try {
                  const r = await auth.requestOtp(phone);
                  setOtpHint(r.devCode ? `Dev kod: ${r.devCode}` : "SMS yuborildi.");
                } catch (e) {
                  setErr((e as Error).message);
                  throw e;
                } finally {
                  setBusy(false);
                }
              }}
              onVerifyOtp={async (phone, code, nm) => {
                setBusy(true);
                setErr(null);
                try {
                  await auth.verifyOtp(phone, code, nm);
                  setOtpHint(null);
                  setMsg("Tizimga kirdingiz.");
                } catch (e) {
                  setErr((e as Error).message);
                } finally {
                  setBusy(false);
                }
              }}
              busy={busy}
              hint={otpHint}
              authenticated={Boolean(auth.user)}
              authPhone={auth.user?.phone ?? null}
            />
          ) : null}

          {step === 7 ? (
            <div className="space-y-5">
              {!reservation ? (
                <>
                  {/* Сводка брони */}
                  <div className="grid gap-4 md:grid-cols-[1fr_auto]">
                    <div className="rounded-2xl border border-[color:var(--border)] bg-white p-4">
                      <div className="mb-3 flex items-center justify-between">
                        <div className="text-xs font-semibold uppercase tracking-wider text-[color:var(--muted)]">
                          Sizning broningiz
                        </div>
                        {selectedBranch ? (
                          <MapLinkButton
                            name={selectedBranch.name}
                            address={selectedBranch.address}
                            lat={selectedBranch.lat}
                            lng={selectedBranch.lng}
                            variant="pill"
                          />
                        ) : null}
                      </div>
                      <ul className="space-y-2.5 text-sm">
                        <SummaryRow k="Filial" v={selectedBranch?.name ?? "—"} />
                        <SummaryRow k="Manzil" v={selectedBranch?.address ?? "—"} />
                        <SummaryRow k="Sana va vaqt" v={date && time ? `${date} · ${time}` : "—"} />
                        <SummaryRow k="Mehmonlar" v={`${guests} kishi`} />
                        <SummaryRow k="Zona" v={selectedZone?.name ?? "—"} />
                        <SummaryRow
                          k="Stol"
                          v={selectedTable ? `T-${selectedTable.number} · ${selectedTable.seats} kishilik` : "—"}
                        />
                        <SummaryRow k="Telefon" v={auth.user?.phone ?? "—"} />
                      </ul>
                    </div>

                    {/* Мини-план с выбранным столом */}
                    <div className="hidden md:block">
                      <MiniSelectedTable
                        tables={tablesPayload?.tables ?? []}
                        selectedId={tableId}
                        zoneType={selectedZone?.type}
                      />
                    </div>
                  </div>

                  {/* Предзаказ еды */}
                  <PreorderMenu
                    branchId={branchId}
                    cart={preorderCart}
                    onChange={setPreorderCart}
                  />

                  {/* Депозит */}
                  <div className="flex items-center justify-between gap-3 rounded-2xl border border-[color:var(--brand-100)] bg-[color:var(--brand-50)] p-4">
                    <div>
                      <div className="text-xs font-semibold uppercase tracking-wider text-[color:var(--brand-700)]">
                        Depozit
                      </div>
                      <div className="text-[22px] font-extrabold text-[color:var(--brand-700)]">
                        {useBonus && (loyalty?.bonuses ?? 0) > 0 ? (
                          <>
                            <span className="text-[color:var(--muted)] line-through">50 000</span>{" "}
                            <span>0 so&apos;m</span>
                          </>
                        ) : (
                          <>50 000 so&apos;m</>
                        )}
                      </div>
                      <div className="mt-0.5 text-xs text-[color:var(--muted)]">
                        {useBonus && (loyalty?.bonuses ?? 0) > 0
                          ? "Bonus qo'llanildi — to'lov shart emas."
                          : "Tashrifdan so'ng cheq orqali qaytariladi."}
                      </div>
                    </div>
                    <svg width="48" height="48" viewBox="0 0 48 48" fill="none" aria-hidden>
                      <rect x="4" y="10" width="40" height="28" rx="4" fill="#1b7a4e" />
                      <rect x="4" y="16" width="40" height="5" fill="#0f4e32" />
                      <rect x="8" y="28" width="14" height="4" rx="1" fill="#dcfce7" />
                    </svg>
                  </div>

                  {loyalty && loyalty.bonuses > 0 ? (
                    <LoyaltyBonusToggle
                      loyalty={loyalty}
                      active={useBonus}
                      onToggle={setUseBonus}
                      disabled={busy}
                    />
                  ) : null}

                  {/* Выбор метода оплаты — скрыт когда применён бонус */}
                  {!(useBonus && (loyalty?.bonuses ?? 0) > 0) ? (
                    <div>
                      <div className="mb-2 text-xs font-semibold uppercase tracking-wider text-[color:var(--muted)]">
                        To&apos;lov usuli
                      </div>
                      <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
                        <PayMethodCard
                          id="demo"
                          label="Demo"
                          sub="Test rejimi"
                          color="#1B7A4E"
                          selected={payMethod === "demo"}
                          onSelect={() => setPayMethod("demo")}
                        />
                        <PayMethodCard
                          id="payme"
                          label="Payme"
                          sub="Payme.uz"
                          color="#00cccc"
                          selected={payMethod === "payme"}
                          onSelect={() => setPayMethod("payme")}
                        />
                        <PayMethodCard
                          id="click"
                          label="Click"
                          sub="Click Evolution"
                          color="#22c1f3"
                          selected={payMethod === "click"}
                          onSelect={() => setPayMethod("click")}
                        />
                        <PayMethodCard
                          id="uzum"
                          label="Uzum"
                          sub="Tez orada"
                          color="#7c3aed"
                          selected={payMethod === "uzum"}
                          onSelect={() => setPayMethod("uzum")}
                          disabled
                        />
                      </div>
                      {payMethod === "demo" ? (
                        <p className="mt-2 rounded-xl border border-[color:var(--brand-100)] bg-[color:var(--brand-50)] px-3 py-2 text-[11px] text-[color:var(--brand-700)]">
                          Demo rejim — to&apos;lov yaratilgach avtomatik tasdiqlanadi. Haqiqiy pul yechilmaydi.
                        </p>
                      ) : null}
                    </div>
                  ) : null}

                  <p className="rounded-xl border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900">
                    15 daqiqadan ortiq kechikishda depozit qaytarilmaydi.
                  </p>

                  <button
                    type="button"
                    onClick={() => void submitReservation()}
                    disabled={
                      busy ||
                      !auth.user ||
                      !tableId ||
                      (!(useBonus && (loyalty?.bonuses ?? 0) > 0) && payMethod === "uzum")
                    }
                    className="h-14 w-full rounded-2xl bg-[color:var(--brand)] text-base font-extrabold text-white shadow-[0_10px_25px_rgba(27,122,78,0.25)] transition hover:bg-[color:var(--brand-600)] disabled:cursor-not-allowed disabled:bg-[color:var(--muted-2)] disabled:shadow-none"
                  >
                    {busy
                      ? "Yuborilmoqda…"
                      : useBonus && (loyalty?.bonuses ?? 0) > 0
                        ? "Bonus bilan tasdiqlash · 0 so'm"
                        : payMethod === "uzum"
                          ? "Uzum tez orada qo'shiladi"
                          : payMethod === "demo"
                            ? "Demo to'lov · 50 000 so'm (test)"
                            : "To'lash va tasdiqlash · 50 000 so'm"}
                  </button>
                  {!auth.user ? (
                    <p className="text-xs text-[color:var(--muted)]">
                      Oldin 6-qadamda telefon raqamingizni tasdiqlang.
                    </p>
                  ) : !tableId ? (
                    <p className="text-xs text-[color:var(--muted)]">
                      Stolni 5-qadamda tanlang.
                    </p>
                  ) : null}
                </>
              ) : reservation.status === "CONFIRMED" ? (
                <div className="space-y-4">
                  <ReservationTicket
                    reservationId={reservation.id}
                    branchName={selectedBranch?.name}
                    address={selectedBranch?.address}
                    tableNumber={selectedTable?.number?.toString() ?? null}
                    startAt={reservation.startAt ?? (date && time ? `${date}T${time}:00` : null)}
                    guests={guests}
                    userName={auth.user?.name ?? null}
                    userPhone={auth.user?.phone ?? null}
                  />
                  <div className="flex flex-wrap gap-2">
                    <Link
                      href="/mening-bronlarim"
                      className="bq-btn bq-btn-secondary h-11 flex-1 text-sm"
                    >
                      Mening bronlarim
                    </Link>
                    <Link
                      href="/"
                      className="bq-btn bq-btn-primary h-11 flex-1 text-sm"
                    >
                      Bosh sahifaga
                    </Link>
                  </div>
                </div>
              ) : (
                <div className="space-y-4">
                  <div className="flex items-start justify-between gap-3 rounded-2xl border border-[color:var(--brand-100)] bg-[color:var(--brand-50)] p-4">
                    <div className="flex items-start gap-3">
                      <span className="inline-flex h-10 w-10 items-center justify-center rounded-full bg-[color:var(--brand)] text-white">
                        ✓
                      </span>
                      <div>
                        <div className="text-lg font-extrabold text-[color:var(--brand-700)]">
                          Bron yaratildi!
                        </div>
                        <div className="mt-0.5 text-xs text-[color:var(--muted)]">
                          ID: <code className="font-mono">{reservation.id.slice(0, 8)}…</code>
                        </div>
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="text-[10px] uppercase text-[color:var(--muted)]">Depozit</div>
                      <div className="text-xl font-extrabold text-[color:var(--brand-700)]">
                        {Number(reservation.depositAmount).toLocaleString("ru-RU")} so&apos;m
                      </div>
                    </div>
                  </div>

                  <p className="text-xs text-[color:var(--muted)]">
                    10 daqiqa ichida to&apos;lang, aks holda bron bekor qilinadi.
                  </p>

                  <button
                    type="button"
                    onClick={() => void pay()}
                    disabled={busy}
                    className="h-12 w-full rounded-xl bg-[color:var(--brand)] font-extrabold text-white shadow-[0_8px_20px_rgba(27,122,78,0.25)] transition hover:bg-[color:var(--brand-600)] disabled:opacity-50"
                  >
                    {busy ? "…" : "Demo to'lov · tasdiqlash"}
                  </button>
                  <div className="grid gap-2 sm:grid-cols-2">
                    <button
                      type="button"
                      onClick={() => void startCheckout("payme")}
                      disabled={busy}
                      className="h-11 rounded-xl bg-[#00cccc] font-semibold text-white shadow-sm transition hover:opacity-90 disabled:opacity-50"
                    >
                      Payme (real)
                    </button>
                    <button
                      type="button"
                      onClick={() => void startCheckout("click")}
                      disabled={busy}
                      className="h-11 rounded-xl bg-[#22c1f3] font-semibold text-white shadow-sm transition hover:opacity-90 disabled:opacity-50"
                    >
                      Click (real)
                    </button>
                  </div>
                  {checkout?.checkoutUrl ? (
                    <a
                      href={checkout.checkoutUrl}
                      target="_blank"
                      rel="noreferrer"
                      className="block rounded-xl bg-white px-3 py-2 text-center text-sm font-semibold text-[color:var(--brand-700)] shadow-sm hover:bg-[color:var(--brand-50)]"
                    >
                      {checkout.provider.toUpperCase()} — to&apos;lov sahifasini ochish ↗
                    </a>
                  ) : null}
                  {checkout?.mockFallback ? (
                    <p className="text-[11px] text-[color:var(--muted)]">
                      API kalitlari yo&apos;q — mock yoki .env da PAYME_/CLICK_ sozlang.
                    </p>
                  ) : null}
                </div>
              )}
            </div>
          ) : null}
        </StepCard>

        {step < 7 || !reservation ? (
          <div className="flex items-center justify-between gap-3">
            <button
              type="button"
              onClick={goBack}
              disabled={step === 1 || busy}
              className="h-11 rounded-xl border border-zinc-300 bg-white px-4 text-sm font-medium text-zinc-700 shadow-sm hover:bg-zinc-50 disabled:opacity-40 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-200"
            >
              ← Orqaga
            </button>
            {step < 7 ? (
              <button
                type="button"
                onClick={goNext}
                disabled={!canAdvance(step) || busy}
                className="h-11 flex-1 rounded-xl bg-emerald-700 px-4 text-sm font-semibold text-white shadow hover:bg-emerald-800 disabled:cursor-not-allowed disabled:bg-zinc-300 disabled:text-zinc-600 dark:disabled:bg-zinc-800 dark:disabled:text-zinc-500 sm:max-w-xs"
              >
                Davom etish →
              </button>
            ) : null}
          </div>
        ) : null}

        {auth.user ? (
          <details className="rounded-2xl border border-zinc-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
            <summary className="cursor-pointer text-sm font-medium">Mening bronlarim</summary>
            <div className="mt-3">
              <MyReservations token={auth.token} />
            </div>
          </details>
        ) : null}
      </div>
    </div>
  );
}

function subtitleFor(step: number): string {
  switch (step) {
    case 1:
      return "Qaysi filialda stol band qilmoqchisiz?";
    case 2:
      return "Sana va vaqtni tanlang.";
    case 3:
      return "Mehmonlar sonini kiriting.";
    case 4:
      return "Ichki zalmi yoki ko‘chami?";
    case 5:
      return "Kartadan bo‘sh stolni bosing.";
    case 6:
      return "Telefon raqamingizni SMS kod bilan tasdiqlang.";
    case 7:
      return "Ma’lumotlarni tekshirib, bronni yarating.";
    default:
      return "";
  }
}

function StepCard({ title, subtitle, children }: { title: string; subtitle?: string; children: React.ReactNode }) {
  return (
    <section className="rounded-3xl border border-zinc-200 bg-white p-5 shadow-sm dark:border-zinc-800 dark:bg-zinc-900 sm:p-6">
      <div className="mb-4">
        <h2 className="text-lg font-semibold sm:text-xl">{title}</h2>
        {subtitle ? <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">{subtitle}</p> : null}
      </div>
      {children}
    </section>
  );
}

function Stepper({ step, onJump }: { step: number; onJump: (s: number) => void }) {
  const pct = ((step - 1) / 6) * 100;
  return (
    <div className="space-y-2">
      <div className="relative h-1.5 w-full overflow-hidden rounded-full bg-zinc-200 dark:bg-zinc-800">
        <div
          className="h-full rounded-full bg-gradient-to-r from-emerald-500 to-emerald-700 transition-all"
          style={{ width: `${pct}%` }}
        />
      </div>
      <ol className="flex justify-between gap-1 text-[10px] sm:text-xs">
        {STEP_TITLES.map((t, i) => {
          const n = i + 1;
          const done = n < step;
          const active = n === step;
          return (
            <li key={t} className="flex min-w-0 flex-1 flex-col items-center">
              <button
                type="button"
                onClick={() => onJump(n)}
                disabled={n >= step}
                className={`flex h-6 w-6 items-center justify-center rounded-full text-[11px] font-semibold transition ${
                  active
                    ? "bg-emerald-600 text-white shadow ring-2 ring-emerald-300"
                    : done
                      ? "bg-emerald-500 text-white hover:bg-emerald-600"
                      : "bg-zinc-200 text-zinc-500 dark:bg-zinc-800"
                } ${n >= step ? "cursor-default" : "cursor-pointer"}`}
                aria-label={`Qadam ${n}: ${t}`}
              >
                {done ? "✓" : n}
              </button>
              <span
                className={`mt-1 max-w-full truncate ${
                  active ? "font-medium text-emerald-700 dark:text-emerald-400" : "text-zinc-500 dark:text-zinc-400"
                }`}
              >
                {t}
              </span>
            </li>
          );
        })}
      </ol>
    </div>
  );
}

function PayMethodCard({
  label,
  sub,
  color,
  selected,
  onSelect,
  disabled = false,
}: {
  id: string;
  label: string;
  sub: string;
  color: string;
  selected: boolean;
  onSelect: () => void;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      onClick={onSelect}
      disabled={disabled}
      aria-pressed={selected}
      className={`relative flex items-center gap-3 rounded-2xl border-2 p-3 text-left transition ${
        selected
          ? "border-[color:var(--brand)] bg-[color:var(--brand-50)] shadow-[0_6px_18px_rgba(27,122,78,0.15)]"
          : "border-[color:var(--border)] bg-white hover:border-[color:var(--brand)]"
      } ${disabled ? "cursor-not-allowed opacity-60" : ""}`}
    >
      <span
        className="inline-flex h-10 w-10 items-center justify-center rounded-xl text-white"
        style={{ background: color }}
      >
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
          <rect x="2" y="5" width="20" height="14" rx="2" />
          <path d="M2 10h20" />
        </svg>
      </span>
      <div className="min-w-0 flex-1">
        <div className="text-sm font-extrabold text-[color:var(--fg)]">{label}</div>
        <div className="text-[11px] text-[color:var(--muted)]">{sub}</div>
      </div>
      <span
        aria-hidden
        className={`flex h-5 w-5 items-center justify-center rounded-full border-2 ${
          selected
            ? "border-[color:var(--brand)] bg-[color:var(--brand)]"
            : "border-[color:var(--border-strong)] bg-white"
        }`}
      >
        {selected ? <span className="h-2 w-2 rounded-full bg-white" /> : null}
      </span>
    </button>
  );
}

function MiniSelectedTable({
  tables,
  selectedId,
  zoneType = "INDOOR",
}: {
  tables: PlanTable[];
  selectedId: string | null;
  zoneType?: string;
}) {
  if (!tables.length || !selectedId) {
    return (
      <div className="flex h-[180px] w-[220px] items-center justify-center rounded-2xl border border-dashed border-[color:var(--border-strong)] bg-[color:var(--bg-soft)] text-xs text-[color:var(--muted)]">
        Tanlangan stol ko&apos;rinmaydi
      </div>
    );
  }
  const sel = tables.find((t) => t.id === selectedId);
  if (!sel) return null;
  // Простая компоновка: абстрактный прямоугольник зала и точка "вы здесь"
  const accent = zoneType === "OUTDOOR" ? "#86c8a4" : zoneType === "VIP" ? "#9333ea" : "#cbd5e1";
  return (
    <div className="relative h-[180px] w-[220px] overflow-hidden rounded-2xl border border-[color:var(--border)] bg-white p-2">
      <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-[color:var(--muted)]">
        Tanlangan stol
      </div>
      <div className="relative h-[140px] w-full rounded-xl bg-[#f8fafc]">
        <div
          className="absolute inset-1 rounded-lg border-2"
          style={{ borderColor: accent }}
        />
        <div className="absolute left-2 top-2 inline-flex items-center gap-1 rounded-full bg-white px-1.5 py-0.5 text-[9px] font-bold text-[color:var(--fg)] shadow-sm">
          <span className="h-1.5 w-1.5 rounded-full" style={{ background: accent }} />
          Zal
        </div>
        <div
          className="absolute flex h-9 w-12 items-center justify-center rounded-md bg-[color:var(--brand)] text-[10px] font-extrabold text-white shadow-lg"
          style={{
            left: "50%",
            top: "50%",
            transform: "translate(-50%, -50%)",
          }}
          title="Sizning stolingiz"
        >
          T-{sel.number}
        </div>
        <div
          className="absolute left-1/2 top-1/2 h-14 w-14 -translate-x-1/2 -translate-y-1/2 animate-ping rounded-full border-2 border-[color:var(--brand)] opacity-40"
        />
      </div>
      <div className="mt-1 text-center text-[10px] text-[color:var(--muted)]">
        {sel.seats} kishilik ·{" "}
        {sel.type === "VIP" ? "VIP" : sel.type === "FAMILY" ? "Oilaviy" : "Standart"}
      </div>
    </div>
  );
}

function isZoneClosed(z: Zone): boolean {
  if (!z.isSeasonal) return false;
  const now = Date.now();
  const from = z.activeFrom ? new Date(z.activeFrom).getTime() : null;
  const to = z.activeTo ? new Date(z.activeTo).getTime() : null;
  if (from !== null && now < from) return true;
  if (to !== null && now > to) return true;
  return false;
}

function zoneLabel(type: string): string {
  switch (type) {
    case "OUTDOOR":
      return "Ochiq havo";
    case "VIP":
      return "VIP zona";
    default:
      return "Ichki zal";
  }
}

function ZoneIllustration({ type, className }: { type: string; className?: string }) {
  // Схематичные иллюстрации по типу зоны (SVG, адаптируются под светлую/тёмную тему)
  const common = `aspect-[5/2] w-full ${className ?? ""}`;
  if (type === "OUTDOOR") {
    return (
      <div
        className={common}
        style={{
          background:
            "linear-gradient(180deg, #eaf6ef 0%, #cfeadb 60%, #a5d8be 100%)",
        }}
      >
        <svg viewBox="0 0 200 80" className="h-full w-full" preserveAspectRatio="xMidYMid meet" aria-hidden>
          <circle cx="170" cy="20" r="10" fill="#fde68a" />
          <path d="M0 60 Q50 40 100 55 T200 52 L200 80 L0 80 Z" fill="#86c8a4" />
          <rect x="30" y="42" width="24" height="18" rx="3" fill="#ffffff" stroke="#1b7a4e" />
          <rect x="95" y="45" width="28" height="16" rx="3" fill="#ffffff" stroke="#1b7a4e" />
          <rect x="150" y="48" width="22" height="14" rx="3" fill="#ffffff" stroke="#1b7a4e" />
          <path d="M15 60 l4 -12 l4 12" fill="#22c55e" />
          <path d="M175 60 l5 -14 l5 14" fill="#22c55e" />
        </svg>
      </div>
    );
  }
  if (type === "VIP") {
    return (
      <div
        className={common}
        style={{
          background:
            "linear-gradient(135deg, #3b0764 0%, #6b21a8 60%, #9333ea 100%)",
        }}
      >
        <svg viewBox="0 0 200 80" className="h-full w-full" preserveAspectRatio="xMidYMid meet" aria-hidden>
          <path d="M0 55 L200 55" stroke="#fde68a" strokeWidth="1" opacity="0.3" />
          <path d="M100 12 l8 24 h22 l-18 14 l7 24 l-19 -15 l-19 15 l7 -24 l-18 -14 h22 z" fill="#fde68a" />
          <rect x="30" y="48" width="32" height="20" rx="3" fill="#ffffff" opacity="0.9" />
          <rect x="140" y="48" width="32" height="20" rx="3" fill="#ffffff" opacity="0.9" />
        </svg>
      </div>
    );
  }
  // INDOOR
  return (
    <div
      className={common}
      style={{
        background:
          "linear-gradient(135deg, #dbeafe 0%, #e0e7ff 60%, #bfdbfe 100%)",
      }}
    >
      <svg viewBox="0 0 200 80" className="h-full w-full" preserveAspectRatio="xMidYMid meet" aria-hidden>
        <rect x="10" y="10" width="180" height="60" rx="6" fill="#ffffff" stroke="#94a3b8" />
        <rect x="20" y="18" width="30" height="12" fill="#e2e8f0" />
        <rect x="60" y="18" width="30" height="12" fill="#e2e8f0" />
        <rect x="100" y="18" width="30" height="12" fill="#e2e8f0" />
        <rect x="140" y="18" width="30" height="12" fill="#e2e8f0" />
        <circle cx="40" cy="50" r="7" fill="#1b7a4e" opacity="0.85" />
        <circle cx="90" cy="50" r="7" fill="#1b7a4e" opacity="0.85" />
        <circle cx="140" cy="50" r="7" fill="#1b7a4e" opacity="0.85" />
        <circle cx="170" cy="50" r="7" fill="#1b7a4e" opacity="0.85" />
      </svg>
    </div>
  );
}

function ZoneCard({
  zone,
  active,
  closed,
  onClick,
}: {
  zone: Zone;
  active: boolean;
  closed: boolean;
  onClick: () => void;
}) {
  const available = zone.availableTables ?? 0;
  const total = zone._count?.tables ?? 0;
  const noTables = total === 0 || available === 0;

  return (
    <button
      type="button"
      onClick={onClick}
      disabled={closed}
      aria-pressed={active}
      className={`bq-card overflow-hidden text-left transition ${
        active
          ? "ring-2 ring-[color:var(--brand)] border-[color:var(--brand)] shadow-[0_10px_30px_rgba(27,122,78,0.18)]"
          : closed
            ? "cursor-not-allowed opacity-70"
            : "bq-card-hover"
      }`}
    >
      <div className="relative">
        <ZoneIllustration type={zone.type} />
        <div className="absolute left-3 top-3 inline-flex items-center gap-1.5 rounded-full bg-white/95 px-2.5 py-1 text-[11px] font-bold text-[color:var(--fg)] shadow-sm">
          {zoneLabel(zone.type)}
          {zone.floor != null ? (
            <span className="text-[color:var(--muted)]">· {zone.floor}-qavat</span>
          ) : null}
        </div>
        {closed ? (
          <div className="absolute inset-0 flex items-center justify-center bg-black/35">
            <span className="rounded-full bg-white px-3 py-1 text-xs font-bold text-[color:var(--danger)] shadow">
              Mavsumiy yopiq
            </span>
          </div>
        ) : active ? (
          <div className="absolute bottom-3 right-3 inline-flex h-7 w-7 items-center justify-center rounded-full bg-[color:var(--brand)] text-white shadow-[0_4px_12px_rgba(27,122,78,0.45)]">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
              <path d="M5 12l5 5L20 7" />
            </svg>
          </div>
        ) : null}
      </div>
      <div className="flex items-center justify-between p-4">
        <div>
          <div className="text-base font-extrabold text-[color:var(--fg)]">{zone.name}</div>
          <div className="mt-0.5 text-xs text-[color:var(--muted)]">
            {total} ta stol
          </div>
        </div>
        {!closed ? (
          noTables ? (
            <span className="inline-flex items-center gap-1 rounded-full bg-[color:var(--bg-soft)] px-2.5 py-1 text-[11px] font-bold text-[color:var(--muted)]">
              Stol yo&apos;q
            </span>
          ) : (
            <span
              className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-[11px] font-bold ${
                available > 5
                  ? "bg-[color:var(--brand-50)] text-[color:var(--brand-700)]"
                  : "bg-amber-50 text-amber-800"
              }`}
            >
              <span
                className="h-1.5 w-1.5 rounded-full"
                style={{ background: available > 5 ? "#22c55e" : "#f59e0b" }}
              />
              {available} ta bo&apos;sh
            </span>
          )
        ) : null}
      </div>
    </button>
  );
}

function GuestAvatars({ count }: { count: number }) {
  const shown = Math.min(count, 10);
  const overflow = count - shown;
  return (
    <div className="flex flex-wrap items-end gap-1.5">
      {Array.from({ length: shown }).map((_, i) => (
        <svg
          key={i}
          width="28"
          height="36"
          viewBox="0 0 28 36"
          aria-hidden
          className="transition-transform hover:-translate-y-0.5"
        >
          <circle cx="14" cy="10" r="6" fill="var(--brand-700)" />
          <path
            d="M4 34c0-5.5 4.5-10 10-10s10 4.5 10 10"
            fill="var(--brand)"
          />
        </svg>
      ))}
      {overflow > 0 ? (
        <span className="ml-1 inline-flex h-8 min-w-[2rem] items-center justify-center rounded-full bg-[color:var(--brand-50)] px-2 text-xs font-bold text-[color:var(--brand-700)]">
          +{overflow}
        </span>
      ) : null}
    </div>
  );
}

function GuestHint({
  guests,
  totalTables,
}: {
  guests: number;
  totalTables: number | null;
}) {
  if (totalTables === null) {
    return (
      <p className="text-xs text-[color:var(--muted)]">
        Filialni va sanani tanlang — shundan keyin imkoniyatlar soni ko&apos;rsatiladi.
      </p>
    );
  }
  if (totalTables === 0) {
    return (
      <div className="rounded-xl border border-amber-300 bg-amber-50 px-3 py-2 text-sm font-medium text-amber-800">
        {guests} kishilik stol topilmadi. Mehmonlar sonini kamaytiring yoki boshqa filialni tanlang.
      </div>
    );
  }
  return (
    <div className="rounded-xl border border-[color:var(--brand-100)] bg-[color:var(--brand-50)] px-3 py-2 text-sm font-medium text-[color:var(--brand-700)]">
      {guests} kishilik stol uchun <b>{totalTables} ta variant</b> bor.
    </div>
  );
}

function loadColor(load: number): string {
  if (load >= 0.85) return "#ef4444";
  if (load >= 0.5) return "#f59e0b";
  if (load > 0) return "#22c55e";
  return "#d1d5db";
}

function DayLoadLegend() {
  return (
    <div className="flex items-center gap-2 text-[10px] text-[color:var(--muted)]">
      <span className="inline-flex items-center gap-1">
        <span className="h-1.5 w-1.5 rounded-full" style={{ background: "#22c55e" }} />
        Ko&apos;p bo&apos;sh
      </span>
      <span className="inline-flex items-center gap-1">
        <span className="h-1.5 w-1.5 rounded-full" style={{ background: "#f59e0b" }} />
        O&apos;rtacha
      </span>
      <span className="inline-flex items-center gap-1">
        <span className="h-1.5 w-1.5 rounded-full" style={{ background: "#ef4444" }} />
        Band
      </span>
    </div>
  );
}

function SummaryRow({ k, v }: { k: string; v: string }) {
  return (
    <li className="flex items-start justify-between gap-4 border-b border-zinc-100 py-2 last:border-b-0 dark:border-zinc-800">
      <span className="text-zinc-500 dark:text-zinc-400">{k}</span>
      <span className="text-right font-medium">{v}</span>
    </li>
  );
}

function MyReservations({ token }: { token: string | null }) {
  const [rows, setRows] = useState<{ id: string; startAt: string; status: string; branch: { name: string } }[]>([]);
  useEffect(() => {
    if (!token) return;
    apiFetch<{ id: string; startAt: string; status: string; branch: { name: string } }[]>("/user/reservations", { token })
      .then(setRows)
      .catch(() => setRows([]));
  }, [token]);
  if (!rows.length) return <p className="text-sm text-zinc-500">Hozircha bron yo‘q.</p>;
  return (
    <ul className="space-y-2 text-sm">
      {rows.map((r) => (
        <li key={r.id} className="flex items-center justify-between rounded-lg border border-zinc-200 bg-zinc-50 px-3 py-2 dark:border-zinc-700 dark:bg-zinc-950">
          <span>{r.branch.name} — {new Date(r.startAt).toLocaleString()}</span>
          <span className="rounded-full bg-zinc-200 px-2 py-0.5 text-[10px] font-medium uppercase text-zinc-700 dark:bg-zinc-800 dark:text-zinc-200">
            {r.status}
          </span>
        </li>
      ))}
    </ul>
  );
}
