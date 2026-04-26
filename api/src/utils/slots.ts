import { SLOT_STEP_MINUTES } from '../constants/booking';

export type WorkHoursJson = { open?: string; close?: string };

const DEFAULT_OPEN = '10:00';
const DEFAULT_CLOSE = '22:00';

function parseHm(s: string): { h: number; m: number } {
  const [h, m] = s.split(':').map((x) => parseInt(x, 10));
  return { h: h || 0, m: m || 0 };
}

function minutesSinceMidnight(d: Date): number {
  return d.getHours() * 60 + d.getMinutes();
}

export function buildSlotLabelsForDate(dateStr: string, workHours: unknown): string[] {
  const day = new Date(`${dateStr}T12:00:00`);
  if (Number.isNaN(day.getTime())) return [];
  const wh = (workHours ?? {}) as WorkHoursJson;
  const open = wh.open && /^\d{1,2}:\d{2}$/.test(wh.open) ? wh.open : DEFAULT_OPEN;
  const close = wh.close && /^\d{1,2}:\d{2}$/.test(wh.close) ? wh.close : DEFAULT_CLOSE;
  const o = parseHm(open);
  const c = parseHm(close);
  let start = o.h * 60 + o.m;
  const end = c.h * 60 + c.m;
  const labels: string[] = [];
  for (let t = start; t + SLOT_STEP_MINUTES <= end; t += SLOT_STEP_MINUTES) {
    const hh = Math.floor(t / 60)
      .toString()
      .padStart(2, '0');
    const mm = (t % 60).toString().padStart(2, '0');
    labels.push(`${hh}:${mm}`);
  }
  return labels;
}

export function combineDateAndTime(dateStr: string, timeHm: string): Date {
  return new Date(`${dateStr}T${timeHm}:00`);
}

export function isPastSlot(slotStart: Date): boolean {
  return slotStart.getTime() < Date.now();
}

export function startOfUtcDay(d: Date): Date {
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
}

export function daysFromToday(dateStr: string): number {
  const target = new Date(`${dateStr}T12:00:00`);
  const today = new Date();
  const t0 = new Date(today.getFullYear(), today.getMonth(), today.getDate());
  const t1 = new Date(target.getFullYear(), target.getMonth(), target.getDate());
  return Math.round((t1.getTime() - t0.getTime()) / (24 * 3600 * 1000));
}
