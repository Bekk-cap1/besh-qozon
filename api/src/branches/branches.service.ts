import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { ReviewsService } from '../reviews/reviews.service';
import { buildSlotLabelsForDate, daysFromToday } from '../utils/slots';
import {
  MAX_DAYS_AHEAD,
  RESERVATION_DURATION_MINUTES,
} from '../constants/booking';

function isoDate(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

@Injectable()
export class BranchesService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly reviews: ReviewsService,
  ) {}

  async findAll() {
    const branches = await this.prisma.branch.findMany({
      where: { isActive: true },
      orderBy: { name: 'asc' },
      include: {
        _count: { select: { zones: true } },
      },
    });

    // Aggregate table counts per branch in a single query
    const rows = await this.prisma.restaurantTable.findMany({
      select: { status: true, zone: { select: { branchId: true } } },
    });
    const agg = new Map<string, { total: number; available: number }>();
    for (const t of rows) {
      const bId = t.zone.branchId;
      const cur = agg.get(bId) ?? { total: 0, available: 0 };
      cur.total++;
      if (t.status === 'AVAILABLE') cur.available++;
      agg.set(bId, cur);
    }

    const ratings = await this.reviews.averageByBranch(branches.map((b) => b.id));

    return branches.map((b) => {
      const s = agg.get(b.id) ?? { total: 0, available: 0 };
      const r = ratings[b.id];
      return {
        ...b,
        totalTables: s.total,
        availableTables: s.available,
        averageRating: r?.avg ?? null,
        reviewsCount: r?.count ?? 0,
      };
    });
  }

  async findOne(id: string) {
    const b = await this.prisma.branch.findFirst({
      where: { id, isActive: true },
      include: {
        zones: { orderBy: { name: 'asc' } },
      },
    });
    if (!b) throw new NotFoundException('Filial topilmadi');
    return b;
  }

  async slotsForDate(branchId: string, date: string) {
    const b = await this.prisma.branch.findFirst({
      where: { id: branchId, isActive: true },
    });
    if (!b) throw new NotFoundException('Filial topilmadi');
    const delta = daysFromToday(date);
    if (delta < 0 || delta > MAX_DAYS_AHEAD) {
      return { slots: [] as string[], message: 'Sana ruxsat etilgan oralig‘dan tashqari' };
    }
    const slots = buildSlotLabelsForDate(date, b.workHours);
    return { slots, date, branchId };
  }

  /**
   * Загрузка календаря: для каждого из `days` ближайших дней возвращает
   * `load` (0..1), количество броней и общую вместимость.
   */
  async dayLoad(branchId: string, days: number) {
    const b = await this.prisma.branch.findFirst({
      where: { id: branchId, isActive: true },
    });
    if (!b) throw new NotFoundException('Filial topilmadi');

    const d = Math.max(1, Math.min(MAX_DAYS_AHEAD + 1, days || 15));

    const totalTables = await this.prisma.restaurantTable.count({
      where: { zone: { branchId }, status: 'AVAILABLE' },
    });

    const slotsPerDay = Math.max(
      1,
      buildSlotLabelsForDate(isoDate(new Date()), b.workHours).length,
    );

    const today = new Date();
    const start = new Date(today.getFullYear(), today.getMonth(), today.getDate());
    const end = new Date(start);
    end.setDate(end.getDate() + d);

    const reservations = await this.prisma.reservation.findMany({
      where: {
        branchId,
        startAt: { gte: start, lt: end },
        status: { notIn: ['CANCELLED_BY_USER', 'CANCELLED_BY_RESTAURANT', 'NO_SHOW'] },
      },
      select: { startAt: true },
    });

    const capacity = Math.max(1, totalTables * slotsPerDay);

    const perDay: { date: string; load: number; reservations: number }[] = [];
    for (let i = 0; i < d; i++) {
      const day = new Date(start);
      day.setDate(day.getDate() + i);
      const iso = isoDate(day);
      const count = reservations.filter((r) => isoDate(r.startAt) === iso).length;
      perDay.push({
        date: iso,
        load: Math.min(1, count / capacity),
        reservations: count,
      });
    }

    return {
      branchId,
      days: perDay,
      totalTables,
      slotsPerDay,
    };
  }

  /**
   * Загрузка по тайм-слотам выбранного дня с учётом количества гостей.
   * Для каждого слота возвращает freeTables / totalTables.
   */
  async slotLoad(branchId: string, dateStr: string, guests: number) {
    const b = await this.prisma.branch.findFirst({
      where: { id: branchId, isActive: true },
    });
    if (!b) throw new NotFoundException('Filial topilmadi');
    const delta = daysFromToday(dateStr);
    if (delta < 0 || delta > MAX_DAYS_AHEAD) {
      return { slots: [], date: dateStr, guests, totalTables: 0 };
    }

    const g = Math.max(1, guests || 1);
    const slots = buildSlotLabelsForDate(dateStr, b.workHours);

    const tables = await this.prisma.restaurantTable.findMany({
      where: {
        zone: { branchId },
        status: 'AVAILABLE',
        seats: { gte: g },
      },
      select: { id: true, seats: true },
    });
    const totalTables = tables.length;
    const tableIds = tables.map((t) => t.id);

    if (!totalTables || !slots.length) {
      return {
        slots: slots.map((s) => ({ slot: s, freeTables: 0, totalTables })),
        date: dateStr,
        guests: g,
        totalTables,
      };
    }

    const dayStart = new Date(`${dateStr}T00:00:00`);
    const dayEnd = new Date(`${dateStr}T23:59:59`);
    const reservations = await this.prisma.reservation.findMany({
      where: {
        tableId: { in: tableIds },
        status: { notIn: ['CANCELLED_BY_USER', 'CANCELLED_BY_RESTAURANT', 'NO_SHOW'] },
        startAt: { lt: dayEnd },
        endAt: { gt: dayStart },
      },
      select: { tableId: true, startAt: true, endAt: true },
    });

    const result = slots.map((slot) => {
      const start = new Date(`${dateStr}T${slot}:00`);
      const end = new Date(start.getTime() + RESERVATION_DURATION_MINUTES * 60_000);
      const busy = new Set<string>();
      for (const r of reservations) {
        if (r.startAt < end && r.endAt > start) busy.add(r.tableId);
      }
      return { slot, freeTables: totalTables - busy.size, totalTables };
    });

    return { slots: result, date: dateStr, guests: g, totalTables };
  }
}
