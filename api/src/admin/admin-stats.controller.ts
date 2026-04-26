import { Controller, Get, Query, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Roles } from '../auth/roles.decorator';
import { RolesGuard } from '../auth/roles.guard';
import { PrismaService } from '../prisma/prisma.service';

function isoDay(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

@Controller('admin/stats')
@UseGuards(JwtAuthGuard, RolesGuard)
@Roles('ADMIN')
export class AdminStatsController {
  constructor(private readonly prisma: PrismaService) {}

  /**
   * Общая статистика для Dashboard.
   * Возвращает:
   *  - metrics: today/yesterday бронирований/депозита/no-show
   *  - daily: по дням за N дней — count и revenue
   *  - statusBreakdown: по статусам за N дней
   *  - branches: бандлаж по филиалам (totalTables/availableTables)
   */
  @Get('overview')
  async overview(
    @Query('days') daysRaw?: string,
    @Query('branchId') branchId?: string,
  ) {
    const days = Math.max(1, Math.min(90, Number(daysRaw) || 14));

    const now = new Date();
    const todayStart = new Date(now);
    todayStart.setHours(0, 0, 0, 0);
    const tomorrowStart = new Date(todayStart.getTime() + 24 * 3600 * 1000);
    const yesterdayStart = new Date(todayStart.getTime() - 24 * 3600 * 1000);
    const rangeStart = new Date(todayStart.getTime() - (days - 1) * 24 * 3600 * 1000);
    const rangeEnd = tomorrowStart;

    const where: Record<string, unknown> = {
      startAt: { gte: rangeStart, lt: rangeEnd },
    };
    if (branchId) where.branchId = branchId;

    const reservations = await this.prisma.reservation.findMany({
      where,
      select: {
        id: true,
        startAt: true,
        status: true,
        guestsCount: true,
        depositAmount: true,
        branchId: true,
      },
      orderBy: { startAt: 'asc' },
    });

    // Ежедневная агрегация
    const perDay = new Map<string, { date: string; count: number; revenue: number; confirmed: number; noShow: number }>();
    for (let i = 0; i < days; i++) {
      const d = new Date(rangeStart.getTime() + i * 24 * 3600 * 1000);
      const key = isoDay(d);
      perDay.set(key, { date: key, count: 0, revenue: 0, confirmed: 0, noShow: 0 });
    }
    const statusMap = new Map<string, number>();

    let todayCount = 0;
    let yesterdayCount = 0;
    let todayRevenue = 0;
    let todayNoShow = 0;
    let todayPending = 0;

    const revenueStatuses: string[] = ['CONFIRMED', 'COMPLETED', 'PENDING_PAYMENT'];
    for (const r of reservations) {
      const key = isoDay(r.startAt);
      const row = perDay.get(key);
      if (row) {
        row.count += 1;
        if (revenueStatuses.includes(r.status)) {
          row.revenue += Number(r.depositAmount ?? 0);
        }
        if (r.status === 'CONFIRMED') row.confirmed += 1;
        if (r.status === 'NO_SHOW') row.noShow += 1;
      }
      statusMap.set(r.status, (statusMap.get(r.status) ?? 0) + 1);

      const t = r.startAt.getTime();
      if (t >= todayStart.getTime() && t < tomorrowStart.getTime()) {
        todayCount += 1;
        if (revenueStatuses.includes(r.status)) {
          todayRevenue += Number(r.depositAmount ?? 0);
        }
        if (r.status === 'NO_SHOW') todayNoShow += 1;
        if (r.status === 'PENDING_PAYMENT') todayPending += 1;
      } else if (t >= yesterdayStart.getTime() && t < todayStart.getTime()) {
        yesterdayCount += 1;
      }
    }

    const daily = Array.from(perDay.values());
    const statusBreakdown = Array.from(statusMap.entries()).map(([status, count]) => ({ status, count }));

    // Филиалы с bандом столов
    const branches = await this.prisma.branch.findMany({
      where: { isActive: true },
      select: { id: true, name: true },
    });
    const allTables = await this.prisma.restaurantTable.findMany({
      where: branchId ? { zone: { branchId } } : undefined,
      select: { status: true, zone: { select: { branchId: true } } },
    });
    const bTotals = new Map<string, { total: number; available: number }>();
    for (const t of allTables) {
      const bid = t.zone?.branchId;
      if (!bid) continue;
      const cur = bTotals.get(bid) ?? { total: 0, available: 0 };
      cur.total += 1;
      if (t.status === 'AVAILABLE') cur.available += 1;
      bTotals.set(bid, cur);
    }
    const branchesOut = branches.map((b) => ({
      id: b.id,
      name: b.name,
      totalTables: bTotals.get(b.id)?.total ?? 0,
      availableTables: bTotals.get(b.id)?.available ?? 0,
    }));

    return {
      metrics: {
        todayCount,
        yesterdayCount,
        todayRevenue,
        todayNoShow,
        todayPending,
        totalTables: Array.from(bTotals.values()).reduce((s, x) => s + x.total, 0),
        availableTables: Array.from(bTotals.values()).reduce((s, x) => s + x.available, 0),
      },
      daily,
      statusBreakdown,
      branches: branchesOut,
      rangeStart: isoDay(rangeStart),
      rangeEnd: isoDay(new Date(rangeEnd.getTime() - 24 * 3600 * 1000)),
    };
  }

  /**
   * Агрегации для страницы «Hisobotlar»: топ блюд, средний чек,
   * процент no-show, загрузка по часам и топ филиалы.
   */
  @Get('reports')
  async reports(
    @Query('days') daysRaw?: string,
    @Query('branchId') branchId?: string,
  ) {
    const days = Math.max(1, Math.min(180, Number(daysRaw) || 30));
    const now = new Date();
    const todayStart = new Date(now);
    todayStart.setHours(0, 0, 0, 0);
    const tomorrowStart = new Date(todayStart.getTime() + 24 * 3600 * 1000);
    const rangeStart = new Date(todayStart.getTime() - (days - 1) * 24 * 3600 * 1000);

    const whereRes: Record<string, unknown> = {
      startAt: { gte: rangeStart, lt: tomorrowStart },
    };
    if (branchId) whereRes.branchId = branchId;

    const [reservations, preorderRows, branches] = await Promise.all([
      this.prisma.reservation.findMany({
        where: whereRes,
        select: {
          id: true,
          startAt: true,
          status: true,
          guestsCount: true,
          depositAmount: true,
          branchId: true,
        },
      }),
      this.prisma.reservationItem.findMany({
        where: { reservation: whereRes },
        select: {
          name: true,
          quantity: true,
          unitPrice: true,
          menuItemId: true,
        },
      }),
      this.prisma.branch.findMany({
        where: { isActive: true },
        select: { id: true, name: true },
      }),
    ]);

    const total = reservations.length;
    const completed = reservations.filter((r) => r.status === 'COMPLETED').length;
    const noShow = reservations.filter((r) => r.status === 'NO_SHOW').length;
    const confirmed = reservations.filter((r) => r.status === 'CONFIRMED').length;
    const cancelled = reservations.filter((r) =>
      ['CANCELLED_BY_USER', 'CANCELLED_BY_RESTAURANT'].includes(r.status),
    ).length;

    const revenueRelevant = reservations.filter((r) =>
      ['CONFIRMED', 'COMPLETED'].includes(r.status),
    );
    const revenue = revenueRelevant.reduce((s, r) => s + Number(r.depositAmount ?? 0), 0);
    const preorderRevenue = preorderRows.reduce(
      (s, it) => s + Number(it.unitPrice) * it.quantity,
      0,
    );
    const avgCheck = revenueRelevant.length
      ? Math.round((revenue + preorderRevenue) / revenueRelevant.length)
      : 0;

    const noShowPct = total > 0 ? Math.round((noShow / total) * 1000) / 10 : 0;

    // Топ блюд
    const topMap = new Map<
      string,
      { menuItemId: string; name: string; quantity: number; revenue: number }
    >();
    for (const it of preorderRows) {
      const cur = topMap.get(it.menuItemId) ?? {
        menuItemId: it.menuItemId,
        name: it.name,
        quantity: 0,
        revenue: 0,
      };
      cur.quantity += it.quantity;
      cur.revenue += Number(it.unitPrice) * it.quantity;
      topMap.set(it.menuItemId, cur);
    }
    const topDishes = Array.from(topMap.values())
      .sort((a, b) => b.quantity - a.quantity)
      .slice(0, 10);

    // По часам
    const hourly = new Array(24).fill(0).map((_, h) => ({ hour: h, count: 0 }));
    for (const r of reservations) {
      hourly[r.startAt.getHours()].count += 1;
    }

    // По филиалам
    const byBranchMap = new Map<string, { branchId: string; count: number; revenue: number }>();
    for (const r of reservations) {
      const cur = byBranchMap.get(r.branchId) ?? {
        branchId: r.branchId,
        count: 0,
        revenue: 0,
      };
      cur.count += 1;
      if (['CONFIRMED', 'COMPLETED'].includes(r.status)) {
        cur.revenue += Number(r.depositAmount ?? 0);
      }
      byBranchMap.set(r.branchId, cur);
    }
    const byBranch = Array.from(byBranchMap.values())
      .map((x) => ({
        ...x,
        name: branches.find((b) => b.id === x.branchId)?.name ?? '—',
      }))
      .sort((a, b) => b.count - a.count);

    return {
      range: {
        from: isoDay(rangeStart),
        to: isoDay(new Date(tomorrowStart.getTime() - 24 * 3600 * 1000)),
        days,
      },
      summary: {
        total,
        completed,
        confirmed,
        noShow,
        cancelled,
        noShowPct,
        revenue,
        preorderRevenue,
        avgCheck,
      },
      topDishes,
      hourly,
      byBranch,
    };
  }
}
