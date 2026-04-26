import {
  BadRequestException,
  ForbiddenException,
  Injectable,
  NotFoundException,
  OnModuleInit,
} from '@nestjs/common';
import { DepositStatus, ReservationStatus, TableType } from '@prisma/client';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { ReservationJobsProducer } from '../jobs/reservation-jobs.producer';
import { PaymentsService } from '../payments/payments.service';
import { PrismaService } from '../prisma/prisma.service';
import { TelegramNotifyService } from '../telegram/telegram-notify.service';
import {
  DEPOSIT_UZS_DEFAULT,
  DEPOSIT_UZS_VIP,
  HOLD_MINUTES,
  LOYALTY_THRESHOLD,
  MAX_ACTIVE_RESERVATIONS_PER_DAY,
  MAX_DAYS_AHEAD,
  RESERVATION_DURATION_MINUTES,
} from '../constants/booking';
import { daysFromToday, isPastSlot } from '../utils/slots';

@Injectable()
export class ReservationsService implements OnModuleInit {
  constructor(
    private readonly prisma: PrismaService,
    private readonly events: EventEmitter2,
    private readonly jobs: ReservationJobsProducer,
    private readonly payments: PaymentsService,
    private readonly telegram: TelegramNotifyService,
  ) {}

  onModuleInit() {
    setInterval(() => {
      void this.releaseExpiredHolds();
    }, 60_000);
  }

  private emitZone(zoneId: string) {
    this.events.emit('zone.refresh', zoneId);
    this.events.emit('reservations.refresh', { zoneId });
  }

  async releaseExpiredHolds() {
    const now = new Date();
    const expired = await this.prisma.reservation.findMany({
      where: {
        status: ReservationStatus.PENDING_PAYMENT,
        holdExpiresAt: { lt: now },
      },
      select: { id: true, table: { select: { zoneId: true } } },
    });
    if (!expired.length) return;
    await this.prisma.reservation.updateMany({
      where: { id: { in: expired.map((e) => e.id) } },
      data: {
        status: ReservationStatus.CANCELLED_BY_USER,
        depositStatus: DepositStatus.NONE,
      },
    });
    for (const e of expired) {
      this.emitZone(e.table.zoneId);
    }
  }

  async blockedTableStates(
    tableIds: string[],
    startAt: Date,
    endAt: Date,
  ): Promise<Map<string, 'held' | 'booked'>> {
    await this.releaseExpiredHolds();
    if (!tableIds.length) return new Map();
    const rows = await this.prisma.reservation.findMany({
      where: {
        tableId: { in: tableIds },
        status: { in: [ReservationStatus.CONFIRMED, ReservationStatus.PENDING_PAYMENT] },
        AND: [{ startAt: { lt: endAt } }, { endAt: { gt: startAt } }],
      },
      select: { tableId: true, status: true },
    });
    const map = new Map<string, 'held' | 'booked'>();
    for (const r of rows) {
      const kind = r.status === ReservationStatus.CONFIRMED ? 'booked' : 'held';
      const prev = map.get(r.tableId);
      if (prev === 'booked') continue;
      map.set(r.tableId, kind);
    }
    return map;
  }

  async getProfile(userId: string) {
    const user = await this.prisma.user.findUnique({
      where: { id: userId },
      select: {
        id: true,
        phone: true,
        name: true,
        role: true,
        completedBookings: true,
        loyaltyBonuses: true,
        telegramLinkedAt: true,
        telegramUsername: true,
        noShowStreak: true,
        bookingBlockedUntil: true,
      },
    });
    if (!user) throw new NotFoundException('Foydalanuvchi topilmadi');
    const completed = user.completedBookings ?? 0;
    const inCycle = completed % LOYALTY_THRESHOLD;
    const toNext = inCycle === 0 && completed > 0 ? 0 : LOYALTY_THRESHOLD - inCycle;
    return {
      ...user,
      loyalty: {
        threshold: LOYALTY_THRESHOLD,
        completed,
        inCycle,
        toNext,
        bonuses: user.loyaltyBonuses ?? 0,
        bonusValueUzs: 50_000,
      },
    };
  }

  async updateProfile(userId: string, dto: { name?: string }) {
    const data: { name?: string | null } = {};
    if (dto.name !== undefined) {
      const clean = (dto.name ?? '').trim();
      data.name = clean || null;
    }
    await this.prisma.user.update({ where: { id: userId }, data });
    return this.getProfile(userId);
  }

  async listMine(userId: string) {
    return this.prisma.reservation.findMany({
      where: { userId },
      orderBy: { startAt: 'desc' },
      take: 50,
      include: {
        branch: { select: { id: true, name: true, address: true, lat: true, lng: true } },
        table: { select: { id: true, number: true, seats: true, type: true, zone: { select: { id: true, name: true } } } },
        payments: true,
        preorderItems: true,
      },
    });
  }

  async getOneForUser(id: string, userId: string) {
    const r = await this.prisma.reservation.findFirst({
      where: { id, userId },
      include: {
        branch: true,
        table: { include: { zone: true } },
        payments: true,
        preorderItems: true,
      },
    });
    if (!r) throw new NotFoundException('Bron topilmadi');
    return r;
  }

  async create(
    userId: string,
    dto: {
      branchId: string;
      tableId: string;
      startAt: string;
      guestsCount: number;
      useBonus?: boolean;
      items?: { menuItemId: string; quantity: number }[];
    },
  ) {
    await this.releaseExpiredHolds();
    const u = await this.prisma.user.findUnique({ where: { id: userId } });
    if (u?.bookingBlockedUntil && u.bookingBlockedUntil > new Date()) {
      throw new ForbiddenException(
        'Bron qilish bloklangan (TZ: ketma-ket 3 no-show). Muddati: ' + u.bookingBlockedUntil.toISOString(),
      );
    }
    const startAt = new Date(dto.startAt);
    if (Number.isNaN(startAt.getTime())) throw new BadRequestException('Noto‘g‘ri startAt');
    const dateStr = startAt.toISOString().slice(0, 10);
    const delta = daysFromToday(dateStr);
    if (delta < 0 || delta > MAX_DAYS_AHEAD) throw new BadRequestException('Sana oralig‘i noto‘g‘ri');
    if (isPastSlot(startAt)) throw new BadRequestException('O‘tgan vaqt tanlanmagan');

    const endAt = new Date(startAt.getTime() + RESERVATION_DURATION_MINUTES * 60 * 1000);

    const dayStart = new Date(startAt.getFullYear(), startAt.getMonth(), startAt.getDate());
    const dayEnd = new Date(dayStart.getTime() + 24 * 60 * 60 * 1000);
    const dayCount = await this.prisma.reservation.count({
      where: {
        userId,
        startAt: { gte: dayStart, lt: dayEnd },
        status: { in: [ReservationStatus.PENDING_PAYMENT, ReservationStatus.CONFIRMED] },
      },
    });
    if (dayCount >= MAX_ACTIVE_RESERVATIONS_PER_DAY) {
      throw new BadRequestException('Kuniga 3 tadan ortiq faol bron qilish mumkin emas');
    }

    const table = await this.prisma.restaurantTable.findFirst({
      where: { id: dto.tableId, zone: { branchId: dto.branchId } },
      include: { zone: true },
    });
    if (!table) throw new NotFoundException('Stol topilmadi');
    if (table.status === 'MAINTENANCE') throw new BadRequestException('Stol texnik xizmatda');

    if (dto.guestsCount > table.seats) {
      throw new BadRequestException('Mehmonlar soni stul sonidan oshmasligi kerak');
    }

    const overlap = await this.prisma.reservation.findFirst({
      where: {
        tableId: table.id,
        status: { in: [ReservationStatus.CONFIRMED, ReservationStatus.PENDING_PAYMENT] },
        AND: [{ startAt: { lt: endAt } }, { endAt: { gt: startAt } }],
      },
    });
    if (overlap) throw new BadRequestException('Bu vaqt uchun stol allaqachon band');

    const baseDeposit =
      table.type === TableType.VIP ? DEPOSIT_UZS_VIP : DEPOSIT_UZS_DEFAULT;

    // Применение бонуса: если useBonus и у юзера есть, депозит = 0, авто-confirm
    let deposit = baseDeposit;
    let useBonus = false;
    if (dto.useBonus) {
      const freshUser = await this.prisma.user.findUnique({
        where: { id: userId },
        select: { loyaltyBonuses: true },
      });
      if ((freshUser?.loyaltyBonuses ?? 0) > 0) {
        useBonus = true;
        deposit = 0;
      } else {
        throw new BadRequestException('Sizda bonus yo\'q');
      }
    }

    const holdExpiresAt = new Date(Date.now() + HOLD_MINUTES * 60 * 1000);

    const r = await this.prisma.reservation.create({
      data: {
        userId,
        tableId: table.id,
        branchId: dto.branchId,
        startAt,
        endAt,
        guestsCount: dto.guestsCount,
        status: useBonus ? ReservationStatus.CONFIRMED : ReservationStatus.PENDING_PAYMENT,
        depositAmount: deposit,
        depositStatus: useBonus ? DepositStatus.PAID : DepositStatus.PENDING,
        holdExpiresAt: useBonus ? null : holdExpiresAt,
      },
      include: {
        branch: { select: { id: true, name: true } },
        table: { select: { id: true, number: true, zoneId: true } },
      },
    });

    if (useBonus) {
      await this.prisma.user.update({
        where: { id: userId },
        data: { loyaltyBonuses: { decrement: 1 } },
      });
      await this.jobs.schedulePostConfirm(r.id, startAt);
    } else {
      await this.jobs.scheduleHoldExpiry(r.id, holdExpiresAt);
    }

    // Сохраняем предзаказ (если есть и валиден)
    if (dto.items && dto.items.length > 0) {
      const ids = Array.from(new Set(dto.items.map((i) => i.menuItemId)));
      const menu = await this.prisma.menuItem.findMany({
        where: { id: { in: ids }, branchId: dto.branchId, isActive: true },
      });
      const byId = new Map(menu.map((m) => [m.id, m] as const));
      const rows = dto.items
        .filter((i) => byId.has(i.menuItemId))
        .map((i) => {
          const m = byId.get(i.menuItemId)!;
          return {
            reservationId: r.id,
            menuItemId: m.id,
            name: m.name,
            unitPrice: m.price,
            quantity: Math.max(1, Math.min(50, i.quantity | 0)),
          };
        });
      if (rows.length > 0) {
        await this.prisma.reservationItem.createMany({ data: rows });
      }
    }
    this.emitZone(table.zoneId);

    void this.telegram.notifyUser(
      userId,
      [
        '🪑 <b>Bron yaratildi</b>',
        '',
        `Filial: <b>${r.branch.name}</b>`,
        `Stol: <b>${r.table.number}</b>`,
        `Vaqt: <b>${r.startAt.toLocaleString('uz-UZ')}</b>`,
        `Mehmonlar: <b>${r.guestsCount}</b>`,
        '',
        useBonus
          ? '🎁 Bonus qo\'llanildi — depozit 0 so\'m. Bron avtomatik tasdiqlandi.'
          : `To'lovni ${HOLD_MINUTES} daqiqa ichida amalga oshiring, aks holda bron bekor qilinadi.`,
      ].join('\n'),
    );

    void this.telegram.notifyAdmins(
      [
        '🆕 <b>Yangi bron</b>',
        '',
        `Filial: <b>${r.branch.name}</b>`,
        `Stol: <b>T-${r.table.number}</b>`,
        `Vaqt: <b>${r.startAt.toLocaleString('uz-UZ')}</b>`,
        `Mehmonlar: <b>${r.guestsCount}</b>`,
        `Holat: <b>${useBonus ? 'Tasdiqlangan (bonus)' : 'To\'lov kutilmoqda'}</b>`,
      ].join('\n'),
    );

    return r;
  }

  async cancel(id: string, userId: string) {
    const r = await this.prisma.reservation.findFirst({ where: { id, userId } });
    if (!r) throw new NotFoundException('Bron topilmadi');
    if (
      r.status !== ReservationStatus.PENDING_PAYMENT &&
      r.status !== ReservationStatus.CONFIRMED
    ) {
      throw new BadRequestException('Bu holatda bekor qilib bo‘lmaydi');
    }
    if (r.status === ReservationStatus.CONFIRMED) {
      const msBefore = r.startAt.getTime() - Date.now();
      if (msBefore < 2 * 60 * 60 * 1000) {
        throw new BadRequestException('2 soatdan kam vaqt qolganda bekor qilish cheklangan (TZ)');
      }
    }
    const updated = await this.prisma.reservation.update({
      where: { id },
      data: { status: ReservationStatus.CANCELLED_BY_USER, depositStatus: DepositStatus.REFUNDED },
      include: { table: { select: { zoneId: true } } },
    });
    await this.jobs.cancelScheduledForReservation(id);
    this.emitZone(updated.table.zoneId);

    void this.telegram.notifyUser(
      userId,
      '❌ <b>Bron bekor qilindi</b>\n\nAgar qaytarilishi kerak bo\'lsa — depozit 1-3 kun ichida qaytariladi.',
    );
    void this.telegram.notifyAdmins(
      `❌ <b>Bron bekor qilindi</b>\n\nID: <code>${updated.id.slice(0, 8)}</code>\nMijoz: bekor qildi`,
    );
    return updated;
  }

  async payMock(id: string, userId: string) {
    await this.releaseExpiredHolds();
    return this.payments.completeMockReservation(id, userId);
  }

  async checkout(userId: string, reservationId: string, provider: 'payme' | 'click') {
    return this.payments.createCheckout(reservationId, userId, provider);
  }

  /** Admin */
  async adminList(params: { branchId?: string; date?: string }) {
    const where: Record<string, unknown> = {};
    if (params.branchId) where.branchId = params.branchId;
    if (params.date) {
      const d = new Date(`${params.date}T00:00:00`);
      const next = new Date(d.getTime() + 24 * 3600 * 1000);
      where.startAt = { gte: d, lt: next };
    }
    return this.prisma.reservation.findMany({
      where,
      orderBy: { startAt: 'desc' },
      take: 200,
      include: {
        user: { select: { id: true, phone: true, name: true } },
        branch: { select: { id: true, name: true } },
        table: { select: { id: true, number: true } },
        payments: true,
      },
    });
  }

  /**
   * Выгрузка броней в формате строк CSV (без заголовка) —
   * заголовок формируется в контроллере.
   */
  async adminExportCsv(params: {
    branchId?: string;
    from?: string;
    to?: string;
    status?: string;
  }): Promise<Array<Array<string | number>>> {
    const where: Record<string, unknown> = {};
    if (params.branchId) where.branchId = params.branchId;
    if (params.from || params.to) {
      const range: Record<string, Date> = {};
      if (params.from) range.gte = new Date(`${params.from}T00:00:00`);
      if (params.to) range.lt = new Date(new Date(`${params.to}T00:00:00`).getTime() + 24 * 3600 * 1000);
      where.startAt = range;
    }
    if (params.status) where.status = params.status as ReservationStatus;

    const items = await this.prisma.reservation.findMany({
      where,
      orderBy: { startAt: 'desc' },
      take: 10_000,
      include: {
        user: { select: { phone: true, name: true, telegramUsername: true } },
        branch: { select: { name: true } },
        table: {
          select: { number: true, zone: { select: { name: true } } },
        },
        preorderItems: { select: { quantity: true, unitPrice: true } },
      },
    });

    return items.map((r) => {
      const shortId = r.id.slice(0, 8).toUpperCase();
      const preorderTotal = r.preorderItems.reduce(
        (s, it) => s + Number(it.unitPrice) * it.quantity,
        0,
      );
      return [
        r.id,
        shortId,
        r.createdAt.toISOString(),
        r.startAt.toISOString(),
        r.status,
        r.branch?.name ?? '',
        r.table?.number ?? '',
        r.table?.zone?.name ?? '',
        r.guestsCount,
        Number(r.depositAmount),
        r.depositStatus,
        r.user?.name ?? '',
        r.user?.phone ?? '',
        r.user?.telegramUsername ? '@' + r.user.telegramUsername : '',
        preorderTotal,
      ];
    });
  }

  async adminSetStatus(id: string, status: ReservationStatus) {
    const r = await this.prisma.reservation.findUnique({
      where: { id },
      include: { table: { select: { zoneId: true } } },
    });
    if (!r) throw new NotFoundException('Bron topilmadi');
    const loyaltyChange = { earnedBonus: false, totalBonuses: 0, totalCompleted: 0 };
    const updated = await this.prisma.$transaction(async (tx) => {
      const res = await tx.reservation.update({
        where: { id },
        data: { status },
      });
      if (status === ReservationStatus.COMPLETED) {
        const user = await tx.user.update({
          where: { id: r.userId },
          data: {
            noShowStreak: 0,
            bookingBlockedUntil: null,
            completedBookings: { increment: 1 },
          },
          select: { completedBookings: true, loyaltyBonuses: true },
        });
        loyaltyChange.totalCompleted = user.completedBookings;
        loyaltyChange.totalBonuses = user.loyaltyBonuses;
        if (user.completedBookings > 0 && user.completedBookings % LOYALTY_THRESHOLD === 0) {
          const bonused = await tx.user.update({
            where: { id: r.userId },
            data: { loyaltyBonuses: { increment: 1 } },
            select: { loyaltyBonuses: true },
          });
          loyaltyChange.earnedBonus = true;
          loyaltyChange.totalBonuses = bonused.loyaltyBonuses;
        }
      }
      return res;
    });
    await this.jobs.cancelScheduledForReservation(id);
    this.emitZone(r.table.zoneId);

    if (status === ReservationStatus.CONFIRMED) {
      void this.telegram.notifyUser(
        r.userId,
        '✅ <b>Broningiz tasdiqlandi</b>\n\nKelishingizni kutamiz! Kechikmasdan keling.',
      );
    }
    if (status === ReservationStatus.COMPLETED) {
      const { totalCompleted, earnedBonus, totalBonuses } = loyaltyChange;
      const remaining = LOYALTY_THRESHOLD - (totalCompleted % LOYALTY_THRESHOLD || LOYALTY_THRESHOLD);
      const baseMsg = [
        '🎉 <b>Rahmat tashrifingiz uchun!</b>',
        '',
        `Jami yakunlangan bronlar: <b>${totalCompleted}</b>`,
      ];
      if (earnedBonus) {
        baseMsg.push(
          '',
          `🎁 <b>Yangi bonus!</b> Sizda ${totalBonuses} ta bonus bor — keyingi bronda ishlating.`,
        );
      } else {
        baseMsg.push(
          `Keyingi bonus uchun: <b>${remaining} ta bron</b> qoldi.`,
        );
      }
      void this.telegram.notifyUser(r.userId, baseMsg.join('\n'));
    }
    if (
      status === ReservationStatus.CANCELLED_BY_RESTAURANT ||
      status === ReservationStatus.CANCELLED_BY_USER
    ) {
      void this.telegram.notifyUser(
        r.userId,
        '❌ <b>Bron bekor qilindi</b>\n\nDepozit qaytariladi (agar to\'langan bo\'lsa).',
      );
    }

    if (status === ReservationStatus.NO_SHOW) {
      void this.telegram.notifyAdmins(
        `🚫 <b>No-show</b>\n\nID: <code>${id.slice(0, 8)}</code>\nMijoz kelmadi — depozit ushlab qolindi.`,
      );
    }

    return updated;
  }
}
