import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { Job } from 'bullmq';
import { DepositStatus, ReservationStatus } from '@prisma/client';
import { RESERVATION_DURATION_MINUTES } from '../constants/booking';
import { PrismaService } from '../prisma/prisma.service';
import { TelegramNotifyService } from '../telegram/telegram-notify.service';
import type { BeshJob } from './reservation-job.types';
import { ReservationJobsProducer } from './reservation-jobs.producer';

@Processor('besh')
export class ReservationJobsProcessor extends WorkerHost {
  private readonly log = new Logger(ReservationJobsProcessor.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly events: EventEmitter2,
    private readonly producer: ReservationJobsProducer,
    private readonly telegram: TelegramNotifyService,
  ) {
    super();
  }

  async process(job: Job<BeshJob>): Promise<void> {
    const d = job.data;
    if (d.type === 'EXPIRE_HOLD') await this.expireHold(d.reservationId);
    if (d.type === 'REMINDER_SMS') await this.reminder(d.reservationId);
    if (d.type === 'SLOT_ENDING_SOON') await this.slotEndingSoon(d.reservationId);
    if (d.type === 'OVERSTAY_CHECK') await this.overstayCheck(d.reservationId);
    if (d.type === 'NO_SHOW') await this.noShow(d.reservationId);
  }

  private emitZone(zoneId: string) {
    this.events.emit('zone.refresh', zoneId);
  }

  private async expireHold(reservationId: string) {
    const r = await this.prisma.reservation.findUnique({
      where: { id: reservationId },
      include: { table: { select: { zoneId: true } } },
    });
    if (!r || r.status !== ReservationStatus.PENDING_PAYMENT) return;
    if (r.holdExpiresAt && r.holdExpiresAt.getTime() > Date.now()) return;
    await this.prisma.reservation.update({
      where: { id: reservationId },
      data: { status: ReservationStatus.CANCELLED_BY_USER, depositStatus: DepositStatus.NONE },
    });
    this.emitZone(r.table.zoneId);
    this.log.log(`EXPIRE_HOLD ${reservationId}`);
  }

  private async reminder(reservationId: string) {
    const r = await this.prisma.reservation.findUnique({
      where: { id: reservationId },
      include: { user: true, branch: true, table: true },
    });
    if (!r || r.status !== ReservationStatus.CONFIRMED) return;
    const when = r.startAt.toLocaleTimeString('uz-UZ', {
      hour: '2-digit',
      minute: '2-digit',
    });
    const sent = await this.telegram.notifyUser(
      r.userId,
      [
        '⏰ <b>30 daqiqadan so\'ng broningiz boshlanadi</b>',
        '',
        `🏠 Filial: <b>${r.branch.name}</b>`,
        `📍 Manzil: ${r.branch.address}`,
        `🪑 Stol: <b>T-${r.table.number}</b>`,
        `👥 Mehmonlar: <b>${r.guestsCount}</b>`,
        `🕒 Vaqt: <b>${when}</b>`,
        '',
        '⚠️ 15 daqiqadan ortiq kechiksangiz, depozit qaytarilmaydi.',
      ].join('\n'),
    );
    if (!sent) {
      // fallback: SMS
      await this.producer.enqueueSms(
        r.user.phone,
        `Eslatma: ${r.branch.name} — broningiz 30 daqiqadan so'ng (${when}). Stol T-${r.table.number}.`,
      );
    }
    this.log.log(`REMINDER sent for ${reservationId} (30 min before)`);
  }

  /**
   * Мягкое напоминание гостю за 15 минут до конца «видимого» слота.
   * Снижает шанс что гость 1 засидится и помешает гостю 2.
   * Не выгоняет — просто предупреждает.
   */
  private async slotEndingSoon(reservationId: string) {
    const r = await this.prisma.reservation.findUnique({
      where: { id: reservationId },
      include: { user: true, branch: true, table: true },
    });
    if (!r) return;
    // Игнор если гость не пришёл / уже ушёл / отменил.
    if (r.status !== ReservationStatus.CONFIRMED) return;
    // Если бронь уже фактически прошла больше чем длительность — пропустим
    // (на случай если джоб задержался).
    const slotEndsAt = r.startAt.getTime() + RESERVATION_DURATION_MINUTES * 60 * 1000;
    if (Date.now() > slotEndsAt + 5 * 60 * 1000) return;

    const endTime = new Date(slotEndsAt).toLocaleTimeString('uz-UZ', {
      hour: '2-digit',
      minute: '2-digit',
    });
    await this.telegram.notifyUser(
      r.userId,
      [
        '⏳ <b>Sizning bron vaqtingiz tugashiga 15 daqiqa qoldi</b>',
        '',
        `🪑 Stol: <b>T-${r.table.number}</b>`,
        `🕒 Tugash vaqti: <b>${endTime}</b>`,
        '',
        '☕ Agar yana o\'tirmoqchi bo\'lsangiz, iltimos, ofitsiantga ayting —',
        'keyingi mehmon kelishi mumkin. Tushunganingiz uchun rahmat!',
      ].join('\n'),
    );
    this.log.log(`SLOT_ENDING_SOON sent for ${reservationId}`);
  }

  /**
   * Проверка «засиделся ли предыдущий гость» — за 3 мин после старта гостя 2.
   *
   * Срабатывает только если:
   *   - бронь гостя 2 ещё CONFIRMED (он не отменил, не помечен NO_SHOW)
   *   - на том же столе есть другая CONFIRMED-бронь, у которой endAt уже прошёл,
   *     но статус не сменён на COMPLETED (т.е. гость 1 ещё не выписан хостессом).
   *
   * Если совпало — гостю 2 в Telegram отправляются варианты свободных столов
   * того же филиала, чтобы он мог при желании пересесть. Админу — алерт.
   */
  private async overstayCheck(reservationId: string) {
    const r = await this.prisma.reservation.findUnique({
      where: { id: reservationId },
      include: {
        user: true,
        branch: true,
        table: { include: { zone: true } },
      },
    });
    if (!r) return;
    if (r.status !== ReservationStatus.CONFIRMED) return;

    const now = new Date();
    // Ищем «зависшую» предыдущую бронь на том же столе.
    const previous = await this.prisma.reservation.findFirst({
      where: {
        tableId: r.tableId,
        id: { not: r.id },
        status: ReservationStatus.CONFIRMED,
        endAt: { lte: now },
        startAt: { lt: r.startAt },
      },
      orderBy: { endAt: 'desc' },
    });
    if (!previous) {
      this.log.log(`OVERSTAY_CHECK ${reservationId} — clean, no overstay`);
      return;
    }

    // Подбираем альтернативы: тот же филиал, вмещает гостей, не на ремонте,
    // не пересекается со слотом гостя 2 (startAt..endAt), не равен текущему столу.
    const candidates = await this.prisma.restaurantTable.findMany({
      where: {
        zone: { branchId: r.branchId },
        id: { not: r.tableId },
        status: { not: 'MAINTENANCE' },
        seats: { gte: r.guestsCount },
      },
      include: {
        zone: { select: { name: true } },
        reservations: {
          where: {
            status: { in: [ReservationStatus.CONFIRMED, ReservationStatus.PENDING_PAYMENT] },
            AND: [{ startAt: { lt: r.endAt } }, { endAt: { gt: r.startAt } }],
          },
          select: { id: true },
        },
      },
      orderBy: [{ seats: 'asc' }, { number: 'asc' }],
      take: 30,
    });
    const free = candidates.filter((c) => c.reservations.length === 0).slice(0, 3);

    const startTime = r.startAt.toLocaleTimeString('uz-UZ', {
      hour: '2-digit',
      minute: '2-digit',
    });

    if (free.length === 0) {
      // Нет свободных — отправляем мягкое предупреждение «возможна задержка».
      await this.telegram.notifyUser(
        r.userId,
        [
          '⏰ <b>Stolingiz haqida ma\'lumot</b>',
          '',
          `Filial: <b>${r.branch.name}</b>`,
          `Stol: <b>T-${r.table.number}</b>`,
          `Vaqt: <b>${startTime}</b>`,
          '',
          '⚠️ Avvalgi mehmon hozircha stolni bo\'shatmadi.',
          'Iltimos, restoranga kelganda administrator bilan bog\'laning —',
          'sizga maqbul yechim topiladi.',
        ].join('\n'),
      );
    } else {
      const altLines = free
        .map((t, i) => `${i + 1}. <b>T-${t.number}</b> · ${t.seats} kishi · ${t.zone.name}`)
        .join('\n');
      await this.telegram.notifyUser(
        r.userId,
        [
          '⚠️ <b>Stolingiz hozircha band</b>',
          '',
          `Avvalgi mehmon <b>T-${r.table.number}</b> stolini hali bo\'shatmadi.`,
          'Quyidagi bo\'sh stollardan birini tanlasangiz, biz darhol o\'tkazib yuboramiz:',
          '',
          altLines,
          '',
          '👉 Restoranga kelganda administratorga aytsangiz kifoya — qo\'lda',
          'almashtiramiz. Yoki bir oz kuting — avvalgi mehmon ham hozir ketadi.',
        ].join('\n'),
      );
    }

    // Админам — детальный алерт.
    void this.telegram.notifyAdmins(
      [
        '🚨 <b>Overstay xavfi</b>',
        '',
        `Filial: <b>${r.branch.name}</b>`,
        `Stol: <b>T-${r.table.number}</b> (${r.table.zone.name})`,
        '',
        `Avvalgi bron: <code>${previous.id.slice(0, 8)}</code> · tugashi kerak edi: ${previous.endAt.toLocaleTimeString('uz-UZ', { hour: '2-digit', minute: '2-digit' })}`,
        `Yangi mehmon: ${r.user.name ?? r.user.phone} · ${r.guestsCount} kishi · ${startTime}`,
        '',
        free.length > 0
          ? `Bo'sh muqobil stollar: ${free.map((t) => 'T-' + t.number).join(', ')}`
          : '⚠️ Bo\'sh muqobil yo\'q — qo\'lda yechish kerak',
      ].join('\n'),
    );

    this.log.log(`OVERSTAY_CHECK ${reservationId} — alert sent (${free.length} alternatives)`);
  }

  private async noShow(reservationId: string) {
    const r = await this.prisma.reservation.findUnique({
      where: { id: reservationId },
      include: { table: { select: { zoneId: true } }, user: true },
    });
    if (!r || r.status !== ReservationStatus.CONFIRMED) return;
    const streak = r.user.noShowStreak + 1;
    await this.prisma.$transaction(async (tx) => {
      await tx.reservation.update({
        where: { id: reservationId },
        data: { status: ReservationStatus.NO_SHOW, depositStatus: DepositStatus.FORFEITED },
      });
      await tx.user.update({
        where: { id: r.userId },
        data: {
          noShowStreak: streak,
          ...(streak >= 3
            ? { bookingBlockedUntil: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000) }
            : {}),
        },
      });
    });
    this.emitZone(r.table.zoneId);
    const msg =
      `Kelmadingiz: bron no-show, depozit qaytarilmaydi. Ketma-ket: ${streak}.` +
      (streak >= 3 ? ' 7 kunga bron bloklandi.' : '');
    const sent = await this.telegram.notifyUser(r.userId, `⚠️ <b>No-show</b>\n\n${msg}`);
    if (!sent) {
      await this.producer.enqueueSms(r.user.phone, msg);
    }
    this.log.log(`NO_SHOW ${reservationId}`);
  }
}
