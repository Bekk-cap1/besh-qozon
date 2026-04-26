import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { Job } from 'bullmq';
import { DepositStatus, ReservationStatus } from '@prisma/client';
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
