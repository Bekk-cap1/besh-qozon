import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { DepositStatus, PaymentStatus, ReservationStatus } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { ReservationJobsProducer } from '../jobs/reservation-jobs.producer';
import { TelegramNotifyService } from '../telegram/telegram-notify.service';
import { PaymeCheckoutService } from './payme-checkout.service';
import { ClickCheckoutService } from './click-checkout.service';

@Injectable()
export class PaymentsService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly events: EventEmitter2,
    private readonly jobs: ReservationJobsProducer,
    private readonly payme: PaymeCheckoutService,
    private readonly click: ClickCheckoutService,
    private readonly telegram: TelegramNotifyService,
  ) {}

  private emitZone(zoneId: string) {
    this.events.emit('zone.refresh', zoneId);
  }

  async createCheckout(reservationId: string, userId: string, provider: 'payme' | 'click') {
    const r = await this.prisma.reservation.findFirst({
      where: { id: reservationId, userId },
      include: { table: true },
    });
    if (!r) throw new NotFoundException('Bron topilmadi');
    if (r.status !== ReservationStatus.PENDING_PAYMENT) {
      throw new BadRequestException('To‘lov faqat kutilayotgan bronda');
    }
    if (r.holdExpiresAt && r.holdExpiresAt < new Date()) {
      throw new BadRequestException('To‘lov muddati tugagan');
    }

    await this.prisma.payment.deleteMany({
      where: { reservationId, status: PaymentStatus.PENDING },
    });

    const pay = await this.prisma.payment.create({
      data: {
        reservationId: r.id,
        userId,
        amount: r.depositAmount,
        method: provider.toUpperCase(),
        provider,
        status: PaymentStatus.PENDING,
      },
    });

    const amountNum = Number(r.depositAmount);
    const built =
      provider === 'payme'
        ? this.payme.buildCheckoutUrl(pay.id, amountNum)
        : this.click.buildCheckoutUrl(pay.id, amountNum);

    if (!built) {
      await this.prisma.payment.update({
        where: { id: pay.id },
        data: { checkoutUrl: null },
      });
      return {
        paymentId: pay.id,
        provider,
        checkoutUrl: null as string | null,
        message:
          provider === 'payme'
            ? 'PAYME_MERCHANT_ID sozlang yoki POST /api/reservations/:id/pay (mock) ishlating'
            : 'CLICK_* o‘zgaruvchilarni sozlang yoki mock to‘lov',
        mockFallback: true,
      };
    }

    await this.prisma.payment.update({
      where: { id: pay.id },
      data: { checkoutUrl: built.url },
    });

    return {
      paymentId: pay.id,
      provider,
      checkoutUrl: built.url,
      mockFallback: false,
    };
  }

  /** Payme merchant PerformTransaction yoki mock */
  async completeMockReservation(reservationId: string, userId: string) {
    const r = await this.prisma.reservation.findFirst({
      where: { id: reservationId, userId },
      include: { table: true },
    });
    if (!r) throw new NotFoundException('Bron topilmadi');
    if (r.status !== ReservationStatus.PENDING_PAYMENT) {
      throw new BadRequestException('To‘lov kutilmayapti');
    }
    if (r.holdExpiresAt && r.holdExpiresAt < new Date()) {
      throw new BadRequestException('To‘lov muddati tugagan');
    }
    const pay = await this.prisma.payment.create({
      data: {
        reservationId: r.id,
        userId,
        amount: r.depositAmount,
        method: 'PAYME_MOCK',
        provider: 'mock',
        status: PaymentStatus.PENDING,
      },
    });
    return this.confirmPaymentSuccess(pay.id, `mock_${Date.now()}`);
  }

  async confirmPaymentSuccess(paymentId: string, externalId: string) {
    const pay = await this.prisma.payment.findUnique({
      where: { id: paymentId },
      include: { reservation: { include: { table: true } } },
    });
    if (!pay) throw new NotFoundException('Payment topilmadi');
    if (pay.status === PaymentStatus.COMPLETED) {
      return this.prisma.reservation.findUnique({
        where: { id: pay.reservationId },
        include: {
          branch: { select: { id: true, name: true } },
          table: { select: { id: true, number: true, zoneId: true } },
          payments: true,
        },
      });
    }
    if (pay.reservation.status !== ReservationStatus.PENDING_PAYMENT) return pay.reservation;

    const res = await this.prisma.$transaction(async (tx) => {
      await tx.payment.update({
        where: { id: pay.id },
        data: {
          status: PaymentStatus.COMPLETED,
          transactionId: externalId,
        },
      });
      return tx.reservation.update({
        where: { id: pay.reservationId },
        data: {
          status: ReservationStatus.CONFIRMED,
          depositStatus: DepositStatus.PAID,
        },
        include: {
          branch: { select: { id: true, name: true } },
          table: { select: { id: true, number: true, zoneId: true } },
          payments: true,
        },
      });
    });

    await this.jobs.cancelScheduledForReservation(res.id);
    await this.jobs.schedulePostConfirm(res.id, res.startAt);
    this.emitZone(res.table.zoneId);

    // Отправляем QR + подтверждение в Telegram (если привязан)
    const shortCode = shortFromUuid(res.id);
    const when = res.startAt.toLocaleString('uz-UZ');
    void this.telegram.sendQR(
      res.userId,
      res.id,
      [
        '✅ <b>To\'lov muvaffaqiyatli</b>',
        '',
        `Bron kodi: <b>${shortCode}</b>`,
        `Filial: <b>${res.branch.name}</b>`,
        `Stol: <b>T-${res.table.number}</b>`,
        `Vaqt: <b>${when}</b>`,
        '',
        'Restoranda QR-kodni xodimga ko\'rsating.',
      ].join('\n'),
    );
    return res;
  }
}

/** Короткий человекочитаемый код из UUID: BQ-XXXXXX. */
function shortFromUuid(uuid: string): string {
  const clean = uuid.replace(/-/g, '').toUpperCase();
  // Берём первые 6 HEX-символов как базу
  return `BQ-${clean.slice(0, 6)}`;
}
