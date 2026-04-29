import { Injectable } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import {
  RESERVATION_DURATION_MINUTES,
  SLOT_ENDING_REMINDER_MINUTES,
} from '../constants/booking';
import type { BeshJob, SmsJob } from './reservation-job.types';

@Injectable()
export class ReservationJobsProducer {
  constructor(
    @InjectQueue('besh') private readonly besh: Queue<BeshJob>,
    @InjectQueue('sms') private readonly sms: Queue<SmsJob>,
  ) {}

  async scheduleHoldExpiry(reservationId: string, runAt: Date) {
    const delay = Math.max(0, runAt.getTime() - Date.now());
    await this.besh.add('besh', { type: 'EXPIRE_HOLD', reservationId }, { delay, jobId: `hold-${reservationId}`, removeOnComplete: true });
  }

  async schedulePostConfirm(reservationId: string, startAt: Date) {
    const now = Date.now();
    // Напоминание за 30 минут до начала брони.
    const remindAt = startAt.getTime() - 30 * 60 * 1000;
    const remindDelay = Math.max(0, remindAt - now);
    await this.besh.add(
      'besh',
      { type: 'REMINDER_SMS', reservationId },
      { delay: remindDelay, jobId: `rem-${reservationId}`, removeOnComplete: true },
    );

    // Мягкое напоминание гостю за SLOT_ENDING_REMINDER_MINUTES до конца слота.
    // Например: бронь 16:00, слот 2 ч → конец видимый = 18:00 → пинг в 17:45.
    // Это снижает риск, что гость 1 «засидится» и помешает гостю 2.
    const slotEndAt = startAt.getTime() + RESERVATION_DURATION_MINUTES * 60 * 1000;
    const endingPingAt = slotEndAt - SLOT_ENDING_REMINDER_MINUTES * 60 * 1000;
    const endingDelay = Math.max(0, endingPingAt - now);
    await this.besh.add(
      'besh',
      { type: 'SLOT_ENDING_SOON', reservationId },
      { delay: endingDelay, jobId: `end-${reservationId}`, removeOnComplete: true },
    );

    // Проверка «засиделся ли гость 1» — через 3 мин после старта гостя 2.
    // Если на том же столе предыдущая бронь до сих пор CONFIRMED (хостес
    // не отметил COMPLETED) — гостю 2 отправляем варианты свободных столов.
    const overstayAt = startAt.getTime() + 3 * 60 * 1000;
    const overstayDelay = Math.max(0, overstayAt - now);
    await this.besh.add(
      'besh',
      { type: 'OVERSTAY_CHECK', reservationId },
      { delay: overstayDelay, jobId: `ovs-${reservationId}`, removeOnComplete: true },
    );

    // No-show: 15 минут после начала. Если status всё ещё CONFIRMED → no-show.
    const noShowAt = startAt.getTime() + 15 * 60 * 1000;
    const noShowDelay = Math.max(0, noShowAt - now);
    await this.besh.add(
      'besh',
      { type: 'NO_SHOW', reservationId },
      { delay: noShowDelay, jobId: `ns-${reservationId}`, removeOnComplete: true },
    );
  }

  async enqueueSms(phone: string, text: string) {
    await this.sms.add('sms', { phone, text }, { removeOnComplete: true });
  }

  async cancelScheduledForReservation(reservationId: string) {
    for (const id of [
      `hold-${reservationId}`,
      `rem-${reservationId}`,
      `end-${reservationId}`,
      `ovs-${reservationId}`,
      `ns-${reservationId}`,
    ]) {
      const job = await this.besh.getJob(id);
      if (job) await job.remove();
    }
  }
}
