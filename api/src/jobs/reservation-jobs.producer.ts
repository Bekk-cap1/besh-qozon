import { Injectable } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
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
    // Напоминание за 30 минут до начала брони
    const remindAt = startAt.getTime() - 30 * 60 * 1000;
    const remindDelay = Math.max(0, remindAt - now);
    await this.besh.add(
      'besh',
      { type: 'REMINDER_SMS', reservationId },
      { delay: remindDelay, jobId: `rem-${reservationId}`, removeOnComplete: true },
    );
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
    for (const id of [`hold-${reservationId}`, `rem-${reservationId}`, `ns-${reservationId}`]) {
      const job = await this.besh.getJob(id);
      if (job) await job.remove();
    }
  }
}
