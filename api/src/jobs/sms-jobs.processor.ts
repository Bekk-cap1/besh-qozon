import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Job } from 'bullmq';
import { EskizService } from '../sms/eskiz.service';
import type { SmsJob } from './reservation-job.types';

@Processor('sms')
export class SmsJobsProcessor extends WorkerHost {
  constructor(private readonly eskiz: EskizService) {
    super();
  }

  async process(job: Job<SmsJob>): Promise<void> {
    await this.eskiz.sendSms(job.data.phone, job.data.text);
  }
}
