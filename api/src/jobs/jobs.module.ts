import { BullModule } from '@nestjs/bullmq';
import { Global, Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import Redis from 'ioredis';
import { PrismaModule } from '../prisma/prisma.module';
import { SmsModule } from '../sms/sms.module';
import { ReservationJobsProcessor } from './reservation-jobs.processor';
import { ReservationJobsProducer } from './reservation-jobs.producer';
import { SmsJobsProcessor } from './sms-jobs.processor';

@Global()
@Module({
  imports: [
    ConfigModule,
    PrismaModule,
    SmsModule,
    BullModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (config: ConfigService) => {
        const url = config.get<string>('REDIS_URL');
        const conn = url
          ? new Redis(url, { maxRetriesPerRequest: null })
          : new Redis({
              host: config.get<string>('REDIS_HOST', '127.0.0.1'),
              port: +config.get<string>('REDIS_PORT', '6379'),
              maxRetriesPerRequest: null,
            });
        return { connection: conn };
      },
    }),
    BullModule.registerQueue({ name: 'besh' }, { name: 'sms' }),
  ],
  providers: [ReservationJobsProcessor, SmsJobsProcessor, ReservationJobsProducer],
  exports: [ReservationJobsProducer, BullModule],
})
export class JobsModule {}
