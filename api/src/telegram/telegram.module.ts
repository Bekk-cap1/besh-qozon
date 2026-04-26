import { Global, Module } from '@nestjs/common';
import { AuthModule } from '../auth/auth.module';
import { PrismaModule } from '../prisma/prisma.module';
import { TelegramAuthService } from './telegram-auth.service';
import { TelegramController } from './telegram.controller';
import { TelegramNotifyService } from './telegram-notify.service';
import { TelegramService } from './telegram.service';

@Global()
@Module({
  imports: [PrismaModule, AuthModule],
  providers: [TelegramService, TelegramAuthService, TelegramNotifyService],
  controllers: [TelegramController],
  exports: [TelegramService, TelegramNotifyService],
})
export class TelegramModule {}
