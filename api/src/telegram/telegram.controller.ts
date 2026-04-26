import { Body, Controller, Get, Post, Query } from '@nestjs/common';
import { IsString, Length } from 'class-validator';
import { TelegramAuthService } from './telegram-auth.service';
import { TelegramService } from './telegram.service';

class VerifyDto {
  @IsString()
  @Length(10, 128)
  loginToken!: string;
}

@Controller('auth/telegram')
export class TelegramController {
  constructor(
    private readonly tg: TelegramService,
    private readonly auth: TelegramAuthService,
  ) {}

  /** Инициировать логин — вернуть deep-link на бота. */
  @Post('init')
  async init() {
    if (!this.tg.isEnabled) {
      return {
        enabled: false,
        message: 'Telegram login is not configured. Set TELEGRAM_BOT_TOKEN and TELEGRAM_ENABLED=true.',
      };
    }
    const r = await this.auth.initLogin(this.tg.botUsername);
    return { enabled: true, ...r };
  }

  /** Поллинг статуса логина по loginToken. Возвращает JWT после успешной привязки. */
  @Get('status')
  async status(@Query('loginToken') loginToken?: string) {
    if (!loginToken) return { status: 'expired' };
    return this.auth.getStatus(loginToken);
  }

  /** Альтернатива GET (некоторые CSRF-настройки могут блокировать). */
  @Post('verify')
  verify(@Body() dto: VerifyDto) {
    return this.auth.getStatus(dto.loginToken);
  }
}
