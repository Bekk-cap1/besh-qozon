import { Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { TelegramService } from './telegram.service';

/**
 * Отправка уведомлений пользователю по его привязанному Telegram chat_id.
 * Если Telegram отключен или юзер не привязан — возвращает false (вызывающий код обычно шлёт SMS).
 */
@Injectable()
export class TelegramNotifyService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly tg: TelegramService,
  ) {}

  get isEnabled(): boolean {
    return this.tg.isEnabled;
  }

  async notifyUser(userId: string, text: string): Promise<boolean> {
    if (!this.tg.isEnabled) return false;
    const user = await this.prisma.user.findUnique({
      where: { id: userId },
      select: { telegramChatId: true },
    });
    if (!user?.telegramChatId) return false;
    await this.tg.sendMessage(user.telegramChatId, text);
    return true;
  }

  async notifyPhone(phone: string, text: string): Promise<boolean> {
    if (!this.tg.isEnabled) return false;
    const user = await this.prisma.user.findUnique({
      where: { phone },
      select: { telegramChatId: true },
    });
    if (!user?.telegramChatId) return false;
    await this.tg.sendMessage(user.telegramChatId, text);
    return true;
  }

  async sendQR(userId: string, payload: string, caption?: string): Promise<boolean> {
    if (!this.tg.isEnabled) return false;
    const user = await this.prisma.user.findUnique({
      where: { id: userId },
      select: { telegramChatId: true },
    });
    if (!user?.telegramChatId) return false;
    const qrUrl = `https://api.qrserver.com/v1/create-qr-code/?size=600x600&margin=16&data=${encodeURIComponent(payload)}`;
    await this.tg.sendPhoto(user.telegramChatId, qrUrl, caption);
    return true;
  }

  /**
   * Рассылка во все Telegram-чаты, связанные с администраторами.
   * Используется для уведомлений о новых бронях, no-show и отменах.
   */
  async notifyAdmins(text: string): Promise<number> {
    if (!this.tg.isEnabled) return 0;
    const admins = await this.prisma.user.findMany({
      where: { role: 'ADMIN', telegramChatId: { not: null } },
      select: { telegramChatId: true },
    });
    let sent = 0;
    for (const a of admins) {
      if (!a.telegramChatId) continue;
      try {
        await this.tg.sendMessage(a.telegramChatId, text);
        sent += 1;
      } catch {
        /* ignore individual send errors */
      }
    }
    return sent;
  }
}
