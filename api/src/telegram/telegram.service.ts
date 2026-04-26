import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import type { TgReplyMarkup, TgUpdate } from './telegram.types';
import { TelegramAuthService } from './telegram-auth.service';

/**
 * Минимальный Telegram-бот на чистом HTTP (без внешних зависимостей).
 * Поддерживает:
 *  - long-polling (getUpdates)
 *  - sendMessage / sendChatAction
 *  - обработку /start <loginToken> и shared-contact
 */
@Injectable()
export class TelegramService implements OnModuleInit, OnModuleDestroy {
  private readonly log = new Logger(TelegramService.name);
  private readonly token: string;
  private readonly enabled: boolean;
  private readonly baseUrl: string;
  private offset = 0;
  private polling = false;
  private aborter: AbortController | null = null;
  private pollTimer: NodeJS.Timeout | null = null;

  constructor(
    private readonly config: ConfigService,
    private readonly auth: TelegramAuthService,
  ) {
    this.token = this.config.get<string>('TELEGRAM_BOT_TOKEN') ?? '';
    this.enabled =
      this.config.get<string>('TELEGRAM_ENABLED') === 'true' && this.token.length > 0;
    this.baseUrl = `https://api.telegram.org/bot${this.token}`;
  }

  get isEnabled(): boolean {
    return this.enabled;
  }

  get botUsername(): string {
    return (this.config.get<string>('TELEGRAM_BOT_USERNAME') ?? 'BeshqozonBot').replace(
      /^@/,
      '',
    );
  }

  onModuleInit() {
    if (!this.enabled) {
      this.log.warn(
        'Telegram disabled — TELEGRAM_ENABLED / TELEGRAM_BOT_TOKEN not set. Skip polling.',
      );
      return;
    }
    this.log.log(`Telegram bot @${this.botUsername} polling started.`);
    void this.poll();
  }

  onModuleDestroy() {
    this.polling = false;
    if (this.aborter) this.aborter.abort();
    if (this.pollTimer) clearTimeout(this.pollTimer);
  }

  // ----- Public API -----

  /** Отправить текстовое сообщение в chat_id (silently skip если бот отключён). */
  async sendMessage(
    chatId: string | number,
    text: string,
    markup?: TgReplyMarkup,
  ): Promise<void> {
    if (!this.enabled) return;
    try {
      await this.request('sendMessage', {
        chat_id: chatId,
        text,
        parse_mode: 'HTML',
        disable_web_page_preview: true,
        ...(markup ? { reply_markup: markup } : {}),
      });
    } catch (e) {
      this.log.warn(`sendMessage failed: ${(e as Error).message}`);
    }
  }

  /** Отправить фото по URL (для QR-кода используем внешний сервис генерации). */
  async sendPhoto(
    chatId: string | number,
    photoUrl: string,
    caption?: string,
  ): Promise<void> {
    if (!this.enabled) return;
    try {
      await this.request('sendPhoto', {
        chat_id: chatId,
        photo: photoUrl,
        ...(caption ? { caption, parse_mode: 'HTML' } : {}),
      });
    } catch (e) {
      this.log.warn(`sendPhoto failed: ${(e as Error).message}`);
    }
  }

  // ----- Polling -----

  private async poll() {
    if (!this.enabled) return;
    this.polling = true;
    while (this.polling) {
      try {
        this.aborter = new AbortController();
        const updates = await this.request<TgUpdate[]>(
          'getUpdates',
          { offset: this.offset, timeout: 25, allowed_updates: ['message'] },
          this.aborter.signal,
          30_000,
        );
        for (const u of updates ?? []) {
          this.offset = u.update_id + 1;
          try {
            await this.handleUpdate(u);
          } catch (e) {
            this.log.error(`handleUpdate error: ${(e as Error).message}`);
          }
        }
      } catch (e) {
        const msg = (e as Error).message;
        if (!/aborted/i.test(msg)) {
          this.log.warn(`Polling error: ${msg}. Retry in 5s.`);
          await new Promise<void>((r) => {
            this.pollTimer = setTimeout(() => r(), 5_000);
          });
        }
      }
    }
  }

  private async handleUpdate(u: TgUpdate) {
    const msg = u.message;
    if (!msg) return;
    const chatId = String(msg.chat.id);
    const from = msg.from;

    // /start <token>
    if (msg.text && msg.text.startsWith('/start')) {
      const parts = msg.text.trim().split(/\s+/);
      const loginToken = parts[1];

      if (!loginToken) {
        await this.sendMessage(
          chatId,
          [
            `Assalomu alaykum${from?.first_name ? ', ' + escape(from.first_name) : ''}! 👋`,
            '',
            'Men — <b>Beshqozon</b> bot.',
            '',
            'Saytga kirish uchun: <a href="https://beshqozon.uz/">beshqozon.uz</a> ga kiring va «Telegram orqali kirish» tugmasini bosing.',
          ].join('\n'),
        );
        return;
      }

      // Shared contact кнопка для линковки
      const exists = await this.auth.touchStart(loginToken, {
        telegramId: String(from?.id ?? chatId),
        chatId,
        username: from?.username ?? null,
        firstName: from?.first_name ?? null,
      });

      if (!exists) {
        await this.sendMessage(
          chatId,
          'Havola eskirgan yoki noto\'g\'ri. Iltimos, saytda qayta «Telegram orqali kirish»ni bosing.',
        );
        return;
      }

      await this.sendMessage(
        chatId,
        [
          `Salom${from?.first_name ? ', ' + escape(from.first_name) : ''}! 🌟`,
          '',
          'Saytga kirish uchun <b>telefon raqamingizni</b> yuboring:',
          'Pastdagi <b>«📱 Raqamni yuborish»</b> tugmasini bosing.',
        ].join('\n'),
        {
          keyboard: [[{ text: '📱 Raqamni yuborish', request_contact: true }]],
          resize_keyboard: true,
          one_time_keyboard: true,
        },
      );
      return;
    }

    // Ответ с контактом = завершение логина
    if (msg.contact) {
      const phone = normalizePhone(msg.contact.phone_number);
      if (!phone) {
        await this.sendMessage(chatId, 'Raqam noto\'g\'ri, qayta urinib ko\'ring.');
        return;
      }

      const result = await this.auth.linkByContact({
        chatId,
        telegramId: String(from?.id ?? chatId),
        username: from?.username ?? null,
        firstName: from?.first_name ?? null,
        phone,
      });

      if (!result.ok) {
        await this.sendMessage(
          chatId,
          result.reason ?? 'Kirish muvaffaqiyatsiz. Saytda qayta urinib ko\'ring.',
          { remove_keyboard: true },
        );
        return;
      }

      await this.sendMessage(
        chatId,
        [
          '✅ <b>Muvaffaqiyatli kirildi!</b>',
          '',
          'Endi saytga qayting — avtomatik tizimga kirasiz.',
          '',
          'Bu yerga bronlaringiz bo\'yicha eslatmalar keladi. Savollar bo\'lsa, administratorga yozing.',
        ].join('\n'),
        { remove_keyboard: true },
      );
      return;
    }

    // Fallback
    await this.sendMessage(
      chatId,
      'Saytga kirish uchun <a href="https://beshqozon.uz/">beshqozon.uz</a> ga kiring va «Telegram orqali kirish» tugmasini bosing.',
    );
  }

  // ----- HTTP -----

  private async request<T>(
    method: string,
    body: Record<string, unknown>,
    signal?: AbortSignal,
    timeoutMs = 30_000,
  ): Promise<T> {
    const res = await fetch(`${this.baseUrl}/${method}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal: signal ?? AbortSignal.timeout(timeoutMs),
    });
    const json = (await res.json()) as { ok: boolean; result?: T; description?: string };
    if (!json.ok) {
      throw new Error(json.description || `Telegram API ${method} failed`);
    }
    return json.result as T;
  }
}

function escape(s: string): string {
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function normalizePhone(raw: string): string | null {
  const digits = raw.replace(/[^\d+]/g, '');
  if (!digits) return null;
  return digits.startsWith('+') ? digits : '+' + digits;
}
