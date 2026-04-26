import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { JwtService } from '@nestjs/jwt';
import { UserRole } from '@prisma/client';
import { randomBytes } from 'node:crypto';
import { PrismaService } from '../prisma/prisma.service';
import { RedisService } from '../redis/redis.service';

type PendingState =
  | { status: 'waiting' }
  | {
      status: 'started';
      telegramId: string;
      chatId: string;
      username: string | null;
      firstName: string | null;
    }
  | {
      status: 'linked';
      accessToken: string;
      user: { id: string; phone: string; name: string | null; role: UserRole };
    };

const TTL_SEC = 10 * 60; // 10 минут на логин

function keyFor(token: string) {
  return `tg:login:${token}`;
}

@Injectable()
export class TelegramAuthService {
  private readonly log = new Logger(TelegramAuthService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly redis: RedisService,
    private readonly jwt: JwtService,
    private readonly config: ConfigService,
  ) {}

  /** Создать короткий токен и вернуть deep-link. */
  async initLogin(botUsername: string) {
    const token = randomBytes(16).toString('base64url');
    const state: PendingState = { status: 'waiting' };
    await this.redis.setex(keyFor(token), TTL_SEC, JSON.stringify(state));
    const deepLink = `https://t.me/${botUsername}?start=${encodeURIComponent(token)}`;
    return { loginToken: token, deepLink, botUsername, ttl: TTL_SEC };
  }

  /** Состояние pending-логина (статус для веб-поллинга). */
  async getStatus(loginToken: string) {
    const raw = await this.redis.get(keyFor(loginToken));
    if (!raw) return { status: 'expired' as const };
    const st = JSON.parse(raw) as PendingState;
    if (st.status === 'linked') {
      // Вычитываем и удаляем — одноразово
      await this.redis.del(keyFor(loginToken));
      return { status: 'linked' as const, accessToken: st.accessToken, user: st.user };
    }
    if (st.status === 'started') {
      return { status: 'started' as const };
    }
    return { status: 'waiting' as const };
  }

  /** Зафиксировать, что бот принял /start <token> и знает юзера. */
  async touchStart(
    loginToken: string,
    data: {
      telegramId: string;
      chatId: string;
      username: string | null;
      firstName: string | null;
    },
  ): Promise<boolean> {
    const raw = await this.redis.get(keyFor(loginToken));
    if (!raw) return false;
    const state: PendingState = {
      status: 'started',
      telegramId: data.telegramId,
      chatId: data.chatId,
      username: data.username,
      firstName: data.firstName,
    };
    await this.redis.setex(keyFor(loginToken), TTL_SEC, JSON.stringify(state));
    return true;
  }

  /** Завершить логин после получения контакта (телефона) в телеге. */
  async linkByContact(data: {
    chatId: string;
    telegramId: string;
    username: string | null;
    firstName: string | null;
    phone: string;
  }): Promise<{ ok: boolean; reason?: string }> {
    // Найти pending-логин для этого chatId/telegramId
    // Для простоты ищем сканом (логинов мало); продакшн: хранить обратный индекс tg:chat:<id> -> token
    const clients = await this.findPendingByTelegramId(data.telegramId);
    if (!clients.length) {
      return {
        ok: false,
        reason: 'Saytdagi havola eskirgan. Iltimos, saytda qaytadan «Telegram orqali kirish»ni bosing.',
      };
    }

    // Сохранить/обновить пользователя
    const user = await this.prisma.user.upsert({
      where: { phone: data.phone },
      create: {
        phone: data.phone,
        name: data.firstName ?? null,
        smsVerified: true,
        role: UserRole.CUSTOMER,
        telegramId: data.telegramId,
        telegramChatId: data.chatId,
        telegramUsername: data.username ?? null,
        telegramFirstName: data.firstName ?? null,
        telegramLinkedAt: new Date(),
      },
      update: {
        smsVerified: true,
        telegramId: data.telegramId,
        telegramChatId: data.chatId,
        telegramUsername: data.username ?? null,
        telegramFirstName: data.firstName ?? null,
        telegramLinkedAt: new Date(),
      },
    });

    const accessToken = await this.jwt.signAsync({
      sub: user.id,
      role: user.role,
    });
    const linked: PendingState = {
      status: 'linked',
      accessToken,
      user: { id: user.id, phone: user.phone, name: user.name, role: user.role },
    };

    for (const token of clients) {
      await this.redis.setex(keyFor(token), TTL_SEC, JSON.stringify(linked));
    }
    this.log.log(`Telegram login linked: user=${user.id} phone=${data.phone}`);
    return { ok: true };
  }

  /** Поиск pending-токенов по telegramId (перебором ключей). Для low-load окей. */
  private async findPendingByTelegramId(telegramId: string): Promise<string[]> {
    const stream = this.redis.raw.scanStream({ match: 'tg:login:*', count: 100 });
    const matches: string[] = [];
    for await (const chunk of stream as AsyncIterable<string[]>) {
      for (const key of chunk) {
        const raw = await this.redis.raw.get(key);
        if (!raw) continue;
        try {
          const st = JSON.parse(raw) as PendingState;
          if (st.status === 'started' && st.telegramId === telegramId) {
            matches.push(key.replace(/^tg:login:/, ''));
          }
        } catch {
          /* skip */
        }
      }
    }
    return matches;
  }
}
