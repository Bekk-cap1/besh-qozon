import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

type TokenCache = { token: string; exp: number };

@Injectable()
export class EskizService {
  private readonly logger = new Logger(EskizService.name);
  private cache: TokenCache | null = null;

  constructor(private readonly config: ConfigService) {}

  private async fetchToken(): Promise<string> {
    const email = this.config.get<string>('ESKIZ_EMAIL');
    const password = this.config.get<string>('ESKIZ_PASSWORD');
    if (!email || !password) {
      throw new Error('ESKIZ_EMAIL/ESKIZ_PASSWORD sozlanmagan');
    }
    const res = await fetch('https://notify.eskiz.uz/api/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password }),
    });
    if (!res.ok) {
      const t = await res.text();
      throw new Error(`Eskiz auth: ${res.status} ${t}`);
    }
    const data = (await res.json()) as { data?: { token?: string } };
    const token = data?.data?.token;
    if (!token) throw new Error('Eskiz: token yo‘q');
    return token;
  }

  private async getToken(): Promise<string> {
    const now = Date.now();
    if (this.cache && this.cache.exp > now + 5000) return this.cache.token;
    const token = await this.fetchToken();
    this.cache = { token, exp: now + 50 * 60 * 1000 };
    return token;
  }

  /** TZ: Eskiz.uz — ishlamasa konsolga yozadi, devda SMS yuborilmaydi */
  async sendSms(phone: string, message: string): Promise<{ ok: boolean; skipped?: boolean }> {
    const email = this.config.get<string>('ESKIZ_EMAIL');
    if (!email) {
      this.logger.warn(`[SMS dev] ${phone}: ${message}`);
      return { ok: true, skipped: true };
    }
    const token = await this.getToken();
    const from = this.config.get<string>('ESKIZ_FROM', '4546');
    const res = await fetch('https://notify.eskiz.uz/api/message/sms/send', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({
        mobile_phone: phone.replace(/\s/g, ''),
        message,
        from,
      }),
    });
    if (!res.ok) {
      const t = await res.text();
      this.logger.error(`Eskiz send: ${res.status} ${t}`);
      return { ok: false };
    }
    return { ok: true };
  }
}
