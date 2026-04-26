import {
  BadRequestException,
  HttpException,
  HttpStatus,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { JwtService } from '@nestjs/jwt';
import { UserRole } from '@prisma/client';
import { ReservationJobsProducer } from '../jobs/reservation-jobs.producer';
import { PrismaService } from '../prisma/prisma.service';
import { RedisService } from '../redis/redis.service';

type OtpEntry = { code: string; expiresAt: number };

@Injectable()
export class AuthService {
  private readonly otpStore = new Map<string, OtpEntry>();

  constructor(
    private readonly prisma: PrismaService,
    private readonly jwt: JwtService,
    private readonly config: ConfigService,
    private readonly redis: RedisService,
    private readonly jobs: ReservationJobsProducer,
  ) {}

  async requestOtp(phone: string) {
    const devBypass = this.config.get<string>('DEV_OTP');
    const code =
      devBypass && devBypass.length >= 4
        ? devBypass
        : String(Math.floor(100000 + Math.random() * 900000));
    this.otpStore.set(phone, {
      code,
      expiresAt: Date.now() + 5 * 60 * 1000,
    });
    await this.redis.del(`otp:fail:${phone}`);
    const text = `Beshqozon: tasdiqlash kodi ${code}. Hech kimga bermang.`;
    await this.jobs.enqueueSms(phone, text);
    const isProd = this.config.get<string>('NODE_ENV') === 'production';
    return {
      ok: true,
      message: 'SMS navbatga qo‘yildi (Eskiz sozlangan bo‘lsa yuboriladi).',
      ...(!isProd ? { devCode: code } : {}),
    };
  }

  async verifyOtp(phone: string, code: string, name?: string) {
    const blockKey = `otp:block:${phone}`;
    if (await this.redis.get(blockKey)) {
      throw new HttpException('3 marta noto‘g‘ri urinish — 5 daqiqa kuting (TZ).', HttpStatus.TOO_MANY_REQUESTS);
    }
    const entry = this.otpStore.get(phone);
    if (!entry || entry.expiresAt < Date.now()) {
      throw new UnauthorizedException('Kod muddati o‘tgan yoki topilmadi');
    }
    if (entry.code !== code) {
      const n = await this.redis.incrWithTtl(`otp:fail:${phone}`, 300);
      if (n >= 3) {
        await this.redis.setex(blockKey, 300, '1');
        throw new HttpException('3 marta noto‘g‘ri urinish — 5 daqiqa kuting (TZ).', HttpStatus.TOO_MANY_REQUESTS);
      }
      throw new UnauthorizedException('Noto‘g‘ri kod');
    }
    this.otpStore.delete(phone);
    await this.redis.del(`otp:fail:${phone}`);

    const user = await this.prisma.user.upsert({
      where: { phone },
      create: { phone, name: name ?? null, smsVerified: true, role: UserRole.CUSTOMER },
      update: { name: name ?? undefined, smsVerified: true },
    });

    const token = await this.jwt.signAsync({
      sub: user.id,
      role: user.role,
    });

    return {
      accessToken: token,
      user: { id: user.id, phone: user.phone, name: user.name, role: user.role },
    };
  }

  async devAdminToken(phone: string) {
    if (this.config.get<string>("ALLOW_DEV_ADMIN") !== "true") {
      throw new BadRequestException("Faqat ALLOW_DEV_ADMIN=true bo’lganda");
    }
    const user = await this.prisma.user.findUnique({ where: { phone } });
    if (!user || user.role !== UserRole.ADMIN) {
      throw new UnauthorizedException("Admin foydalanuvchi topilmadi");
    }
    const accessToken = await this.jwt.signAsync({
      sub: user.id,
      role: user.role,
    });
    return { accessToken, user: { id: user.id, phone: user.phone, role: user.role } };
  }

  async adminLogin(login: string, password: string) {
    const cfgLogin = this.config.get<string>("ADMIN_LOGIN");
    const cfgPassword = this.config.get<string>("ADMIN_PASSWORD");

    if (!cfgLogin || !cfgPassword) {
      throw new BadRequestException(".env da ADMIN_LOGIN va ADMIN_PASSWORD sozlanmagan");
    }
    if (login !== cfgLogin || password !== cfgPassword) {
      throw new UnauthorizedException("Login yoki parol noto\’g\’ri");
    }

    const user = await this.prisma.user.findFirst({ where: { role: UserRole.ADMIN } });
    if (!user) {
      throw new UnauthorizedException("Admin foydalanuvchi bazada topilmadi. Seed ni ishlatib ko\’ring.");
    }

    const accessToken = await this.jwt.signAsync({ sub: user.id, role: user.role });
    return { accessToken, user: { id: user.id, phone: user.phone, role: user.role } };
  }
}
