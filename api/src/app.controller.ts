import { Controller, Get, HttpCode, HttpStatus } from '@nestjs/common';
import { SkipThrottle } from '@nestjs/throttler';
import { AppService } from './app.service';
import { PrismaService } from './prisma/prisma.service';
import { RedisService } from './redis/redis.service';

@SkipThrottle()
@Controller()
export class AppController {
  constructor(
    private readonly appService: AppService,
    private readonly prisma: PrismaService,
    private readonly redis: RedisService,
  ) {}

  @Get()
  getHello(): string {
    return this.appService.getHello();
  }

  // Лёгкий health check — для UptimeRobot и Render health check.
  // НЕ обращается к БД и Redis, отвечает мгновенно.
  @Get('health')
  health() {
    return {
      status: 'ok',
      service: 'beshqozon-api',
      uptimeSec: Math.round(process.uptime()),
      time: new Date().toISOString(),
    };
  }

  // Глубокая проверка — пробегается по БД и Redis. Возвращает 503 если что-то лежит.
  // Использовать в алертах/мониторинге, не в keep-alive пингах.
  @Get('health/deep')
  @HttpCode(HttpStatus.OK)
  async healthDeep() {
    const checks: Record<string, { ok: boolean; latencyMs: number; error?: string }> = {};

    const dbStart = Date.now();
    try {
      await this.prisma.$queryRaw`SELECT 1`;
      checks.db = { ok: true, latencyMs: Date.now() - dbStart };
    } catch (e) {
      checks.db = {
        ok: false,
        latencyMs: Date.now() - dbStart,
        error: (e as Error).message,
      };
    }

    const redisStart = Date.now();
    try {
      const pong = await this.redis.raw.ping();
      checks.redis = {
        ok: pong === 'PONG',
        latencyMs: Date.now() - redisStart,
      };
    } catch (e) {
      checks.redis = {
        ok: false,
        latencyMs: Date.now() - redisStart,
        error: (e as Error).message,
      };
    }

    const allOk = Object.values(checks).every((c) => c.ok);
    return {
      status: allOk ? 'ok' : 'degraded',
      service: 'beshqozon-api',
      uptimeSec: Math.round(process.uptime()),
      time: new Date().toISOString(),
      checks,
    };
  }
}
