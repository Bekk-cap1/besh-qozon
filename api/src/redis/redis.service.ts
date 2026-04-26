import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';

@Injectable()
export class RedisService implements OnModuleDestroy {
  private readonly client: Redis;

  constructor(config: ConfigService) {
    const url = config.get<string>('REDIS_URL');
    this.client = url
      ? new Redis(url, { maxRetriesPerRequest: null })
      : new Redis({
          host: config.get<string>('REDIS_HOST', '127.0.0.1'),
          port: +config.get<string>('REDIS_PORT', '6379'),
          maxRetriesPerRequest: null,
        });
  }

  onModuleDestroy() {
    void this.client.quit();
  }

  get raw(): Redis {
    return this.client;
  }

  async get(key: string) {
    return this.client.get(key);
  }

  async setex(key: string, seconds: number, value: string) {
    await this.client.setex(key, seconds, value);
  }

  async incrWithTtl(key: string, ttlSec: number): Promise<number> {
    const n = await this.client.incr(key);
    if (n === 1) await this.client.expire(key, ttlSec);
    return n;
  }

  async del(key: string) {
    await this.client.del(key);
  }
}
