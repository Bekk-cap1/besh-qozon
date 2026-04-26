import { Controller, Headers, HttpCode, Post, Req } from '@nestjs/common';
import type { RawBodyRequest } from '@nestjs/common/interfaces';
import { SkipThrottle } from '@nestjs/throttler';
import type { Request } from 'express';
import { PaymeMerchantService } from './payme-merchant.service';

@SkipThrottle()
@Controller('integrations/payme')
export class PaymeMerchantController {
  constructor(private readonly payme: PaymeMerchantService) {}

  @Post()
  @HttpCode(200)
  async rpc(@Req() req: RawBodyRequest<Request>, @Headers('authorization') authorization?: string) {
    const raw = req.rawBody?.toString('utf8') ?? JSON.stringify(req.body ?? {});
    return this.payme.handleRpc(raw, authorization);
  }
}
