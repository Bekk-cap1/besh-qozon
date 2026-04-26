import { BadRequestException, Controller, Get, Query, Res } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { SkipThrottle } from '@nestjs/throttler';
import { createHash } from 'crypto';
import type { Response } from 'express';
import { PaymentsService } from './payments.service';

@SkipThrottle()
@Controller('integrations/click')
export class ClickWebhookController {
  constructor(
    private readonly payments: PaymentsService,
    private readonly config: ConfigService,
  ) {}

  /** Click `complete` redirect — query parametrlar Click hujjatiga mos */
  @Get('complete')
  async complete(
    @Query('click_trans_id') clickTransId: string,
    @Query('merchant_trans_id') merchantTransId: string,
    @Query('sign_string') signString: string,
    @Query('sign') sign: string,
    @Query('error') error: string | undefined,
    @Res() res: Response,
  ) {
    if (error && error !== '0') {
      return res.redirect(302, this.config.get('CLICK_RETURN_URL', 'http://localhost:3000/bron') + '?pay=fail');
    }
    const skip = this.config.get<string>('CLICK_SKIP_SIGN') === 'true';
    if (!skip) {
      const secret = this.config.get<string>('CLICK_SECRET_KEY');
      if (!secret || sign !== createHash('md5').update(signString + secret).digest('hex')) {
        throw new BadRequestException('Click imzo noto‘g‘ri');
      }
    }
    await this.payments.confirmPaymentSuccess(merchantTransId, clickTransId);
    return res.redirect(302, this.config.get('CLICK_RETURN_URL', 'http://localhost:3000/bron') + '?pay=ok');
  }
}
