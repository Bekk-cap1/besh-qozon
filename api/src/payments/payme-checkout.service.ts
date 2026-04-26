import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class PaymeCheckoutService {
  constructor(private readonly config: ConfigService) {}

  /** https://developer.help.paycom.uz/initsializatsiya-platezhey/otpravka-cheka-po-metodu-get/ */
  buildCheckoutUrl(paymentId: string, amountUzs: number): { url: string; mock: boolean } | null {
    const merchantId = this.config.get<string>('PAYME_MERCHANT_ID');
    if (!merchantId) return null;
    const tiyin = Math.round(Number(amountUzs) * 100);
    let params = `m=${merchantId};ac.order_id=${paymentId};a=${tiyin}`;
    const ret = this.config.get<string>('PAYME_RETURN_URL');
    if (ret) params += `;c=${encodeURIComponent(ret)}`;
    const b64 = Buffer.from(params, 'utf8').toString('base64');
    return { url: `https://checkout.paycom.uz/${b64}`, mock: false };
  }
}
