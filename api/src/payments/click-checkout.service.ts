import { createHash } from 'crypto';
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class ClickCheckoutService {
  constructor(private readonly config: ConfigService) {}

  /** Click merchant redirect (soddalashtirilgan MD5 imzo) */
  buildCheckoutUrl(paymentId: string, amountUzs: number): { url: string; mock: boolean } | null {
    const serviceId = this.config.get<string>('CLICK_SERVICE_ID');
    const merchantId = this.config.get<string>('CLICK_MERCHANT_ID');
    const secret = this.config.get<string>('CLICK_SECRET_KEY');
    if (!serviceId || !merchantId || !secret) return null;
    const amount = Math.round(Number(amountUzs) * 100);
    const returnUrl = encodeURIComponent(this.config.get<string>('CLICK_RETURN_URL', 'http://localhost:3000/bron'));
    const mUserId = this.config.get<string>('CLICK_MERCHANT_USER_ID', '1');
    const action = '0';
    const signString = `${amount}${action}${merchantId}${mUserId}${paymentId}${serviceId}${returnUrl}${secret}`;
    const sign = createHash('md5').update(signString).digest('hex');
    const url =
      `https://my.click.uz/services/pay` +
      `?service_id=${encodeURIComponent(serviceId)}` +
      `&merchant_id=${encodeURIComponent(merchantId)}` +
      `&amount=${amount}` +
      `&transaction_param=${encodeURIComponent(paymentId)}` +
      `&return_url=${returnUrl}` +
      `&merchant_user_id=${encodeURIComponent(mUserId)}` +
      `&action=${action}` +
      `&sign_string=${encodeURIComponent(signString)}` +
      `&sign=${sign}`;
    return { url, mock: false };
  }
}
