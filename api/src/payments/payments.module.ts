import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { SmsModule } from '../sms/sms.module';
import { ClickCheckoutService } from './click-checkout.service';
import { ClickWebhookController } from './click-webhook.controller';
import { PaymeCheckoutService } from './payme-checkout.service';
import { PaymeMerchantController } from './payme-merchant.controller';
import { PaymeMerchantService } from './payme-merchant.service';
import { PaymentsService } from './payments.service';

@Module({
  imports: [PrismaModule, SmsModule],
  controllers: [PaymeMerchantController, ClickWebhookController],
  providers: [PaymentsService, PaymeCheckoutService, ClickCheckoutService, PaymeMerchantService],
  exports: [PaymentsService],
})
export class PaymentsModule {}
