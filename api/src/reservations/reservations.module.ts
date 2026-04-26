import { Module } from '@nestjs/common';
import { AuthModule } from '../auth/auth.module';
import { PaymentsModule } from '../payments/payments.module';
import { ReservationsController } from './reservations.controller';
import { ReservationsService } from './reservations.service';
import { UserController } from './user.controller';

@Module({
  imports: [AuthModule, PaymentsModule],
  controllers: [ReservationsController, UserController],
  providers: [ReservationsService],
  exports: [ReservationsService],
})
export class ReservationsModule {}
