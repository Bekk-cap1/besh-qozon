import { Module } from '@nestjs/common';
import { EskizService } from './eskiz.service';

@Module({
  providers: [EskizService],
  exports: [EskizService],
})
export class SmsModule {}
