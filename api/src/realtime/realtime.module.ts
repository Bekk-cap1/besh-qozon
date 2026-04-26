import { Module } from '@nestjs/common';
import { TablesGateway } from './tables.gateway';

@Module({
  providers: [TablesGateway],
})
export class RealtimeModule {}
