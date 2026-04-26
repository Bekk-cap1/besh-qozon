import { Controller, Get, Param, ParseUUIDPipe, Query } from '@nestjs/common';
import { TablesService } from './tables.service';

@Controller('zones/:zoneId/tables')
export class TablesController {
  constructor(private readonly tables: TablesService) {}

  @Get()
  listForSlot(
    @Param('zoneId', new ParseUUIDPipe()) zoneId: string,
    @Query('date') date: string,
    @Query('time') time: string,
  ) {
    if (!date || !time) {
      return { message: 'date=YYYY-MM-DD va time=HH:mm majburiy', tables: [] };
    }
    return this.tables.listForSlot(zoneId, date, time);
  }
}
