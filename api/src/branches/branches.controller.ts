import { Controller, Get, Param, ParseUUIDPipe, Query } from '@nestjs/common';
import { BranchesService } from './branches.service';

@Controller('branches')
export class BranchesController {
  constructor(private readonly branchesService: BranchesService) {}

  @Get()
  findAll() {
    return this.branchesService.findAll();
  }

  @Get(':id/slots')
  slots(@Param('id', new ParseUUIDPipe()) id: string, @Query('date') date: string) {
    if (!date) return { slots: [], message: 'date=YYYY-MM-DD majburiy' };
    return this.branchesService.slotsForDate(id, date);
  }

  @Get(':id/day-load')
  dayLoad(
    @Param('id', new ParseUUIDPipe()) id: string,
    @Query('days') days: string,
  ) {
    return this.branchesService.dayLoad(id, Number(days) || 15);
  }

  @Get(':id/slot-load')
  slotLoad(
    @Param('id', new ParseUUIDPipe()) id: string,
    @Query('date') date: string,
    @Query('guests') guests: string,
  ) {
    if (!date) return { slots: [], message: 'date=YYYY-MM-DD majburiy' };
    return this.branchesService.slotLoad(id, date, Number(guests) || 1);
  }

  @Get(':id')
  findOne(@Param('id', new ParseUUIDPipe()) id: string) {
    return this.branchesService.findOne(id);
  }
}
