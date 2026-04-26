import { Controller, Get, Param, ParseUUIDPipe } from '@nestjs/common';
import { ZonesService } from './zones.service';

@Controller('branches/:branchId/zones')
export class ZonesController {
  constructor(private readonly zones: ZonesService) {}

  @Get()
  list(@Param('branchId', new ParseUUIDPipe()) branchId: string) {
    return this.zones.listByBranch(branchId);
  }
}
