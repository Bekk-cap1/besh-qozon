import { Controller, Get, Param, ParseUUIDPipe } from '@nestjs/common';
import { MenuService } from './menu.service';

@Controller('branches/:branchId/menu')
export class MenuController {
  constructor(private readonly menu: MenuService) {}

  @Get()
  list(@Param('branchId', new ParseUUIDPipe()) branchId: string) {
    return this.menu.listForBranch(branchId);
  }
}
