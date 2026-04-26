import {
  Body,
  Controller,
  Get,
  Header,
  Param,
  ParseUUIDPipe,
  Patch,
  Query,
  Res,
  UseGuards,
} from '@nestjs/common';
import type { Response } from 'express';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Roles } from '../auth/roles.decorator';
import { RolesGuard } from '../auth/roles.guard';
import { AdminUpdateReservationDto } from '../reservations/dto/admin-update-reservation.dto';
import { ReservationsService } from '../reservations/reservations.service';

@Controller('admin/reservations')
@UseGuards(JwtAuthGuard, RolesGuard)
@Roles('ADMIN')
export class AdminReservationsController {
  constructor(private readonly reservations: ReservationsService) {}

  @Get()
  list(@Query('branchId') branchId?: string, @Query('date') date?: string) {
    return this.reservations.adminList({ branchId, date });
  }

  @Get('export.csv')
  @Header('Content-Type', 'text/csv; charset=utf-8')
  async exportCsv(
    @Res() res: Response,
    @Query('branchId') branchId?: string,
    @Query('from') from?: string,
    @Query('to') to?: string,
    @Query('status') status?: string,
  ) {
    const rows = await this.reservations.adminExportCsv({ branchId, from, to, status });

    const header = [
      'id',
      'short',
      'created_at',
      'start_at',
      'status',
      'branch',
      'table',
      'zone',
      'guests',
      'deposit',
      'deposit_status',
      'customer_name',
      'phone',
      'telegram',
      'preorder_total',
    ];

    const lines = [header.join(',')];
    for (const r of rows) lines.push(r.map(csvCell).join(','));

    // BOM для корректного открытия в Excel
    const body = '\uFEFF' + lines.join('\r\n');

    const fname = `reservations-${new Date().toISOString().slice(0, 10)}.csv`;
    res.setHeader('Content-Disposition', `attachment; filename="${fname}"`);
    res.send(body);
  }

  @Patch(':id')
  setStatus(@Param('id', new ParseUUIDPipe()) id: string, @Body() dto: AdminUpdateReservationDto) {
    return this.reservations.adminSetStatus(id, dto.status);
  }
}

function csvCell(v: unknown): string {
  const s = v === null || v === undefined ? '' : String(v);
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
