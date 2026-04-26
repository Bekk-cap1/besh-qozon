import { Body, ConflictException, Controller, Delete, Get, NotFoundException, Param, ParseUUIDPipe, Patch, Post, Put, UseGuards } from '@nestjs/common';
import { Prisma, TableStatus } from '@prisma/client';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { IsBoolean, IsNumber, IsOptional, IsString, Length } from 'class-validator';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Roles } from '../auth/roles.decorator';
import { RolesGuard } from '../auth/roles.guard';
import { PrismaService } from '../prisma/prisma.service';
import { BulkTablesDto } from './dto/bulk-tables.dto';
import { CreateTableDto } from './dto/create-table.dto';
import { CreateZoneDto } from './dto/create-zone.dto';
import { UpdateFloorPlanDto } from './dto/update-floor-plan.dto';
import { UpdateFloorConfigDto } from './dto/update-floor-config.dto';
import { UpdateTableDto } from './dto/update-table.dto';

class UpdateBranchDto {
  @IsOptional()
  @IsString()
  @Length(1, 120)
  name?: string;

  @IsOptional()
  @IsString()
  @Length(0, 240)
  address?: string;

  @IsOptional()
  @IsNumber()
  lat?: number | null;

  @IsOptional()
  @IsNumber()
  lng?: number | null;

  @IsOptional()
  @IsString()
  workHours?: string;

  @IsOptional()
  @IsBoolean()
  isActive?: boolean;
}

@Controller('admin')
@UseGuards(JwtAuthGuard, RolesGuard)
@Roles('ADMIN')
export class AdminCatalogController {
  constructor(
    private readonly prisma: PrismaService,
    private readonly events: EventEmitter2,
  ) {}

  @Patch('branches/:id')
  async updateBranch(
    @Param('id', new ParseUUIDPipe()) id: string,
    @Body() dto: UpdateBranchDto,
  ) {
    const exists = await this.prisma.branch.findUnique({ where: { id } });
    if (!exists) throw new NotFoundException('Filial topilmadi');
    const data: Prisma.BranchUpdateInput = {};
    if (dto.name !== undefined) data.name = dto.name;
    if (dto.address !== undefined) data.address = dto.address;
    if (dto.lat !== undefined) data.lat = dto.lat;
    if (dto.lng !== undefined) data.lng = dto.lng;
    if (dto.workHours !== undefined) data.workHours = dto.workHours;
    if (dto.isActive !== undefined) data.isActive = dto.isActive;
    return this.prisma.branch.update({ where: { id }, data });
  }

  @Post('zones')
  createZone(@Body() dto: CreateZoneDto) {
    return this.prisma.zone.create({ data: dto });
  }

  @Get('zones/:zoneId/tables')
  listZoneTables(@Param('zoneId', new ParseUUIDPipe()) zoneId: string) {
    return this.prisma.restaurantTable.findMany({
      where: { zoneId },
      orderBy: { number: 'asc' },
    });
  }

  @Post('zones/:zoneId/tables/bulk')
  async bulkTables(@Param('zoneId', new ParseUUIDPipe()) zoneId: string, @Body() dto: BulkTablesDto) {
    const zone = await this.prisma.zone.findUnique({ where: { id: zoneId } });
    if (!zone) throw new NotFoundException('Zona topilmadi');

    // Защита от дубликатов: смотрим максимальный индекс среди уже существующих
    // столов с таким же префиксом и начинаем нумерацию со следующего.
    const existing = await this.prisma.restaurantTable.findMany({
      where: {
        zoneId,
        number: { startsWith: `${dto.prefix}-` },
      },
      select: { number: true },
    });
    const usedNumbers = new Set(existing.map((t) => t.number));
    let nextIndex = 1;
    for (const n of existing) {
      const match = /-(\d+)$/.exec(n.number);
      if (match) {
        const v = parseInt(match[1], 10);
        if (!Number.isNaN(v) && v >= nextIndex) nextIndex = v + 1;
      }
    }

    const startX = dto.startX ?? 48;
    const startY = dto.startY ?? 56;
    const stepX = dto.stepX ?? 104;
    const rowHeight = dto.rowHeight ?? 72;
    const cols = dto.columnsPerRow ?? Math.min(6, dto.count);
    const rows: Prisma.RestaurantTableCreateManyInput[] = [];
    for (let i = 0; i < dto.count; i++) {
      let number = `${dto.prefix}-${nextIndex + i}`;
      // на случай если вдруг появились разрывы и совпадение — подберём свободный
      while (usedNumbers.has(number)) {
        nextIndex += 1;
        number = `${dto.prefix}-${nextIndex + i}`;
      }
      usedNumbers.add(number);
      const col = i % cols;
      const row = Math.floor(i / cols);
      rows.push({
        zoneId,
        number,
        seats: dto.seatsPerTable,
        type: dto.type,
        xPos: startX + col * stepX,
        yPos: startY + row * rowHeight,
        shape: 'rect',
        status: TableStatus.AVAILABLE,
      });
    }
    await this.prisma.restaurantTable.createMany({ data: rows, skipDuplicates: true });
    this.events.emit('zone.refresh', zoneId);
    return { ok: true, created: rows.length, zoneId, startedFrom: nextIndex };
  }

  @Post('tables')
  async createTable(@Body() dto: CreateTableDto) {
    try {
      const t = await this.prisma.restaurantTable.create({
        data: {
          zoneId: dto.zoneId,
          number: dto.number,
          seats: dto.seats,
          type: dto.type,
          xPos: dto.xPos,
          yPos: dto.yPos,
          shape: dto.shape ?? null,
          status: dto.status ?? TableStatus.AVAILABLE,
        },
      });
      this.events.emit('zone.refresh', t.zoneId);
      return t;
    } catch (e) {
      if (e instanceof Prisma.PrismaClientKnownRequestError && e.code === 'P2002') {
        throw new ConflictException(
          `Bu zonada "${dto.number}" raqamli stol allaqachon mavjud`,
        );
      }
      throw e;
    }
  }

  @Put('tables/:id')
  async updateTable(@Param('id', new ParseUUIDPipe()) id: string, @Body() dto: UpdateTableDto) {
    const data: Record<string, unknown> = {};
    if (dto.zoneId !== undefined) data.zoneId = dto.zoneId;
    if (dto.number !== undefined) data.number = dto.number;
    if (dto.seats !== undefined) data.seats = dto.seats;
    if (dto.type !== undefined) data.type = dto.type;
    if (dto.xPos !== undefined) data.xPos = dto.xPos;
    if (dto.yPos !== undefined) data.yPos = dto.yPos;
    if (dto.shape !== undefined) data.shape = dto.shape;
    if (dto.status !== undefined) data.status = dto.status;
    if (!Object.keys(data).length) {
      return this.prisma.restaurantTable.findUniqueOrThrow({ where: { id } });
    }
    try {
      const t = await this.prisma.restaurantTable.update({
        where: { id },
        data: data as Prisma.RestaurantTableUpdateInput,
      });
      this.events.emit('zone.refresh', t.zoneId);
      return t;
    } catch (e) {
      if (e instanceof Prisma.PrismaClientKnownRequestError && e.code === 'P2002') {
        throw new ConflictException(
          `Bu zonada "${dto.number}" raqamli stol allaqachon mavjud`,
        );
      }
      throw e;
    }
  }

  @Delete('tables/:id')
  async deleteTable(@Param('id', new ParseUUIDPipe()) id: string) {
    const prev = await this.prisma.restaurantTable.findUnique({ where: { id } });
    await this.prisma.restaurantTable.delete({ where: { id } });
    if (prev) this.events.emit('zone.refresh', prev.zoneId);
    return { ok: true };
  }

  @Put('floor-plan/:zoneId')
  async updateFloorPlan(@Param('zoneId', new ParseUUIDPipe()) zoneId: string, @Body() dto: UpdateFloorPlanDto) {
    for (const row of dto.tables) {
      await this.prisma.restaurantTable.updateMany({
        where: { id: row.id, zoneId },
        data: { xPos: row.xPos, yPos: row.yPos },
      });
    }
    this.events.emit('zone.refresh', zoneId);
    return { ok: true, count: dto.tables.length };
  }

  @Patch('floor-plan/:zoneId/config')
  async updateFloorConfig(@Param('zoneId', new ParseUUIDPipe()) zoneId: string, @Body() dto: UpdateFloorConfigDto) {
    const zone = await this.prisma.zone.findUnique({ where: { id: zoneId } });
    if (!zone) throw new NotFoundException('Zona topilmadi');
    await this.prisma.zone.update({
      where: { id: zoneId },
      data: { floorPlanSvg: JSON.stringify({ doors: dto.doors ?? [], labels: dto.labels ?? [] }) },
    });
    return { ok: true };
  }

  @Get('floor-plan/:zoneId/config')
  async getFloorConfig(@Param('zoneId', new ParseUUIDPipe()) zoneId: string) {
    const zone = await this.prisma.zone.findUnique({ where: { id: zoneId }, select: { floorPlanSvg: true } });
    if (!zone) throw new NotFoundException('Zona topilmadi');
    try {
      return zone.floorPlanSvg ? JSON.parse(zone.floorPlanSvg) : { doors: [], labels: [] };
    } catch {
      return { doors: [], labels: [] };
    }
  }
}
