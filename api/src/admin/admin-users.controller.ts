import {
  BadRequestException,
  Controller,
  Delete,
  Get,
  NotFoundException,
  Param,
  ParseUUIDPipe,
  Patch,
  Query,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Roles } from '../auth/roles.decorator';
import { RolesGuard } from '../auth/roles.guard';
import { PrismaService } from '../prisma/prisma.service';

const BLOCK_UNTIL = new Date('2099-01-01T00:00:00.000Z');

@Controller('admin/users')
@UseGuards(JwtAuthGuard, RolesGuard)
@Roles('ADMIN')
export class AdminUsersController {
  constructor(private readonly prisma: PrismaService) {}

  @Get()
  async list(
    @Query('q') q?: string,
    @Query('filter') filter?: string,
    @Query('page') pageRaw?: string,
    @Query('limit') limitRaw?: string,
  ) {
    const page = Math.max(1, Number(pageRaw) || 1);
    const limit = Math.max(1, Math.min(100, Number(limitRaw) || 30));
    const skip = (page - 1) * limit;
    const now = new Date();

    const where: Record<string, unknown> = { role: 'CUSTOMER' };

    if (q?.trim()) {
      where.OR = [
        { phone: { contains: q.trim() } },
        { name: { contains: q.trim(), mode: 'insensitive' } },
      ];
    }

    if (filter === 'blocked') {
      where.bookingBlockedUntil = { gt: now };
    }

    const [users, total] = await Promise.all([
      this.prisma.user.findMany({
        where,
        select: {
          id: true,
          phone: true,
          name: true,
          noShowStreak: true,
          bookingBlockedUntil: true,
          completedBookings: true,
          loyaltyBonuses: true,
          createdAt: true,
          telegramUsername: true,
          _count: { select: { reservations: true } },
        },
        orderBy: { createdAt: 'desc' },
        skip,
        take: limit,
      }),
      this.prisma.user.count({ where }),
    ]);

    return { users, total, page, limit };
  }

  @Patch(':id/block')
  async block(@Param('id', new ParseUUIDPipe()) id: string) {
    const user = await this.prisma.user.findUnique({ where: { id } });
    if (!user) throw new NotFoundException('Foydalanuvchi topilmadi');
    if (user.role === 'ADMIN') throw new BadRequestException('Admin bloklash mumkin emas');
    await this.prisma.user.update({
      where: { id },
      data: { bookingBlockedUntil: BLOCK_UNTIL },
    });
    return { ok: true };
  }

  @Patch(':id/unblock')
  async unblock(@Param('id', new ParseUUIDPipe()) id: string) {
    const user = await this.prisma.user.findUnique({ where: { id } });
    if (!user) throw new NotFoundException('Foydalanuvchi topilmadi');
    await this.prisma.user.update({
      where: { id },
      data: { bookingBlockedUntil: null, noShowStreak: 0 },
    });
    return { ok: true };
  }

  @Delete(':id')
  async remove(@Param('id', new ParseUUIDPipe()) id: string) {
    const user = await this.prisma.user.findUnique({ where: { id } });
    if (!user) throw new NotFoundException('Foydalanuvchi topilmadi');
    if (user.role === 'ADMIN') throw new BadRequestException('Admin o\'chirib bo\'lmaydi');
    await this.prisma.user.delete({ where: { id } });
    return { ok: true };
  }
}
