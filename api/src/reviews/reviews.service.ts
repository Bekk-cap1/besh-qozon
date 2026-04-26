import {
  BadRequestException,
  ForbiddenException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class ReviewsService {
  constructor(private readonly prisma: PrismaService) {}

  async listForBranch(branchId: string) {
    const [items, agg] = await Promise.all([
      this.prisma.review.findMany({
        where: { branchId, isPublished: true },
        orderBy: { createdAt: 'desc' },
        take: 50,
        include: { user: { select: { name: true, phone: true } } },
      }),
      this.prisma.review.aggregate({
        where: { branchId, isPublished: true },
        _avg: { rating: true },
        _count: true,
      }),
    ]);
    return {
      count: agg._count,
      averageRating: agg._avg.rating ? Number(agg._avg.rating.toFixed(2)) : null,
      items: items.map((r) => ({
        id: r.id,
        rating: r.rating,
        comment: r.comment,
        createdAt: r.createdAt,
        userName: r.user?.name || maskPhone(r.user?.phone ?? ''),
      })),
    };
  }

  async listAllForAdmin(branchId?: string) {
    return this.prisma.review.findMany({
      where: branchId ? { branchId } : undefined,
      orderBy: { createdAt: 'desc' },
      take: 200,
      include: {
        user: { select: { name: true, phone: true } },
        branch: { select: { name: true } },
        reservation: { select: { id: true, startAt: true } },
      },
    });
  }

  async create(
    userId: string,
    dto: { reservationId: string; rating: number; comment?: string },
  ) {
    if (!Number.isInteger(dto.rating) || dto.rating < 1 || dto.rating > 5) {
      throw new BadRequestException('Reyting 1-5 oralig\'ida bo\'lishi kerak');
    }
    const res = await this.prisma.reservation.findUnique({
      where: { id: dto.reservationId },
      select: { id: true, userId: true, status: true, branchId: true },
    });
    if (!res) throw new NotFoundException('Bron topilmadi');
    if (res.userId !== userId) throw new ForbiddenException('Sizga tegishli emas');
    if (res.status !== 'COMPLETED') {
      throw new BadRequestException('Faqat yakunlangan bronlarga sharh qoldirish mumkin');
    }
    const existing = await this.prisma.review.findUnique({
      where: { reservationId: dto.reservationId },
    });
    if (existing) throw new BadRequestException('Sharh allaqachon qoldirilgan');

    return this.prisma.review.create({
      data: {
        branchId: res.branchId,
        userId,
        reservationId: dto.reservationId,
        rating: dto.rating,
        comment: (dto.comment ?? '').trim() || null,
      },
    });
  }

  async setPublished(id: string, isPublished: boolean) {
    const exists = await this.prisma.review.findUnique({ where: { id } });
    if (!exists) throw new NotFoundException('Sharh topilmadi');
    return this.prisma.review.update({
      where: { id },
      data: { isPublished },
    });
  }

  async remove(id: string) {
    const exists = await this.prisma.review.findUnique({ where: { id } });
    if (!exists) throw new NotFoundException('Sharh topilmadi');
    await this.prisma.review.delete({ where: { id } });
    return { ok: true };
  }

  async averageByBranch(branchIds: string[]) {
    if (!branchIds.length) return {} as Record<string, { avg: number; count: number }>;
    const rows = await this.prisma.review.groupBy({
      by: ['branchId'],
      where: { branchId: { in: branchIds }, isPublished: true },
      _avg: { rating: true },
      _count: true,
    });
    const out: Record<string, { avg: number; count: number }> = {};
    for (const r of rows) {
      out[r.branchId] = {
        avg: r._avg.rating ? Number(r._avg.rating.toFixed(2)) : 0,
        count: r._count,
      };
    }
    return out;
  }
}

function maskPhone(p: string): string {
  if (!p) return 'Mijoz';
  const digits = p.replace(/\D/g, '');
  if (digits.length < 6) return 'Mijoz';
  return `+${digits.slice(0, 3)}*****${digits.slice(-2)}`;
}
