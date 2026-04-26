import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class ZonesService {
  constructor(private readonly prisma: PrismaService) {}

  async listByBranch(branchId: string) {
    const b = await this.prisma.branch.findFirst({ where: { id: branchId, isActive: true } });
    if (!b) throw new NotFoundException('Filial topilmadi');
    const zones = await this.prisma.zone.findMany({
      where: { branchId },
      orderBy: [{ floor: 'asc' }, { name: 'asc' }],
      include: { _count: { select: { tables: true } } },
    });

    const availCounts = await this.prisma.restaurantTable.groupBy({
      by: ['zoneId'],
      where: { status: 'AVAILABLE', zone: { branchId } },
      _count: { id: true },
    });
    const availMap = new Map(
      availCounts.map((x) => [x.zoneId, x._count.id] as const),
    );

    return zones.map((z) => ({
      ...z,
      availableTables: availMap.get(z.id) ?? 0,
    }));
  }

  async assertZoneInBranch(zoneId: string, branchId: string) {
    const z = await this.prisma.zone.findFirst({ where: { id: zoneId, branchId } });
    if (!z) throw new NotFoundException('Zona topilmadi');
    return z;
  }
}
