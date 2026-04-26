import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class MenuService {
  constructor(private readonly prisma: PrismaService) {}

  async listForBranch(branchId: string) {
    const items = await this.prisma.menuItem.findMany({
      where: { branchId, isActive: true },
      orderBy: [{ category: 'asc' }, { sortOrder: 'asc' }, { name: 'asc' }],
    });
    return items.map((m) => ({
      id: m.id,
      branchId: m.branchId,
      name: m.name,
      description: m.description,
      category: m.category,
      price: Number(m.price),
      imageUrl: m.imageUrl,
    }));
  }

  async adminListForBranch(branchId: string) {
    return this.prisma.menuItem.findMany({
      where: { branchId },
      orderBy: [{ category: 'asc' }, { sortOrder: 'asc' }, { name: 'asc' }],
    });
  }

  async create(dto: {
    branchId: string;
    name: string;
    description?: string;
    category?: string;
    price: number;
    imageUrl?: string;
    sortOrder?: number;
  }) {
    if (!dto.name?.trim()) throw new BadRequestException('Nomi bo\'sh');
    if (!Number.isFinite(dto.price) || dto.price < 0)
      throw new BadRequestException('Narx noto\'g\'ri');
    return this.prisma.menuItem.create({
      data: {
        branchId: dto.branchId,
        name: dto.name.trim(),
        description: dto.description ?? null,
        category: (dto.category ?? 'Asosiy').trim() || 'Asosiy',
        price: dto.price,
        imageUrl: dto.imageUrl ?? null,
        sortOrder: dto.sortOrder ?? 0,
      },
    });
  }

  async update(
    id: string,
    dto: Partial<{
      name: string;
      description: string | null;
      category: string;
      price: number;
      imageUrl: string | null;
      sortOrder: number;
      isActive: boolean;
    }>,
  ) {
    const exists = await this.prisma.menuItem.findUnique({ where: { id } });
    if (!exists) throw new NotFoundException('Menu item topilmadi');
    return this.prisma.menuItem.update({
      where: { id },
      data: {
        ...(dto.name !== undefined ? { name: dto.name.trim() } : {}),
        ...(dto.description !== undefined ? { description: dto.description } : {}),
        ...(dto.category !== undefined ? { category: dto.category } : {}),
        ...(dto.price !== undefined ? { price: dto.price } : {}),
        ...(dto.imageUrl !== undefined ? { imageUrl: dto.imageUrl } : {}),
        ...(dto.sortOrder !== undefined ? { sortOrder: dto.sortOrder } : {}),
        ...(dto.isActive !== undefined ? { isActive: dto.isActive } : {}),
      },
    });
  }

  async remove(id: string) {
    const exists = await this.prisma.menuItem.findUnique({ where: { id } });
    if (!exists) throw new NotFoundException('Menu item topilmadi');
    await this.prisma.menuItem.delete({ where: { id } });
    return { ok: true };
  }
}
