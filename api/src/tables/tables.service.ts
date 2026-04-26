import { Injectable, NotFoundException } from '@nestjs/common';
import { TableStatus } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { RESERVATION_DURATION_MINUTES } from '../constants/booking';
import { combineDateAndTime } from '../utils/slots';
import { ReservationsService } from '../reservations/reservations.service';

export type TableSlotView = {
  id: string;
  zoneId: string;
  number: string;
  seats: number;
  type: string;
  shape: string | null;
  xPos: number;
  yPos: number;
  maintenance: boolean;
  slotState: 'free' | 'held' | 'booked' | 'maintenance';
};

@Injectable()
export class TablesService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly reservations: ReservationsService,
  ) {}

  async listForSlot(zoneId: string, date: string, time: string) {
    const zone = await this.prisma.zone.findUnique({ where: { id: zoneId } });
    if (!zone) throw new NotFoundException('Zona topilmadi');

    const startAt = combineDateAndTime(date, time);
    const endAt = new Date(startAt.getTime() + RESERVATION_DURATION_MINUTES * 60 * 1000);

    const tables = await this.prisma.restaurantTable.findMany({
      where: { zoneId },
      orderBy: { number: 'asc' },
    });

    const blocked = await this.reservations.blockedTableStates(
      tables.map((t) => t.id),
      startAt,
      endAt,
    );

    const out: TableSlotView[] = tables.map((t) => {
      const maintenance = t.status === TableStatus.MAINTENANCE;
      let slotState: TableSlotView['slotState'] = 'free';
      if (!maintenance) {
        const b = blocked.get(t.id);
        if (b === 'booked') slotState = 'booked';
        else if (b === 'held') slotState = 'held';
      } else {
        slotState = 'maintenance';
      }
      return {
        id: t.id,
        zoneId: t.zoneId,
        number: t.number,
        seats: t.seats,
        type: t.type,
        shape: t.shape,
        xPos: t.xPos,
        yPos: t.yPos,
        maintenance,
        slotState,
      };
    });

    return {
      zoneId,
      date,
      time,
      startAt: startAt.toISOString(),
      endAt: endAt.toISOString(),
      tables: out,
    };
  }
}
