import { Module } from '@nestjs/common';
import { AuthModule } from '../auth/auth.module';
import { MenuModule } from '../menu/menu.module';
import { PrismaModule } from '../prisma/prisma.module';
import { ReservationsModule } from '../reservations/reservations.module';
import { AdminCatalogController } from './admin-catalog.controller';
import { AdminMenuController } from './admin-menu.controller';
import { AdminReservationsController } from './admin-reservations.controller';
import { AdminStatsController } from './admin-stats.controller';
import { AdminUsersController } from './admin-users.controller';

@Module({
  imports: [AuthModule, PrismaModule, ReservationsModule, MenuModule],
  controllers: [
    AdminReservationsController,
    AdminCatalogController,
    AdminStatsController,
    AdminMenuController,
    AdminUsersController,
  ],
})
export class AdminModule {}
