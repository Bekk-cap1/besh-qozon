import { IsEnum } from 'class-validator';
import { ReservationStatus } from '@prisma/client';

export class AdminUpdateReservationDto {
  @IsEnum(ReservationStatus)
  status!: ReservationStatus;
}
