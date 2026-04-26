import { Body, Controller, Get, Param, ParseUUIDPipe, Patch, UseGuards } from '@nestjs/common';
import { IsOptional, IsString, Length } from 'class-validator';
import { CurrentUser } from '../common/current-user.decorator';
import type { AuthUser } from '../common/current-user.decorator';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { ReservationsService } from './reservations.service';

class UpdateProfileDto {
  @IsOptional()
  @IsString()
  @Length(0, 80)
  name?: string;
}

@Controller('user')
@UseGuards(JwtAuthGuard)
export class UserController {
  constructor(private readonly reservations: ReservationsService) {}

  @Get('profile')
  profile(@CurrentUser() user: AuthUser) {
    return this.reservations.getProfile(user.userId);
  }

  @Patch('profile')
  updateProfile(@CurrentUser() user: AuthUser, @Body() dto: UpdateProfileDto) {
    return this.reservations.updateProfile(user.userId, dto);
  }

  @Get('reservations')
  list(@CurrentUser() user: AuthUser) {
    return this.reservations.listMine(user.userId);
  }

  @Get('reservations/:id')
  one(
    @CurrentUser() user: AuthUser,
    @Param('id', new ParseUUIDPipe()) id: string,
  ) {
    return this.reservations.getOneForUser(id, user.userId);
  }
}
