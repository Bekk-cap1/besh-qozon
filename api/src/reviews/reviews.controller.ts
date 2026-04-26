import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  ParseUUIDPipe,
  Patch,
  Post,
  Query,
  UseGuards,
} from '@nestjs/common';
import { IsInt, IsOptional, IsString, IsUUID, Length, Max, Min } from 'class-validator';
import { CurrentUser } from '../common/current-user.decorator';
import type { AuthUser } from '../common/current-user.decorator';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Roles } from '../auth/roles.decorator';
import { RolesGuard } from '../auth/roles.guard';
import { ReviewsService } from './reviews.service';

class CreateReviewDto {
  @IsUUID()
  reservationId!: string;

  @IsInt()
  @Min(1)
  @Max(5)
  rating!: number;

  @IsOptional()
  @IsString()
  @Length(0, 500)
  comment?: string;
}

// Публичный просмотр отзывов по филиалу.
@Controller('branches/:branchId/reviews')
export class BranchReviewsController {
  constructor(private readonly service: ReviewsService) {}

  @Get()
  list(@Param('branchId', new ParseUUIDPipe()) branchId: string) {
    return this.service.listForBranch(branchId);
  }
}

// Оставить отзыв (нужен логин).
@Controller('reviews')
@UseGuards(JwtAuthGuard)
export class UserReviewsController {
  constructor(private readonly service: ReviewsService) {}

  @Post()
  create(@CurrentUser() user: AuthUser, @Body() dto: CreateReviewDto) {
    return this.service.create(user.userId, dto);
  }
}

// Админский модерационный контроль.
@Controller('admin/reviews')
@UseGuards(JwtAuthGuard, RolesGuard)
@Roles('ADMIN')
export class AdminReviewsController {
  constructor(private readonly service: ReviewsService) {}

  @Get()
  list(@Query('branchId') branchId?: string) {
    return this.service.listAllForAdmin(branchId);
  }

  @Patch(':id/publish')
  publish(@Param('id', new ParseUUIDPipe()) id: string) {
    return this.service.setPublished(id, true);
  }

  @Patch(':id/unpublish')
  unpublish(@Param('id', new ParseUUIDPipe()) id: string) {
    return this.service.setPublished(id, false);
  }

  @Delete(':id')
  remove(@Param('id', new ParseUUIDPipe()) id: string) {
    return this.service.remove(id);
  }
}
