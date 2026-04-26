import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  ParseUUIDPipe,
  Post,
  Put,
  UseGuards,
} from '@nestjs/common';
import {
  IsBoolean,
  IsInt,
  IsNumber,
  IsOptional,
  IsString,
  Length,
  Max,
  Min,
} from 'class-validator';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Roles } from '../auth/roles.decorator';
import { RolesGuard } from '../auth/roles.guard';
import { MenuService } from '../menu/menu.service';

class CreateMenuItemDto {
  @IsString()
  @Length(1, 120)
  name!: string;

  @IsOptional()
  @IsString()
  @Length(0, 500)
  description?: string;

  @IsOptional()
  @IsString()
  @Length(0, 60)
  category?: string;

  @IsNumber()
  @Min(0)
  @Max(100_000_000)
  price!: number;

  @IsOptional()
  @IsString()
  imageUrl?: string;

  @IsOptional()
  @IsInt()
  @Min(0)
  @Max(99_999)
  sortOrder?: number;
}

class UpdateMenuItemDto {
  @IsOptional()
  @IsString()
  @Length(1, 120)
  name?: string;

  @IsOptional()
  @IsString()
  @Length(0, 500)
  description?: string | null;

  @IsOptional()
  @IsString()
  @Length(0, 60)
  category?: string;

  @IsOptional()
  @IsNumber()
  @Min(0)
  @Max(100_000_000)
  price?: number;

  @IsOptional()
  @IsString()
  imageUrl?: string | null;

  @IsOptional()
  @IsInt()
  @Min(0)
  @Max(99_999)
  sortOrder?: number;

  @IsOptional()
  @IsBoolean()
  isActive?: boolean;
}

@Controller('admin')
@UseGuards(JwtAuthGuard, RolesGuard)
@Roles('ADMIN')
export class AdminMenuController {
  constructor(private readonly menu: MenuService) {}

  @Get('branches/:branchId/menu')
  list(@Param('branchId', new ParseUUIDPipe()) branchId: string) {
    return this.menu.adminListForBranch(branchId);
  }

  @Post('branches/:branchId/menu')
  create(
    @Param('branchId', new ParseUUIDPipe()) branchId: string,
    @Body() dto: CreateMenuItemDto,
  ) {
    return this.menu.create({
      branchId,
      name: dto.name,
      description: dto.description,
      category: dto.category,
      price: dto.price,
      imageUrl: dto.imageUrl,
      sortOrder: dto.sortOrder,
    });
  }

  @Put('menu/:id')
  update(
    @Param('id', new ParseUUIDPipe()) id: string,
    @Body() dto: UpdateMenuItemDto,
  ) {
    return this.menu.update(id, dto);
  }

  @Delete('menu/:id')
  remove(@Param('id', new ParseUUIDPipe()) id: string) {
    return this.menu.remove(id);
  }
}
