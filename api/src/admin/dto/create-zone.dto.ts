import { IsEnum, IsInt, IsOptional, IsString, IsUUID } from 'class-validator';
import { ZoneType } from '@prisma/client';

export class CreateZoneDto {
  @IsUUID()
  branchId!: string;

  @IsString()
  name!: string;

  @IsEnum(ZoneType)
  type!: ZoneType;

  @IsOptional()
  @IsInt()
  floor?: number;
}
