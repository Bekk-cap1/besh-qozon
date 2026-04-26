import { IsEnum, IsInt, IsNumber, IsOptional, IsString, IsUUID, Max, Min } from 'class-validator';
import { TableStatus, TableType } from '@prisma/client';

export class CreateTableDto {
  @IsUUID()
  zoneId!: string;

  @IsString()
  number!: string;

  @IsInt()
  @Min(1)
  @Max(30)
  seats!: number;

  @IsEnum(TableType)
  type!: TableType;

  @IsNumber()
  xPos!: number;

  @IsNumber()
  yPos!: number;

  @IsOptional()
  @IsString()
  shape?: string;

  @IsOptional()
  @IsEnum(TableStatus)
  status?: TableStatus;
}
