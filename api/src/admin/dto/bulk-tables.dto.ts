import { IsEnum, IsInt, IsNumber, IsOptional, IsString, Length, Max, Min } from 'class-validator';
import { TableType } from '@prisma/client';

export class BulkTablesDto {
  @IsInt()
  @Min(1)
  @Max(80)
  count!: number;

  @IsString()
  @Length(1, 8)
  prefix!: string;

  @IsInt()
  @Min(1)
  @Max(30)
  seatsPerTable!: number;

  @IsEnum(TableType)
  type!: TableType;

  @IsOptional()
  @IsNumber()
  startX?: number;

  @IsOptional()
  @IsNumber()
  startY?: number;

  @IsOptional()
  @IsNumber()
  stepX?: number;

  @IsOptional()
  @IsNumber()
  rowHeight?: number;

  /** Bir qatorda nechta stol (masalan ko‘cha uchun 8) */
  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(20)
  columnsPerRow?: number;
}
