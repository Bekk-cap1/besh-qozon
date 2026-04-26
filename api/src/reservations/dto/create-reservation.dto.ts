import {
  ArrayMaxSize,
  IsArray,
  IsBoolean,
  IsInt,
  IsISO8601,
  IsOptional,
  IsUUID,
  Max,
  Min,
  ValidateNested,
} from 'class-validator';
import { Type } from 'class-transformer';

export class PreorderItemDto {
  @IsUUID()
  menuItemId!: string;

  @IsInt()
  @Min(1)
  @Max(50)
  quantity!: number;
}

export class CreateReservationDto {
  @IsUUID()
  branchId!: string;

  @IsUUID()
  tableId!: string;

  /** ISO8601, masalan 2026-04-21T12:00:00 */
  @IsISO8601()
  startAt!: string;

  @IsInt()
  @Min(1)
  @Max(20)
  guestsCount!: number;

  /** Использовать накопленный бонус — депозит 0, автоподтверждение */
  @IsOptional()
  @IsBoolean()
  useBonus?: boolean;

  /** Предзаказ (опционально) */
  @IsOptional()
  @IsArray()
  @ArrayMaxSize(50)
  @ValidateNested({ each: true })
  @Type(() => PreorderItemDto)
  items?: PreorderItemDto[];
}
