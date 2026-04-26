import { Type } from 'class-transformer';
import { ArrayMinSize, IsArray, IsNumber, IsUUID, ValidateNested } from 'class-validator';

export class FloorPlanTablePosDto {
  @IsUUID()
  id!: string;

  @IsNumber()
  xPos!: number;

  @IsNumber()
  yPos!: number;
}

export class UpdateFloorPlanDto {
  @IsArray()
  @ArrayMinSize(1)
  @ValidateNested({ each: true })
  @Type(() => FloorPlanTablePosDto)
  tables!: FloorPlanTablePosDto[];
}
