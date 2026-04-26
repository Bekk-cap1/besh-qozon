import { IsArray, IsIn, IsNumber, IsOptional, IsString, ValidateNested } from 'class-validator';
import { Type } from 'class-transformer';

export class FloorDoorDto {
  @IsString()
  kind!: 'door';

  @IsNumber()
  x!: number;

  @IsNumber()
  y!: number;
}

export class FloorLabelDto {
  @IsString()
  kind!: 'label';

  @IsString()
  text!: string;

  @IsIn(['bar', 'kitchen', 'default'])
  style!: 'bar' | 'kitchen' | 'default';

  @IsNumber()
  x!: number;

  @IsNumber()
  y!: number;
}

export class UpdateFloorConfigDto {
  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => FloorDoorDto)
  doors?: FloorDoorDto[];

  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => FloorLabelDto)
  labels?: FloorLabelDto[];
}
