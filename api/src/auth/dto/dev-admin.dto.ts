import { IsString, Matches, MinLength } from 'class-validator';

export class DevAdminDto {
  @IsString()
  @MinLength(9)
  @Matches(/^\+998\d{9}$/)
  phone!: string;
}
