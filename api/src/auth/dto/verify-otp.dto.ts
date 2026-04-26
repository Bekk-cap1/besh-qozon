import { IsOptional, IsString, Length, Matches, MinLength } from 'class-validator';

export class VerifyOtpDto {
  @IsString()
  @MinLength(9)
  @Matches(/^\+998\d{9}$/)
  phone!: string;

  @IsString()
  @Length(4, 8)
  code!: string;

  @IsOptional()
  @IsString()
  @Length(2, 80)
  name?: string;
}
