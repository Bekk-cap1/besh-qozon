import { IsString, Matches, MinLength } from 'class-validator';

export class RequestOtpDto {
  @IsString()
  @MinLength(9)
  @Matches(/^\+998\d{9}$/, { message: 'Telefon +998XXXXXXXXX formatida bo‘lishi kerak' })
  phone!: string;
}
