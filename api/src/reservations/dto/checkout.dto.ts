import { IsIn } from 'class-validator';

export class CheckoutDto {
  @IsIn(['payme', 'click'])
  provider!: 'payme' | 'click';
}
