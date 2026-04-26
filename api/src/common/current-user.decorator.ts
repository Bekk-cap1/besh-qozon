import { createParamDecorator, ExecutionContext } from '@nestjs/common';

export type AuthUser = { userId: string; role: string };

export const CurrentUser = createParamDecorator(
  (_data: unknown, ctx: ExecutionContext): AuthUser => {
    const req = ctx.switchToHttp().getRequest();
    return req.user as AuthUser;
  },
);
