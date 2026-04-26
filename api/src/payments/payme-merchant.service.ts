import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PaymentStatus, ReservationStatus } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { PaymentsService } from './payments.service';

type RpcBody = {
  jsonrpc?: string;
  id?: string | number | null;
  method?: string;
  params?: Record<string, unknown>;
};

@Injectable()
export class PaymeMerchantService {
  private readonly log = new Logger(PaymeMerchantService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly payments: PaymentsService,
    private readonly config: ConfigService,
  ) {}

  private ok(id: RpcBody['id'], result: Record<string, unknown>) {
    return { jsonrpc: '2.0', id, result };
  }

  private err(id: RpcBody['id'], code: number, msg: string) {
    return {
      jsonrpc: '2.0',
      id,
      error: { code, message: { ru: msg, uz: msg, en: msg } },
    };
  }

  /** Prod uchun Payme hujjatidagi digest bilan kengaytiring. Hozircha faqat flag. */
  private verifyAuth(_rawBody: string, authorization?: string): boolean {
    if (this.config.get<string>('PAYME_SKIP_AUTH') === 'true') return true;
    return !!authorization?.startsWith('Paycom ');
  }

  async handleRpc(rawBody: string, authorization?: string): Promise<Record<string, unknown>> {
    let body: RpcBody;
    try {
      body = JSON.parse(rawBody) as RpcBody;
    } catch {
      return this.err(null, -32700, 'Parse error') as Record<string, unknown>;
    }
    if (!this.verifyAuth(rawBody, authorization)) {
      this.log.warn('Payme auth rad etildi');
      return this.err(body.id, -32504, 'Permission denied') as Record<string, unknown>;
    }
    const method = body.method;
    const id = body.id;
    const params = (body.params ?? {}) as Record<string, unknown>;
    try {
      if (method === 'CheckPerformTransaction') return await this.checkPerform(id, params);
      if (method === 'CreateTransaction') return await this.createTx(id, params);
      if (method === 'PerformTransaction') return await this.performTx(id, params);
      if (method === 'CancelTransaction') return await this.cancelTx(id, params);
      if (method === 'CheckTransaction') return await this.checkTx(id, params);
      return this.err(id, -32601, 'Method not found') as Record<string, unknown>;
    } catch (e) {
      this.log.error(e);
      return this.err(id, -32400, 'System error') as Record<string, unknown>;
    }
  }

  private async checkPerform(id: RpcBody['id'], params: Record<string, unknown>) {
    const account = params.account as { order_id?: string } | undefined;
    const orderId = account?.order_id;
    const amount = Number(params.amount);
    if (!orderId) return this.err(id, -31099, 'Invalid order');
    const pay = await this.prisma.payment.findUnique({
      where: { id: orderId },
      include: { reservation: true },
    });
    if (!pay || pay.status !== PaymentStatus.PENDING) return this.err(id, -31050, 'Order not found');
    if (pay.reservation.status !== ReservationStatus.PENDING_PAYMENT) {
      return this.err(id, -31051, 'Invalid reservation state');
    }
    const expected = Math.round(Number(pay.amount) * 100);
    if (amount !== expected) return this.err(id, -31001, 'Invalid amount');
    return this.ok(id, { allow: true, additional: {} });
  }

  private async createTx(id: RpcBody['id'], params: Record<string, unknown>) {
    const account = params.account as { order_id?: string } | undefined;
    const orderId = account?.order_id;
    const paymeId = String(params.id ?? '');
    if (!orderId || !paymeId) return this.err(id, -31099, 'Invalid params');
    const pay = await this.prisma.payment.findUnique({ where: { id: orderId } });
    if (!pay || pay.status !== PaymentStatus.PENDING) return this.err(id, -31050, 'Order not found');
    await this.prisma.payment.update({
      where: { id: orderId },
      data: { transactionId: paymeId },
    });
    const createTime = Math.floor(Date.now() / 1000);
    return this.ok(id, { create_time: createTime, transaction: paymeId, state: 1, receivers: null });
  }

  private async performTx(id: RpcBody['id'], params: Record<string, unknown>) {
    const account = params.account as { order_id?: string } | undefined;
    const orderId = account?.order_id;
    const paymeId = String(params.id ?? '');
    if (!orderId) return this.err(id, -31099, 'Invalid params');
    let res;
    try {
      res = await this.payments.confirmPaymentSuccess(orderId, paymeId);
    } catch {
      return this.err(id, -31050, 'Payment failed') as Record<string, unknown>;
    }
    if (!res) return this.err(id, -31050, 'Order not found');
    const performTime = Math.floor(Date.now() / 1000);
    return this.ok(id, {
      transaction: paymeId,
      perform_time: performTime,
      state: 2,
      providers: null,
    });
  }

  private async cancelTx(id: RpcBody['id'], params: Record<string, unknown>) {
    const account = params.account as { order_id?: string } | undefined;
    const orderId = account?.order_id;
    if (!orderId) return this.err(id, -31099, 'Invalid params');
    await this.prisma.payment.updateMany({
      where: { id: orderId, status: PaymentStatus.PENDING },
      data: { status: PaymentStatus.FAILED },
    });
    return this.ok(id, { transaction: String(params.id ?? ''), cancel_time: Math.floor(Date.now() / 1000), state: -1 });
  }

  private async checkTx(id: RpcBody['id'], params: Record<string, unknown>) {
    const paymeId = String(params.id ?? '');
    const pay = await this.prisma.payment.findFirst({ where: { transactionId: paymeId } });
    if (!pay) return this.err(id, -31050, 'Not found');
    const st = pay.status === PaymentStatus.COMPLETED ? 2 : 1;
    return this.ok(id, { create_time: Math.floor(pay.createdAt.getTime() / 1000), perform_time: st === 2 ? Math.floor(Date.now() / 1000) : 0, transaction: paymeId, state: st, reason: null });
  }
}
