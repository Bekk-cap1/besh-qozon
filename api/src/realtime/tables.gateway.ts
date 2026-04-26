import { OnEvent } from '@nestjs/event-emitter';
import { OnGatewayConnection, WebSocketGateway, WebSocketServer } from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { isOriginAllowed, parseWebOrigins } from '../common/cors-origins';

@WebSocketGateway({
  cors: {
    origin: (origin, cb) => {
      const allowed = parseWebOrigins(process.env.WEB_ORIGIN);
      cb(null, isOriginAllowed(origin, allowed));
    },
    credentials: true,
  },
})
export class TablesGateway implements OnGatewayConnection {
  @WebSocketServer()
  server!: Server;

  private isUuid(s: unknown): s is string {
    return (
      typeof s === 'string' &&
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(s)
    );
  }

  handleConnection(client: Socket) {
    client.on('joinZone', (zoneId: string) => {
      if (this.isUuid(zoneId)) {
        void client.join(`zone:${zoneId}`);
      }
    });
    client.on('leaveZone', (zoneId: string) => {
      if (this.isUuid(zoneId)) {
        void client.leave(`zone:${zoneId}`);
      }
    });
    client.on('joinAdmin', () => {
      void client.join('admin');
    });
  }

  @OnEvent('zone.refresh')
  handleZoneRefresh(zoneId: string) {
    this.server?.to(`zone:${zoneId}`).emit('tables:refresh', { zoneId });
    this.server?.to('admin').emit('admin:refresh', { scope: 'zone', zoneId });
  }

  @OnEvent('reservations.refresh')
  handleReservationsRefresh(payload: { reservationId?: string; zoneId?: string } = {}) {
    this.server?.to('admin').emit('admin:refresh', { scope: 'reservations', ...payload });
  }
}
