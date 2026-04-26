import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { IoAdapter } from '@nestjs/platform-socket.io';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import helmet from 'helmet';
import { AppModule } from './app.module';
import { isOriginAllowed, parseWebOrigins } from './common/cors-origins';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { rawBody: true });
  app.useWebSocketAdapter(new IoAdapter(app));

  const webOrigins = parseWebOrigins(process.env.WEB_ORIGIN);
  app.use(
    helmet({
      contentSecurityPolicy: false,
      crossOriginEmbedderPolicy: false,
      crossOriginResourcePolicy: { policy: 'cross-origin' },
      hsts:
        process.env.ENABLE_API_HSTS === 'true'
          ? { maxAge: 15_552_000, includeSubDomains: true, preload: false }
          : false,
    }),
  );

  const server = app.getHttpAdapter().getInstance();
  if (process.env.NODE_ENV === 'production' && server?.set) {
    server.set('trust proxy', Number(process.env.TRUST_PROXY_HOPS ?? 1));
  }

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );
  app.setGlobalPrefix('api');
  app.enableCors({
    origin: (origin, callback) => {
      callback(null, isOriginAllowed(origin, webOrigins));
    },
    credentials: true,
    methods: ['GET', 'HEAD', 'PUT', 'PATCH', 'POST', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    maxAge: 86_400,
  });

  const enableSwagger =
    process.env.NODE_ENV !== 'production' || process.env.ENABLE_SWAGGER === 'true';
  if (enableSwagger) {
    const swagger = new DocumentBuilder()
      .setTitle('Beshqozon — stol bron')
      .setDescription('REST + Payme merchant + Click + BullMQ + SMS')
      .setVersion('1.0')
      .addBearerAuth()
      .build();
    SwaggerModule.setup('api/docs', app, SwaggerModule.createDocument(app, swagger));
  }

  const port = Number(process.env.PORT ?? 4000);
  await app.listen(port, '0.0.0.0');
}
bootstrap();
