# Чеклист перед релизом Beshqozon

## Окружение API (`api/.env` или секреты в оркестраторе)

- `NODE_ENV=production`
- `DATABASE_URL` — рабочий Postgres (SSL при необходимости).
- `REDIS_URL` — Redis для BullMQ и лимитов OTP.
- `JWT_SECRET` — длинная случайная строка (не из примера).
- `WEB_ORIGIN` — **только** доверенные фронтенд-URL (через запятую, без пробелов вокруг домена), иначе браузерный CORS и Socket.IO отклонят чужой сайт.
- `ALLOW_DEV_ADMIN=false` — отключить выдачу JWT через `dev-admin-token`.
- `ENABLE_SWAGGER` — не задавать или `false`, чтобы **не** открывать `/api/docs` в проде (или явно `true` только во внутренней сети).
- `ENABLE_API_HSTS` — включать (`true`) только если API отдаётся клиенту **напрямую по HTTPS**; за TLS-терминатором часто достаточно HSTS на edge.
- `TRUST_PROXY_HOPS` — при прокси (например `1`), чтобы корректно учитывался IP для лимитов.
- OTP: задать реальные ключи Eskiz (`ESKIZ_*`), убрать зависимость от `DEV_OTP` в проде.
- Payme: `PAYME_SKIP_AUTH=false`, настроить merchant и ключи по документации Payme.
- Click: `CLICK_SKIP_SIGN=false`, настроить `CLICK_*` по документации.
- CORS: при необходимости ограничить `origin` в `main.ts` доменом фронта.

## Окружение Web (`web/.env` / build-args)

- `NEXT_PUBLIC_API_URL` — **полный публичный** URL API с суффиксом `/api` (вшивается на этапе `next build`).
- Для Docker: передавать `--build-arg NEXT_PUBLIC_API_URL=...` при сборке образа `web`.
- `NEXT_PUBLIC_ENABLE_HSTS=true` — только если сайт всегда отдаётся по **HTTPS** (заголовок HSTS в middleware Next).

## База данных

- Выполнить миграции: `npx prisma migrate deploy` (в CI или entrypoint контейнера API).
- Сид **не** запускать в проде (он чистит данные).

## Проверки после деплоя

- `GET /api/health` — `status: ok`.
- `GET /api/docs` — Swagger (при необходимости отключить или закрыть Basic Auth).
- Бронь: OTP, hold, оплата (тестовая сумма в песочнице провайдера).
- Socket.IO: клиент подключается к тому же хосту, что и API (см. `wsOrigin()` на фронте).

## Docker (пример)

```bash
docker compose -f docker-compose.release.yml build --build-arg NEXT_PUBLIC_API_URL=https://api.ваш-домен.uz/api web
docker compose -f docker-compose.release.yml up -d
```

Затем один раз выполнить миграции в контейнере API (или отдельным job).

## Презентация для руководства

Статический файл: `docs/prezentatsiya-ruhbiyat.html` — открыть в браузере (F11 полный экран, стрелки ← → для слайдов).
