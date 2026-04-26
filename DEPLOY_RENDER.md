# Деплой бэка на Render

В корне лежит `render.yaml` (Blueprint). Он поднимает три ресурса:

| Сервис | Тип | Тариф |
|---|---|---|
| `beshqozon-db` | Postgres 16 | free (90 дней) |
| `beshqozon-redis` | Key Value (Redis) | free |
| `beshqozon-api` | Web Service (Docker) | free |

## Шаг 1. Запушить файл в GitHub

```bash
git add render.yaml DEPLOY_RENDER.md api/src/main.ts
git commit -m "chore(deploy): add Render blueprint"
git push
```

## Шаг 2. Создать Blueprint на Render

1. Зайди на https://dashboard.render.com → **New** → **Blueprint**.
2. Подключи GitHub-репо `Bekk-cap1/besh-qozon`.
3. Render найдёт `render.yaml` и покажет план: 1 БД + 1 Redis + 1 Web. Жми **Apply**.
4. Подожди 5–10 минут. Первая сборка Docker-образа долгая.

## Шаг 3. Заполнить секреты

После создания, открой сервис **beshqozon-api → Environment** и заполни поля с пометкой *Set value*:

| Ключ | Что вписать |
|---|---|
| `ADMIN_LOGIN` | твой логин админа |
| `ADMIN_PASSWORD` | пароль (12+ символов) |
| `TELEGRAM_BOT_TOKEN` | токен от @BotFather (если включаешь TG) |
| `ESKIZ_EMAIL` / `ESKIZ_PASSWORD` | для реальных SMS (необязательно) |
| `PAYME_MERCHANT_ID/KEY`, `CLICK_*` | для реальных платежей (необязательно) |

Если включаешь Telegram — поменяй `TELEGRAM_ENABLED` на `true`.

После сохранения сервис автоматически передеплоится.

## Шаг 4. Проверить

```bash
curl https://beshqozon-api.onrender.com/api/health
# {"status":"ok"}
```

URL твоего API будет вида `https://beshqozon-api.onrender.com`. Запиши его — он понадобится фронту:

```bash
NEXT_PUBLIC_API_URL=https://beshqozon-api.onrender.com/api
```

## Шаг 5. Прокинуть URL фронта в CORS

После деплоя фронта (на Vercel/Netlify/где угодно) обнови `WEB_ORIGIN` в Environment beshqozon-api:

```
WEB_ORIGIN=https://your-frontend.vercel.app,https://www.your-domain.uz
```

Поддерживаются wildcards: `https://*.vercel.app`.

## Что важно знать про free tier

- **Web Service засыпает** после 15 минут без запросов. Первый запрос после сна идёт ~30 секунд (cold start). Telegram long polling в это время будет паузить — для серьёзной работы возьми Starter ($7/мес).
- **Postgres free** живёт 90 дней, потом надо апнуть до Starter ($7/мес).
- **Redis free** — 25 МБ, для BullMQ хватает с большим запасом.
- **Миграции Prisma** запускаются автоматически на каждом деплое (см. CMD в `docker/api.Dockerfile`).

## Если что-то пошло не так

| Симптом | Решение |
|---|---|
| `Application failed to respond` на health check | Проверь, что в логах `Nest application successfully started` и `app.listen(port, '0.0.0.0')` |
| `P1001: Can't reach database server` | DATABASE_URL не подтянулся из Blueprint — проверь в Environment |
| `ECONNREFUSED` к Redis | REDIS_URL пустой — проверь, что redis-сервис создался |
| 502 на WebSocket | Render поддерживает WS по умолчанию, проверь что фронт стучит на `wss://`, а не `ws://` |

## Логи и SSH

```bash
# Через web UI: beshqozon-api → Logs
# Или CLI:
render logs beshqozon-api --tail
render ssh beshqozon-api  # только на платном плане
```
