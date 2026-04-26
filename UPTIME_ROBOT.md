# UptimeRobot — keep-alive для Render free

## Зачем
Render free-сервис **засыпает после 15 минут** без HTTP-запросов. Первый запрос
после сна идёт ~30 секунд (cold start). UptimeRobot бесплатно пингует API
**раз в 5 минут** — этого достаточно, чтобы сервис не успевал засыпать.

Бонус: ты получишь email/Telegram-алерт, если API упал.

## Эндпоинты

| Путь | Что делает | Когда использовать |
|---|---|---|
| `/api/health` | Возвращает 200 OK мгновенно (не лезет в БД) | UptimeRobot, Render health check |
| `/api/health/deep` | Пингует БД и Redis, возвращает latency | Для алертов на реальные проблемы |

Оба эндпоинта **не попадают под rate limit** (`@SkipThrottle()`).

Пример ответа `/api/health`:
```json
{
  "status": "ok",
  "service": "beshqozon-api",
  "uptimeSec": 12345,
  "time": "2026-04-26T07:12:00.000Z"
}
```

Пример ответа `/api/health/deep`:
```json
{
  "status": "ok",
  "service": "beshqozon-api",
  "uptimeSec": 12345,
  "time": "2026-04-26T07:12:00.000Z",
  "checks": {
    "db":    { "ok": true, "latencyMs": 12 },
    "redis": { "ok": true, "latencyMs": 3  }
  }
}
```

## Настройка UptimeRobot

1. Зарегистрируйся на https://uptimerobot.com (бесплатно, 50 мониторов).
2. **+ Add New Monitor**.
3. Параметры:
   | Поле | Значение |
   |---|---|
   | Monitor Type | **HTTP(s)** |
   | Friendly Name | `Beshqozon API` |
   | URL | `https://beshqozon-api.onrender.com/api/health` |
   | Monitoring Interval | **5 minutes** (на free это минимум) |
   | Monitor Timeout | 30 seconds (cold start первого пинга) |
   | HTTP Method | GET |
   | Keyword Monitoring (опционально) | Keyword: `"status":"ok"` (Type: Exists) |
4. **Alert Contacts** — добавь email или Telegram (UptimeRobot → My Settings → Alert Contacts → Add).
5. **Create Monitor**.

После сохранения первый пинг пойдёт сразу. Через 5–10 минут увидишь зелёные точки uptime.

## Если хочешь отдельно мониторить БД и Redis

Создай **второй монитор** с теми же настройками, но URL:
```
https://beshqozon-api.onrender.com/api/health/deep
```
И в Keyword поставь `"status":"ok"`. Если БД или Redis упадут — `status` станет `degraded`, монитор зажжётся красным.

## Что делать с алертом «Down»

1. Открой Render dashboard → `beshqozon-api → Logs`. Посмотри последние строки.
2. Частые причины:
   - **Free Postgres истёк через 90 дней** → Resume или upgrade.
   - **OOM (out of memory)** → free даёт 512 МБ, при пиках может упасть.
   - **Build не прошёл** → Manual Deploy → Clear build cache & deploy.

## Альтернатива — Render Cron Jobs

Если не хочешь стороннего сервиса, на Render есть Cron Jobs ($1/мес).
Создаёшь Cron с командой `curl -fsS https://beshqozon-api.onrender.com/api/health` и расписанием `*/10 * * * *`.

UptimeRobot бесплатнее и сразу даёт алерты — рекомендую его.
