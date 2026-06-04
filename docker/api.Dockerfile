# Build from monorepo root: docker build -f docker/api.Dockerfile .
FROM node:20-slim AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends openssl ca-certificates \
  && rm -rf /var/lib/apt/lists/*
COPY package.json package-lock.json ./
COPY api/package.json ./api/
COPY web/package.json ./web/
RUN rm -f package-lock.json && npm install --include=optional --no-audit --no-fund
COPY api ./api
RUN npm run prisma:generate --workspace=api
RUN npm run build --workspace=api
RUN npm prune --omit=dev

FROM node:20-slim AS runner
WORKDIR /app
ENV NODE_ENV=production
RUN apt-get update && apt-get install -y --no-install-recommends openssl ca-certificates \
  && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/api/package.json ./api/package.json
COPY --from=builder /app/api/dist ./api/dist
COPY --from=builder /app/api/prisma ./api/prisma
RUN chown -R node:node /app
EXPOSE 4000
USER node
WORKDIR /app/api
# Free-tier DB может просыпаться/инициализироваться с задержкой — повторяем миграцию,
# пока база не станет доступной (до ~10 попыток), затем стартуем сервер.
CMD ["sh", "-c", "n=0; until npx --no-install prisma migrate deploy --schema=prisma/schema.prisma; do n=$((n+1)); if [ $n -ge 10 ]; then echo 'migrate failed after 10 attempts'; exit 1; fi; echo \"DB not ready, retry $n/10 in 5s...\"; sleep 5; done && node dist/main.js"]
