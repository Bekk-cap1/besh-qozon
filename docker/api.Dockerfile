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
CMD ["sh", "-c", "npx --no-install prisma migrate deploy --schema=prisma/schema.prisma && node dist/main.js"]
