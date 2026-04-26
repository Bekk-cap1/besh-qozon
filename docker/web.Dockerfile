# Build from monorepo root. NEXT_PUBLIC_API_URL — браузерный URL API (на этапе сборки).
FROM node:20-slim AS builder
WORKDIR /app
ARG NEXT_PUBLIC_API_URL=http://localhost:4000/api
ENV NEXT_PUBLIC_API_URL=$NEXT_PUBLIC_API_URL
ENV NEXT_TELEMETRY_DISABLED=1
COPY package.json package-lock.json ./
COPY api/package.json ./api/
COPY web/package.json ./web/
# package-lock был создан на Windows — платформенные optional-зависимости (lightningcss)
# для Linux отсутствуют в lockfile; переустанавливаем с resolver'ом под текущую платформу.
RUN rm -f package-lock.json && npm install --include=optional --no-audit --no-fund
COPY web ./web
RUN npm run build --workspace=web

FROM node:20-slim AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV PORT=3000
ENV HOSTNAME=0.0.0.0
COPY --from=builder /app/web/.next/standalone ./
COPY --from=builder /app/web/.next/static ./web/.next/static
COPY --from=builder /app/web/public ./web/public
EXPOSE 3000
USER node
CMD ["node", "web/server.js"]
