# ── Stage 1: build client ──────────────────────────────────────────────────
FROM node:20-alpine AS client-builder

WORKDIR /app

# Install bun
RUN npm install -g bun@1.3.14

# Copy workspace manifests
COPY package.json bun.lock* ./
COPY apps/client/package.json ./apps/client/
COPY apps/server/package.json ./apps/server/

# Install all dependencies
RUN bun install --frozen-lockfile

# Copy source and build client
COPY apps/client ./apps/client
RUN cd apps/client && bun run build

# ── Stage 2: build server ──────────────────────────────────────────────────
FROM oven/bun:1.3.14-alpine AS server-builder

WORKDIR /app

COPY package.json bun.lock* ./
COPY apps/server/package.json ./apps/server/
COPY apps/client/package.json ./apps/client/

RUN bun install --frozen-lockfile --production

COPY apps/server ./apps/server

# Bundle server to single file
RUN cd apps/server && NODE_ENV=production bun build src/index.ts --outdir dist --target bun --minify

# ── Stage 3: production runner ─────────────────────────────────────────────
FROM oven/bun:1.3.14-alpine AS runner

WORKDIR /app

RUN addgroup -S bullmq && adduser -S bullmq -G bullmq

# Copy built server bundle
COPY --from=server-builder /app/apps/server/dist ./server/dist

# Copy built client assets to be served as static files
COPY --from=client-builder /app/apps/client/dist ./public

# Copy node_modules needed at runtime (bullmq, ioredis, hono)
COPY --from=server-builder /app/node_modules ./node_modules
COPY --from=server-builder /app/apps/server/node_modules ./apps/server/node_modules

USER bullmq

ARG REDIS_URL=redis://localhost:6379
ARG BULLSTUDIO_USERNAME
ARG BULLSTUDIO_PASSWORD

ENV NODE_ENV=production \
    PORT=4000 \
    HOST=0.0.0.0 \
    REDIS_URL=$REDIS_URL \
    BULLMQ_USERNAME=$BULLSTUDIO_USERNAME \
    BULLMQ_PASSWORD=$BULLSTUDIO_PASSWORD

EXPOSE 4000

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s \
  CMD wget -qO- http://localhost:4000/health || exit 1

CMD ["bun", "run", "server/dist/index.js"]
