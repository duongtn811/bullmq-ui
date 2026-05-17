# BullMQ UI

A lightweight, self-hosted UI for managing BullMQ queues and jobs. Built with Bun, Hono, React, and shadcn/ui.

![BullMQ Studio](bullmq.gif)

## Features

- **Overview** — throughput charts, processing time, slowest & most-failing job types
- **Queues** — list all queues with job counts, pause / resume / clean / delete
- **Jobs** — global job table with filters by queue, status, and name; retry / promote / remove per row
- **Job detail** — data, result, error, and logs tabs with progress bar
- **Workers** — live view of connected workers per queue
- **Redis** — supports local Redis, password auth, TLS (`rediss://`), Valkey, AWS ElastiCache

---

## Getting Started

### Prerequisites

- [Bun](https://bun.sh) v1.3+
- Redis (local, Docker, or remote)

### 1. Install dependencies

```bash
bun install
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and set your `REDIS_URL`:

```env
# Local Redis
REDIS_URL=redis://localhost:6379

# Redis with password
REDIS_URL=redis://:yourpassword@localhost:6379

# TLS (Valkey / AWS ElastiCache)
REDIS_URL=rediss://user:pass@hostname:6380
```

---

## Redis Compatibility

| Provider                      | URL format                                     | Notes                                         |
| ----------------------------- | ---------------------------------------------- | --------------------------------------------- |
| **Local**                     | `redis://localhost:6379`                       | No auth                                       |
| **Self-hosted (password)**    | `redis://:password@host:6379`                  | Standard Redis auth                           |
| **Self-hosted (user + pass)** | `redis://user:password@host:6379`              | ACL-based auth                                |
| **Valkey**                    | `redis://host:6379` or `rediss://host:6380`    | Drop-in Redis replacement; TLS optional       |
| **Redis Cloud**               | `rediss://default:password@host.redis.io:6380` | Upstash, Aiven, Redis Enterprise — always TLS |
| **AWS ElastiCache**           | `rediss://host.cache.amazonaws.com:6379`       | TLS only; IAM auth not supported              |
| **Docker**                    | `redis://host.docker.internal:6379`            | From inside a container                       |

Set `REDIS_URL` in your `.env` file. The server automatically enables TLS when the URL scheme is `rediss://`.

### 3. Start in development mode

Runs the server (port 3001) and the Vite dev server (port 5173) concurrently with hot reload:

```bash
bun run dev
```

Open [http://localhost:5173](http://localhost:5173)

### 4. Build and run for production

```bash
bun run build
bun run start
```

Open [http://localhost:3001](http://localhost:3001)

---

## Docker

### Run with Docker Compose (includes Redis)

```bash
docker compose up -d
```

Open [http://localhost:3001](http://localhost:3001)

### Connect to an external Redis

```bash
REDIS_URL=redis://your-redis-host:6379 docker compose up -d bullmq-ui
```

### Build the image manually

```bash
docker build -t bullmq-ui .
docker run -p 3001:3001 -e REDIS_URL=redis://host.docker.internal:6379 bullmq-ui
```

---

## Tech Stack

| Layer   | Technology                                       |
| ------- | ------------------------------------------------ |
| Runtime | [Bun](https://bun.sh)                            |
| Server  | [Hono](https://hono.dev)                         |
| Queue   | [BullMQ](https://docs.bullmq.io)                 |
| Redis   | [ioredis](https://github.com/redis/ioredis)      |
| Client  | React 18, Vite, TypeScript                       |
| UI      | [shadcn/ui](https://ui.shadcn.com), Tailwind CSS |
| Charts  | [Recharts](https://recharts.org)                 |
