import { Hono } from "hono";
import { cors } from "hono/cors";
import { logger } from "hono/logger";
import { serveStatic } from "hono/bun";
import connectionRoutes from "./routes/connection.js";
import queuesRoutes from "./routes/queues.js";
import jobsRoutes from "./routes/jobs.js";
import overviewRoutes from "./routes/overview.js";
import { ensureConnected } from "./services/bullmq-service.js";

const app = new Hono();

const AUTH_USER = process.env.BULLMQ_USERNAME;
const AUTH_PASS = process.env.BULLMQ_PASSWORD;
const AUTH_ENABLED = !!(AUTH_USER && AUTH_PASS);

app.use("*", logger());
app.use(
  "/api/*",
  cors({
    origin: "*",
    allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowHeaders: ["Content-Type", "Authorization"],
  }),
);

// Public — always accessible regardless of auth config
app.get("/api/auth/enabled", (c) => c.json({ enabled: AUTH_ENABLED }));

// Basic auth middleware for all other /api/* routes
if (AUTH_ENABLED) {
  app.use("/api/*", async (c, next) => {
    if (c.req.path === "/api/auth/enabled") return next();

    const authHeader = c.req.header("Authorization");
    if (!authHeader?.startsWith("Basic ")) {
      return c.json({ error: "Unauthorized" }, 401);
    }

    try {
      const decoded = atob(authHeader.slice(6));
      const colonIdx = decoded.indexOf(":");
      const user = decoded.slice(0, colonIdx);
      const pass = decoded.slice(colonIdx + 1);
      if (user !== AUTH_USER || pass !== AUTH_PASS) {
        return c.json({ error: "Unauthorized" }, 401);
      }
    } catch {
      return c.json({ error: "Unauthorized" }, 401);
    }

    return next();
  });
}

// Health check
app.get("/health", (c) => c.json({ ok: true }));
app.get("/healthz", (c) => c.json({ ok: true }));

// API routes
app.route("/api/connection", connectionRoutes);
app.route("/api/queues", queuesRoutes);
app.route("/api/jobs", jobsRoutes);
app.route("/api/overview", overviewRoutes);

// Serve client build in production
if (process.env.NODE_ENV === "production") {
  app.use("/*", serveStatic({ root: "./public" }));
  app.get("*", serveStatic({ path: "./public/index.html" }));
}

const PORT = parseInt(process.env.PORT ?? "3001", 10);
const HOST = process.env.HOST ?? "0.0.0.0";

// Connect to Redis on startup
ensureConnected()
  .then(() => {
    console.log(`[bullmq-ui] Connected to Redis!`);
  })
  .catch((err: unknown) => {
    console.error(`[bullmq-ui] Redis connection failed:`, err);
    console.warn(`[bullmq-ui] Server will retry on first request`);
  });

console.log(`[bullmq-ui] Server running at http://${HOST}:${PORT}`);

export default {
  port: PORT,
  hostname: HOST,
  fetch: app.fetch,
};
