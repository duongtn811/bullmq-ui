import { Queue, Job as BullJob, JobType } from "bullmq";
import Redis from "ioredis";
import type {
  Job,
  JobSummary,
  Queue as IQueue,
  JobCounts,
  JobQueryOptions,
  JobStatus,
  RedisInfo,
} from "../types/index.js";

const DEFAULT_PREFIX = "{bullmq}";

function parseRedisInfo(raw: string): RedisInfo {
  const normalized = raw.replace(/\r/g, "");
  const get = (key: string) => {
    const match = normalized.match(new RegExp(`^${key}:(.+)$`, "m"));
    return match?.[1]?.trim() ?? "";
  };
  const totalMemory =
    get("total_system_memory_human") || get("maxmemory_human");
  return {
    version: get("redis_version"),
    mode: get("redis_mode") || "standalone",
    usedMemory: get("used_memory_human"),
    totalMemory,
    connectedClients: parseInt(get("connected_clients") || "0", 10),
    maxClients: parseInt(get("maxclients") || "0", 10),
  };
}

function normalizeProgress(
  progress: number | string | object | boolean,
): number | object {
  if (typeof progress === "boolean") return progress ? 100 : 0;
  if (typeof progress === "string") {
    const parsed = parseFloat(progress);
    return isNaN(parsed) ? { value: progress } : parsed;
  }
  return progress;
}

class BullMQService {
  private connection: Redis | null = null;
  private queues = new Map<string, Queue>();
  private prefix: string;
  private redisUrl: string;
  private _connected = false;

  constructor(redisUrl: string, prefix = DEFAULT_PREFIX) {
    this.redisUrl = redisUrl;
    this.prefix = prefix;
  }

  async connect(): Promise<void> {
    if (this._connected) return;

    this.connection = new Redis(this.redisUrl, {
      maxRetriesPerRequest: null,
      enableReadyCheck: true,
      lazyConnect: true,
      retryStrategy: () => null,
      ...(this.redisUrl.startsWith("rediss://") && {
        tls: { rejectUnauthorized: false },
      }),
    });

    this.connection.on("error", (err: Error) => {
      console.error("[bullmq-service] Redis error:", err.message);
      this._connected = false;
    });

    await this.connection.connect();
    await this.connection.ping();
    this._connected = true;
  }

  async disconnect(): Promise<void> {
    for (const q of this.queues.values()) await q.close();
    this.queues.clear();
    if (this.connection) {
      await this.connection.quit();
      this.connection = null;
    }
    this._connected = false;
  }

  isConnected(): boolean {
    return this._connected && this.connection?.status === "ready";
  }

  async getRedisInfo(): Promise<RedisInfo> {
    this.assertConnected();
    const raw = await this.connection!.info();
    return parseRedisInfo(raw);
  }

  async getQueues(): Promise<IQueue[]> {
    const names = await this.discoverQueues();
    return Promise.all(names.map((n) => this.getQueue(n))).then((qs) =>
      qs.filter((q): q is IQueue => q !== null),
    );
  }

  async getQueue(name: string): Promise<IQueue | null> {
    const queue = this.getOrCreate(name);
    const [isPaused, jobCounts] = await Promise.all([
      queue.isPaused(),
      this.getJobCounts(name),
    ]);
    return { name, isPaused, jobCounts };
  }

  async getJobCounts(queueName: string): Promise<JobCounts> {
    const queue = this.getOrCreate(queueName);
    const counts = await queue.getJobCounts(
      "waiting",
      "active",
      "completed",
      "failed",
      "delayed",
      "paused",
      "prioritized",
      "waiting-children",
    );
    return {
      waiting: counts.waiting ?? 0,
      active: counts.active ?? 0,
      completed: counts.completed ?? 0,
      failed: counts.failed ?? 0,
      delayed: counts.delayed ?? 0,
      paused: counts.paused ?? 0,
      prioritized: counts.prioritized ?? 0,
      waitingChildren: counts["waiting-children"] ?? 0,
    };
  }

  async pauseQueue(name: string): Promise<void> {
    await this.getOrCreate(name).pause();
  }

  async resumeQueue(name: string): Promise<void> {
    await this.getOrCreate(name).resume();
  }

  async cleanQueue(
    name: string,
    grace: number,
    limit: number,
    status: "completed" | "failed" | "delayed" | "active" | "wait",
  ): Promise<string[]> {
    return this.getOrCreate(name).clean(grace, limit, status);
  }

  async deleteQueue(name: string): Promise<void> {
    const queue = this.getOrCreate(name);
    await queue.obliterate({ force: true });
    await queue.close();
    this.queues.delete(name);
  }

  async getJobs(queueName: string, options?: JobQueryOptions): Promise<Job[]> {
    const queue = this.getOrCreate(queueName);
    const { filter, sort, limit = 100, offset = 0 } = options ?? {};
    const statuses = resolveStatuses(filter?.status);
    const jobs = await queue.getJobs(statuses, offset, offset + limit - 1);
    let mapped = jobs
      .filter((j): j is BullJob => j !== undefined)
      .map((j) => mapJob(j, queueName));
    if (filter?.name) mapped = mapped.filter((j) => j.name === filter.name);
    if (sort) mapped = sortJobs(mapped, sort.field, sort.order);
    return mapped;
  }

  async getJobsSummary(
    queueName: string,
    options?: JobQueryOptions,
  ): Promise<JobSummary[]> {
    const queue = this.getOrCreate(queueName);
    const { filter, sort, limit = 100, offset = 0 } = options ?? {};
    const types = resolveStatuses(filter?.status);

    const jobIds = await queue.getRanges(
      sanitizeJobTypes(types),
      offset,
      offset + limit - 1,
      false,
    );

    const summaries = await Promise.all(
      jobIds.map((id) => this.getJobSummaryFromKey(queue, id)),
    );
    let result = summaries.filter((s): s is JobSummary => s !== null);
    if (filter?.name) result = result.filter((j) => j.name === filter.name);
    if (sort) result = sortSummaries(result, sort.field, sort.order);
    return result;
  }

  private async getJobSummaryFromKey(
    queue: Queue,
    jobId: string,
  ): Promise<JobSummary | null> {
    try {
      const client = await queue.client;
      const jobKey = queue.toKey(jobId);
      const [name, timestamp, progress, attemptsMade, processedOn, finishedOn, failedReason, delay, priority, parent, rjk] =
        await client.hmget(
          jobKey,
          "name",
          "timestamp",
          "progress",
          "attemptsMade",
          "processedOn",
          "finishedOn",
          "failedReason",
          "delay",
          "priority",
          "parent",
          "rjk",
        );

      const status = await queue.getJobState(jobId);
      let parentId: string | undefined;
      if (parent) {
        try {
          parentId = JSON.parse(parent)?.id;
        } catch {}
      }

      return {
        id: jobId,
        name: name || "",
        queueName: queue.name,
        status: status as JobStatus,
        timestamp: timestamp ? parseInt(timestamp, 10) : 0,
        progress: progress ? normalizeProgress(progress) : 0,
        attemptsMade: attemptsMade ? parseInt(attemptsMade, 10) : 0,
        processedOn: processedOn ? parseInt(processedOn, 10) : undefined,
        finishedOn: finishedOn ? parseInt(finishedOn, 10) : undefined,
        failedReason: failedReason || undefined,
        delay: delay ? parseInt(delay, 10) : undefined,
        priority: priority ? parseInt(priority, 10) : undefined,
        parentId,
        repeatJobKey: rjk || undefined,
      };
    } catch {
      return null;
    }
  }

  async getJob(queueName: string, jobId: string): Promise<Job | null> {
    const queue = this.getOrCreate(queueName);
    const job = await queue.getJob(jobId);
    if (!job) return null;
    return mapJob(job, queueName);
  }

  async getJobLogs(
    queueName: string,
    jobId: string,
  ): Promise<{ logs: string[]; count: number }> {
    return this.getOrCreate(queueName).getJobLogs(jobId);
  }

  async addJob(
    queueName: string,
    jobName: string,
    data: unknown,
    options?: {
      delay?: number;
      priority?: number;
      attempts?: number;
      repeat?: { pattern?: string; every?: number; limit?: number };
    },
  ): Promise<string> {
    const queue = this.getOrCreate(queueName);
    const job = await queue.add(jobName, data, {
      delay: options?.delay,
      priority: options?.priority,
      attempts: options?.attempts,
      repeat: options?.repeat
        ? {
            pattern: options.repeat.pattern,
            every: options.repeat.every,
            limit: options.repeat.limit,
          }
        : undefined,
    });
    return job.id ?? "";
  }

  async retryJob(queueName: string, jobId: string): Promise<void> {
    const queue = this.getOrCreate(queueName);
    const job = await queue.getJob(jobId);
    if (!job) throw new Error(`Job ${jobId} not found`);
    await job.retry();
  }

  async removeJob(queueName: string, jobId: string): Promise<void> {
    const queue = this.getOrCreate(queueName);
    const job = await queue.getJob(jobId);
    if (!job) throw new Error(`Job ${jobId} not found`);
    if (job.repeatJobKey) {
      await queue.removeRepeatableByKey(job.repeatJobKey);
    }
    await job.remove();
  }

  async promoteJob(queueName: string, jobId: string): Promise<void> {
    const queue = this.getOrCreate(queueName);
    const job = await queue.getJob(jobId);
    if (!job) throw new Error(`Job ${jobId} not found`);
    if (job.repeatJobKey) {
      await queue.add(job.name, job.data, {
        jobId: `manual:${job.name}:${job.id}`,
        priority: job.opts.priority,
        attempts: job.opts.attempts,
      });
    } else {
      await job.promote();
    }
  }

  async getWorkers(queueName: string) {
    return this.getOrCreate(queueName).getWorkers();
  }

  async getAllWorkers() {
    const names = await this.discoverQueues();
    const results = await Promise.all(
      names.map(async (name) => {
        const workers = await this.getOrCreate(name).getWorkers();
        return workers.map((w) => ({ ...w, queueName: name }));
      }),
    );
    return results.flat();
  }

  private getOrCreate(name: string): Queue {
    let q = this.queues.get(name);
    if (!q) {
      this.assertConnected();
      q = new Queue(name, {
        connection: this.connection!,
        prefix: this.prefix,
      });
      this.queues.set(name, q);
    }
    return q;
  }

  private assertConnected() {
    if (!this.connection) throw new Error("Not connected to Redis");
  }

  private async discoverQueues(): Promise<string[]> {
    this.assertConnected();
    const pattern = `${this.prefix}:*:meta`;
    const keys: string[] = [];
    let cursor = "0";
    do {
      const [next, results] = await this.connection!.scan(
        cursor,
        "MATCH",
        pattern,
        "COUNT",
        100,
      );
      cursor = next;
      keys.push(...results);
    } while (cursor !== "0");

    const names = keys
      .map((k) => k.split(":")[1] ?? "")
      .filter(Boolean);
    return [...new Set(names)];
  }
}

function mapJob(job: BullJob, queueName: string): Job {
  return {
    id: job.id ?? "",
    name: job.name,
    queueName,
    data: job.data,
    status: deriveStatus(job),
    progress: normalizeProgress(job.progress),
    attemptsMade: job.attemptsMade,
    attemptsLimit: job.opts?.attempts ?? 1,
    failedReason: job.failedReason,
    stacktrace: job.stacktrace ?? undefined,
    returnValue: job.returnvalue,
    timestamp: job.timestamp,
    processedOn: job.processedOn,
    finishedOn: job.finishedOn,
    delay: job.opts?.delay,
    priority: job.opts?.priority,
    parentId: job.parentKey?.split(":").pop(),
    repeatJobKey: job.repeatJobKey,
  };
}

function deriveStatus(job: BullJob): JobStatus {
  if (job.finishedOn && job.failedReason) return "failed";
  if (job.finishedOn) return "completed";
  if (job.processedOn) return "active";
  if (job.opts?.delay && job.timestamp + job.opts.delay > Date.now())
    return "delayed";
  return "waiting";
}

function resolveStatuses(
  status?: JobStatus | JobStatus[],
): JobType[] {
  if (!status)
    return [
      "waiting",
      "active",
      "completed",
      "failed",
      "delayed",
      "paused",
      "prioritized",
      "waiting-children",
    ];
  const arr = Array.isArray(status) ? status : [status];
  return arr as JobType[];
}

function sanitizeJobTypes(types: JobType[]): JobType[] {
  const result = [...types];
  if (result.includes("waiting") && !result.includes("paused")) {
    result.push("paused");
  }
  return [...new Set(result)];
}

function sortJobs(
  jobs: Job[],
  field: string,
  order: "asc" | "desc",
): Job[] {
  return [...jobs].sort((a, b) => {
    const av =
      field === "progress"
        ? typeof a.progress === "number"
          ? a.progress
          : 0
        : ((a as unknown as Record<string, unknown>)[field] as number) ?? 0;
    const bv =
      field === "progress"
        ? typeof b.progress === "number"
          ? b.progress
          : 0
        : ((b as unknown as Record<string, unknown>)[field] as number) ?? 0;
    return order === "asc" ? av - bv : bv - av;
  });
}

function sortSummaries(
  jobs: JobSummary[],
  field: string,
  order: "asc" | "desc",
): JobSummary[] {
  return [...jobs].sort((a, b) => {
    const av =
      field === "progress"
        ? typeof a.progress === "number"
          ? a.progress
          : 0
        : ((a as unknown as Record<string, unknown>)[field] as number) ?? 0;
    const bv =
      field === "progress"
        ? typeof b.progress === "number"
          ? b.progress
          : 0
        : ((b as unknown as Record<string, unknown>)[field] as number) ?? 0;
    return order === "asc" ? av - bv : bv - av;
  });
}

let _instance: BullMQService | null = null;

export function getService(): BullMQService {
  if (!_instance) {
    const url = process.env.REDIS_URL ?? "redis://localhost:6379";
    const prefix = process.env.BULLMQ_PREFIX ?? DEFAULT_PREFIX;
    _instance = new BullMQService(url, prefix);
  }
  return _instance;
}

export async function ensureConnected(): Promise<BullMQService> {
  const svc = getService();
  if (!svc.isConnected()) await svc.connect();
  return svc;
}
