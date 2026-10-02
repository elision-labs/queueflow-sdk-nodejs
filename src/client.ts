/**
 * The ergonomic QueueFlow client and its resource groups.
 *
 * This layer is hand-written: camelCase inputs, `create()` = enqueue+fetch,
 * `waitFor()` polling, the `watch()` SSE stream, idempotent-retry transport
 * policy, the worker run-loop, and typed errors. The wire types, the per-tag
 * API classes, and all (de)serialization come from the generated `../core`
 * and are never hand-edited.
 */

import { Configuration, ResponseError } from "../core/src/runtime";
import { JobsApi } from "../core/src/apis/JobsApi";
import { WorkflowsApi } from "../core/src/apis/WorkflowsApi";
import { WorkerApi } from "../core/src/apis/WorkerApi";
import { SystemApi } from "../core/src/apis/SystemApi";
import { CronApi } from "../core/src/apis/CronApi";
import { DlqApi } from "../core/src/apis/DlqApi";
import { HealthApi } from "../core/src/apis/HealthApi";
import { JobFromJSON } from "../core/src/models/index";
import type {
  BackoffStrategy,
  Job,
  JobStatus,
  JobConfigRequest,
  CreateJobRequest,
  CreateBatchJobsResponse,
  ListJobsResponse,
  Workflow,
  ListWorkflowsResponse,
  CreateWorkflowRequest,
  WorkflowDiagramResponse,
  WorkflowStepState,
  LeasedJob,
  StatsSnapshot,
  CronSchedule,
  ListCronsResponse,
  DeadLetter,
  ListDeadLettersResponse,
  HealthStatus,
  ReadyStatus,
} from "../core/src/models/index";
import {
  AbortError,
  ApiError,
  ConnectionError,
  TimeoutError,
  toQueueFlowError,
} from "./errors";
import { WorkflowBuilder } from "./workflow";
import type { JsonObject } from "./json";

export interface QueueFlowOptions {
  /** Base URL of the QueueFlow server, e.g. `http://localhost:8000`. */
  baseUrl: string;
  /** Bearer token. Any non-empty token authenticates against the dev server. */
  token: string;
  /**
   * Credential for the worker-protocol routes (`qf.worker`: lease, heartbeat,
   * complete, fail). Servers running with `--worker-token` refuse tenant
   * tokens on those routes. Defaults to `token`, which only works in the
   * server's development mode.
   */
  workerToken?: string;
  /** Per-request timeout in milliseconds (default 30_000). */
  timeoutMs?: number;
  /** Times to retry idempotent requests on network / 5xx errors (default 2). */
  maxRetries?: number;
  /** Inject a custom fetch (tests, proxies, polyfills). */
  fetch?: typeof fetch;
}

/** Ergonomic, camelCase input for enqueuing a job. */
export interface CreateJobInput {
  /** Registered task handler to invoke (e.g. `echo`, `sleep`). */
  task: string;
  payload?: JsonObject;
  /** Higher is dequeued first. */
  priority?: number;
  /** Max retry attempts before dead-lettering. */
  maxRetries?: number;
  /** Per-attempt timeout, in seconds. */
  timeout?: number;
  /** Override the destination queue. */
  queue?: string;
  /** How retry delays grow between attempts (default exponential). */
  retryBackoff?: BackoffStrategy;
  /** Base retry delay, in seconds. */
  retryDelaySecs?: number;
  /** Upper bound on any computed retry delay, in seconds. */
  retryMaxDelaySecs?: number;
  /** Retry-delay jitter in `0..=1` (e.g. `0.1` = +/-10%). */
  jitterFactor?: number;
  /** Makes the create idempotent per tenant (sent as `Idempotency-Key`). */
  idempotencyKey?: string;
  /** Don't run before this instant. Created immediately, invisible until then. */
  runAt?: Date | string;
}

/** Filters for the list endpoints. */
export interface ListOptions {
  status?: string;
  queue?: string;
  limit?: number;
  offset?: number;
  orderBy?: "created_at ASC" | "created_at DESC";
  includeTotal?: boolean;
  /**
   * Opaque keyset cursor from a previous page's `next_cursor`. When set,
   * `offset` is ignored and listing continues where that page ended; cheaper
   * than deep OFFSET paging.
   */
  cursor?: string;
  /** Only rows created at or after this instant (inclusive). */
  createdAfter?: Date | string;
  /** Only rows created strictly before this instant (exclusive). */
  createdBefore?: Date | string;
}

function toDate(v: Date | string | undefined): Date | undefined {
  return v === undefined ? undefined : v instanceof Date ? v : new Date(v);
}

/** Options for the `waitFor` pollers. */
export interface WaitOptions {
  /** Give up after this many ms (default 60_000). Throws {@link TimeoutError}. */
  timeoutMs?: number;
  /** Delay between polls in ms (default 500). */
  intervalMs?: number;
  /** Abort the wait early. */
  signal?: AbortSignal;
}

const TERMINAL_JOB_STATUSES = new Set<string>(["completed", "failed", "cancelled"]);
const TERMINAL_WORKFLOW_STATUSES = new Set<string>([
  "completed",
  "failed",
  "partially_failed",
  "cancelled",
]);
const RETRYABLE_STATUS = new Set([502, 503, 504]);

/** Shared transport policy layered over the generated core. */
class Transport {
  readonly baseUrl: string;
  readonly token: string;
  readonly fetchImpl: typeof fetch;
  readonly config: Configuration;
  /** Like `config`, but authenticated with the worker credential. */
  readonly workerConfig: Configuration;
  private readonly timeoutMs: number;
  private readonly retries: number;

  constructor(opts: QueueFlowOptions) {
    if (!opts.baseUrl) throw new Error("QueueFlow: `baseUrl` is required");
    if (!opts.token) throw new Error("QueueFlow: `token` is required");
    this.baseUrl = opts.baseUrl.replace(/\/+$/, "");
    this.token = opts.token;
    this.timeoutMs = opts.timeoutMs ?? 30_000;
    this.retries = opts.maxRetries ?? 2;
    const f = opts.fetch ?? globalThis.fetch;
    if (!f) throw new Error("QueueFlow: no global `fetch`; pass one in options.");
    this.fetchImpl = f;
    this.config = new Configuration({
      basePath: this.baseUrl,
      accessToken: this.token,
      fetchApi: this.fetchImpl,
    });
    this.workerConfig = new Configuration({
      basePath: this.baseUrl,
      accessToken: opts.workerToken ?? this.token,
      fetchApi: this.fetchImpl,
    });
  }

  private initOverrides(timeoutMs?: number): RequestInit | undefined {
    const ms = timeoutMs ?? this.timeoutMs;
    return ms > 0 ? { signal: AbortSignal.timeout(ms) } : undefined;
  }

  /** Run a generated-core call with typed-error mapping and idempotent retry. */
  async call<T>(
    label: string,
    fn: (init?: RequestInit) => Promise<T>,
    opts: { idempotent?: boolean; timeoutMs?: number } = {},
  ): Promise<T> {
    const attempts = (opts.idempotent ? this.retries : 0) + 1;
    let last: Error | undefined;
    for (let attempt = 0; attempt < attempts; attempt++) {
      try {
        return await fn(this.initOverrides(opts.timeoutMs));
      } catch (raw) {
        const err = await toQueueFlowError(raw, label);
        const retryable =
          err instanceof ConnectionError ||
          (err instanceof ApiError && RETRYABLE_STATUS.has(err.status));
        if (!retryable || attempt === attempts - 1) throw err;
        last = err;
        await sleep(backoffMs(attempt));
      }
    }
    throw last;
  }
}


/** Ergonomic, camelCase input for registering a cron schedule. */
export interface CreateCronInput {
  /** Unique schedule name (per tenant). */
  name: string;
  /** 5-field crontab, evaluated in UTC (6/7 fields with leading seconds also accepted). */
  schedule: string;
  /** Registered task handler to enqueue on each firing. */
  task: string;
  payload?: JsonObject;
  /** Queue for the enqueued jobs (server default when omitted). */
  queue?: string;
}

/** Job lifecycle: enqueue, fetch, list, cancel, wait, watch. */
export class JobsResource {
  private readonly api: JobsApi;
  constructor(private readonly t: Transport) {
    this.api = new JobsApi(t.config);
  }

  /** Enqueue a job and return its freshly-created record. */
  async create(input: CreateJobInput): Promise<Job> {
    return this.get(await this.enqueue(input));
  }

  /** Enqueue without a follow-up fetch; returns just the new job id. */
  async enqueue(input: CreateJobInput): Promise<string> {
    const res = await this.t.call(
      "createJob",
      (init) =>
        this.api.createJob(
          { createJobRequest: toCreateJobRequest(input), idempotencyKey: input.idempotencyKey },
          init,
        ),
      { idempotent: input.idempotencyKey !== undefined },
    );
    return res.job_id;
  }

  /** Enqueue up to 1000 jobs in one call. */
  createBatch(inputs: CreateJobInput[]): Promise<CreateBatchJobsResponse> {
    return this.t.call("createBatchJobs", (init) =>
      this.api.createBatchJobs(
        { createBatchJobsRequest: { jobs: inputs.map(toCreateJobRequest) } },
        init,
      ),
    );
  }

  get(id: string): Promise<Job> {
    return this.t.call("getJob", (init) => this.api.getJob({ id }, init), {
      idempotent: true,
    });
  }

  list(opts: ListOptions = {}): Promise<ListJobsResponse> {
    return this.t.call(
      "listJobs",
      (init) =>
        this.api.listJobs(
          {
            status: opts.status,
            queue: opts.queue,
            limit: opts.limit,
            offset: opts.offset,
            orderBy: opts.orderBy,
            includeTotal: opts.includeTotal,
            cursor: opts.cursor,
            createdAfter: toDate(opts.createdAfter),
            createdBefore: toDate(opts.createdBefore),
          },
          init,
        ),
      { idempotent: true },
    );
  }

  cancel(id: string): Promise<void> {
    return this.t.call("cancelJob", (init) => this.api.cancelJob({ id }, init));
  }

  /** Poll until the job reaches a terminal state (completed/failed/cancelled). */
  waitFor(id: string, opts: WaitOptions = {}): Promise<Job> {
    return poll(() => this.get(id), (j) => TERMINAL_JOB_STATUSES.has(j.status), id, "job", opts);
  }

  /**
   * Stream a job's status changes (SSE from `/api/v1/jobs/{id}/events`). The
   * generated client returns this endpoint as an opaque `string`, so the stream
   * is hand-implemented here, but each event is decoded with the generated
   * `JobFromJSON` so the yielded shape matches `get()` exactly (dates included).
   */
  async *watch(id: string, opts: { signal?: AbortSignal } = {}): AsyncGenerator<Job> {
    const url = `${this.t.baseUrl}/api/v1/jobs/${encodeURIComponent(id)}/events`;
    const res = await this.t.fetchImpl(url, {
      method: "GET",
      headers: { authorization: `Bearer ${this.t.token}`, accept: "text/event-stream" },
      signal: opts.signal,
    });
    if (!res.ok || !res.body) {
      throw await toQueueFlowError(new ResponseError(res), "streamJobEvents");
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let event = "message";
    let data: string[] = [];
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let idx: number;
        while ((idx = buffer.indexOf("\n")) >= 0) {
          const line = buffer.slice(0, idx).replace(/\r$/, "");
          buffer = buffer.slice(idx + 1);
          if (line === "") {
            if (data.length && event === "status") {
              const job = JobFromJSON(JSON.parse(data.join("\n")));
              yield job;
              if (TERMINAL_JOB_STATUSES.has(job.status)) return;
            }
            event = "message";
            data = [];
          } else if (line.startsWith("event:")) {
            event = line.slice(6).trim();
          } else if (line.startsWith("data:")) {
            // Per the SSE spec, strip at most ONE leading space; further
            // whitespace is payload.
            let value = line.slice(5);
            if (value.startsWith(" ")) value = value.slice(1);
            data.push(value);
          }
        }
      }
    } finally {
      reader.releaseLock();
    }
  }
}

/** Workflow orchestration: create DAGs, fetch, list, cancel, diagram, wait. */
export class WorkflowsResource {
  private readonly api: WorkflowsApi;
  constructor(private readonly t: Transport) {
    this.api = new WorkflowsApi(t.config);
  }

  /** Create a workflow from a {@link WorkflowBuilder} or a raw request body. */
  async create(workflow: WorkflowBuilder | CreateWorkflowRequest): Promise<Workflow> {
    const body = workflow instanceof WorkflowBuilder ? workflow.build() : workflow;
    const res = await this.t.call("createWorkflow", (init) =>
      this.api.createWorkflow({ createWorkflowRequest: body }, init),
    );
    return this.get(res.workflow_id);
  }

  get(id: string): Promise<Workflow> {
    return this.t.call("getWorkflow", (init) => this.api.getWorkflow({ id }, init), {
      idempotent: true,
    });
  }

  list(opts: ListOptions = {}): Promise<ListWorkflowsResponse> {
    return this.t.call(
      "listWorkflows",
      (init) =>
        this.api.listWorkflows(
          {
            status: opts.status,
            queue: opts.queue,
            limit: opts.limit,
            offset: opts.offset,
            orderBy: opts.orderBy,
            includeTotal: opts.includeTotal,
            cursor: opts.cursor,
            createdAfter: toDate(opts.createdAfter),
            createdBefore: toDate(opts.createdBefore),
          },
          init,
        ),
      { idempotent: true },
    );
  }

  cancel(id: string): Promise<void> {
    return this.t.call("cancelWorkflow", (init) => this.api.cancelWorkflow({ id }, init));
  }

  /** Fetch the Mermaid (`graph TD`) diagram for a workflow's DAG. */
  diagram(id: string): Promise<WorkflowDiagramResponse> {
    return this.t.call(
      "getWorkflowDiagram",
      (init) => this.api.getWorkflowDiagram({ id }, init),
      { idempotent: true },
    );
  }

  /**
   * Runtime status of every step, in declaration order — the live progress
   * view (`get()` returns the step *definitions* only). Each entry carries
   * the step's status and, once scheduled, the id of the job executing it.
   */
  async steps(id: string): Promise<WorkflowStepState[]> {
    const res = await this.t.call(
      "getWorkflowStepStates",
      (init) => this.api.getWorkflowStepStates({ id }, init),
      { idempotent: true },
    );
    return res.steps;
  }

  /** Poll until the workflow reaches a terminal state. */
  waitFor(id: string, opts: WaitOptions = {}): Promise<Workflow> {
    return poll(
      () => this.get(id),
      (w) => TERMINAL_WORKFLOW_STATUSES.has(w.status),
      id,
      "workflow",
      opts,
    );
  }
}

/**
 * The remote worker protocol: lease jobs, heartbeat while running, report
 * completion/failure. Lets TypeScript handlers execute QueueFlow jobs without
 * living in the Rust server binary; retries, the DLQ, and workflow advancement
 * all stay server-side.
 */
export class WorkerResource {
  private readonly api: WorkerApi;
  constructor(private readonly t: Transport) {
    // Worker routes take the worker credential, not the tenant token.
    this.api = new WorkerApi(t.workerConfig);
  }

  /** Lease up to `maxJobs` jobs, long-polling up to `waitSecs` when empty. */
  async lease(
    queue: string,
    opts: { maxJobs?: number; leaseSecs?: number; waitSecs?: number } = {},
  ): Promise<LeasedJob[]> {
    const res = await this.t.call(
      "leaseJobs",
      (init) =>
        this.api.leaseJobs(
          {
            queue,
            leaseJobsRequest: {
              max_jobs: opts.maxJobs,
              lease_secs: opts.leaseSecs,
              wait_secs: opts.waitSecs,
            },
          },
          init,
        ),
      // Leasing is replay-safe; a long poll must outlive the default timeout.
      { idempotent: true, timeoutMs: ((opts.waitSecs ?? 0) + 35) * 1_000 },
    );
    return res.jobs;
  }

  /** Extend a lease. Returns the job's current status (`running` = extended). */
  async heartbeat(lease: LeasedJob, extendSecs: number): Promise<JobStatus> {
    const res = await this.t.call(
      "heartbeatJob",
      (init) =>
        this.api.heartbeatJob(
          { id: lease.job.id, heartbeatRequest: { lease_token: lease.lease_token, extend_secs: extendSecs } },
          init,
        ),
      { idempotent: true },
    );
    return res.status;
  }

  /** Report success. Replaying against a finished job is a server-side no-op. */
  complete(lease: LeasedJob, result: JsonObject = {}): Promise<void> {
    return this.t.call(
      "completeJob",
      (init) =>
        this.api.completeJob(
          { id: lease.job.id, completeJobRequest: { lease_token: lease.lease_token, result } },
          init,
        ),
      { idempotent: true },
    );
  }

  /** Report failure; the server applies the job's retry/dead-letter policy. */
  fail(lease: LeasedJob, error: string, opts: { retryable?: boolean } = {}): Promise<void> {
    return this.t.call(
      "failJob",
      (init) =>
        this.api.failJob(
          {
            id: lease.job.id,
            failJobRequest: { lease_token: lease.lease_token, error, retryable: opts.retryable ?? true },
          },
          init,
        ),
      { idempotent: true },
    );
  }

  /**
   * Run a worker loop: lease, dispatch to `handlers` by task name, heartbeat
   * while the handler runs, and report the outcome. Resolves when `signal`
   * aborts; throws on 401/403 from the lease call (a wrong or missing worker
   * token cannot heal by retrying). Handlers should be idempotent (delivery
   * is at-least-once) and should honour `ctx.signal`, which aborts when the
   * job's lease is lost (cancelled mid-run or reclaimed) — from then on the
   * server owns the outcome and any further work is wasted.
   */
  async run(
    queue: string,
    handlers: Record<string, WorkerHandler>,
    opts: {
      leaseSecs?: number;
      waitSecs?: number;
      signal?: AbortSignal;
      /** Called on transient lease errors (default: throttled console.warn). */
      onError?: (error: Error) => void;
    } = {},
  ): Promise<void> {
    const leaseSecs = opts.leaseSecs ?? 30;
    const waitSecs = opts.waitSecs ?? 20;
    let failures = 0;
    while (!opts.signal?.aborted) {
      let leases: LeasedJob[];
      try {
        leases = await this.lease(queue, { maxJobs: 1, leaseSecs, waitSecs });
        failures = 0;
      } catch (err) {
        const error = err instanceof Error ? err : new Error(String(err));
        // Auth errors cannot heal by retrying: surface them instead of
        // spinning silently at 1 req/s with a bad or missing worker token.
        if (error instanceof ApiError && (error.status === 401 || error.status === 403)) {
          throw error;
        }
        failures += 1;
        if (opts.onError) {
          opts.onError(error);
        } else if (failures === 1 || failures % 30 === 0) {
          console.warn(
            `[queueflow] worker lease failed (${failures} consecutive): ${error.message}`,
          );
        }
        await sleep(1_000, opts.signal);
        continue;
      }
      for (const lease of leases) {
        await this.runOne(lease, handlers, leaseSecs);
      }
    }
  }

  private async runOne(
    lease: LeasedJob,
    handlers: Record<string, WorkerHandler>,
    leaseSecs: number,
  ): Promise<void> {
    const handler = handlers[lease.job.task_name];
    if (!handler) {
      await this.fail(lease, `no remote handler for task '${lease.job.task_name}'`, {
        retryable: false,
      }).catch(() => {});
      return;
    }
    // Heartbeat at half the lease interval. A non-running status (or a 409
    // lost-lease) means the server owns the outcome: abort the handler so it
    // can stop, and report nothing.
    const lost = new AbortController();
    const ticker = setInterval(() => {
      void this.heartbeat(lease, leaseSecs)
        .then((status) => {
          if (status !== "running") lost.abort();
        })
        .catch((err: unknown) => {
          if ((err as { status?: number }).status === 409) lost.abort();
        });
    }, Math.max(1, leaseSecs / 2) * 1_000);

    // Handler outcome and outcome *reporting* are separate concerns: a
    // reporting error must never be re-reported as a job failure (that would
    // burn retry budget on a transport blip and drop a successful result).
    let outcome:
      | { ok: true; result: JsonObject }
      | { ok: false; error: string; retryable: boolean };
    try {
      outcome = { ok: true, result: await handler(lease.job, { signal: lost.signal }) };
    } catch (err) {
      outcome = {
        ok: false,
        error: err instanceof Error ? err.message : String(err),
        // NonRetryableError (or any error carrying `retryable: false`) sends
        // the job straight to the dead-letter queue.
        retryable: (err as { retryable?: boolean } | null)?.retryable !== false,
      };
    } finally {
      clearInterval(ticker);
    }
    if (lost.signal.aborted) return; // the server owns the outcome

    const report = outcome.ok
      ? this.complete(lease, outcome.result)
      : this.fail(lease, outcome.error, { retryable: outcome.retryable });
    await report.catch((err: unknown) => {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(
        `[queueflow] failed to report job ${lease.job.id} outcome; the lease will expire and the server redelivers: ${msg}`,
      );
    });
  }
}

/** Per-job context handed to worker handlers. */
export interface WorkerContext {
  /**
   * Aborts when the job's lease is lost (cancelled mid-run, or reclaimed
   * after expiry). The server owns the outcome from then on; stop working.
   */
  signal: AbortSignal;
}

/** A worker task handler. Delivery is at-least-once: make it idempotent. */
export type WorkerHandler = (job: Job, ctx: WorkerContext) => Promise<JsonObject>;

/** Recurring enqueues on a cron schedule (UTC): create, list, pause, resume. */
export class CronResource {
  private readonly api: CronApi;
  constructor(private readonly t: Transport) {
    this.api = new CronApi(t.config);
  }

  /** Register a schedule and return its freshly-created record. */
  async create(input: CreateCronInput): Promise<CronSchedule> {
    const res = await this.t.call("createCron", (init) =>
      this.api.createCron(
        {
          createCronRequest: {
            name: input.name,
            cron_expr: input.schedule,
            task_name: input.task,
            payload: input.payload,
            queue: input.queue,
          },
        },
        init,
      ),
    );
    return this.get(res.cron_id);
  }

  list(opts: ListOptions = {}): Promise<ListCronsResponse> {
    return this.t.call(
      "listCrons",
      (init) =>
        this.api.listCrons(
          {
            limit: opts.limit,
            offset: opts.offset,
            orderBy: opts.orderBy,
            includeTotal: opts.includeTotal,
            cursor: opts.cursor,
            createdAfter: toDate(opts.createdAfter),
            createdBefore: toDate(opts.createdBefore),
          },
          init,
        ),
      { idempotent: true },
    );
  }

  get(id: string): Promise<CronSchedule> {
    return this.t.call("getCron", (init) => this.api.getCron({ id }, init), {
      idempotent: true,
    });
  }

  /** Delete a schedule; already-enqueued jobs are unaffected. */
  delete(id: string): Promise<void> {
    return this.t.call("deleteCron", (init) => this.api.deleteCron({ id }, init), {
      idempotent: true,
    });
  }

  /** Stop firings until {@link CronResource.resume}. */
  pause(id: string): Promise<void> {
    return this.t.call("pauseCron", (init) => this.api.pauseCron({ id }, init));
  }

  /** Resume firings at the next future occurrence (missed runs are skipped). */
  resume(id: string): Promise<void> {
    return this.t.call("resumeCron", (init) => this.api.resumeCron({ id }, init));
  }
}

/** Dead-letter queue: inspect terminally-failed jobs and replay them. */
export class DlqResource {
  private readonly api: DlqApi;
  constructor(private readonly t: Transport) {
    this.api = new DlqApi(t.config);
  }

  list(opts: ListOptions = {}): Promise<ListDeadLettersResponse> {
    return this.t.call(
      "listDeadLetters",
      (init) =>
        this.api.listDeadLetters(
          {
            queue: opts.queue,
            limit: opts.limit,
            offset: opts.offset,
            orderBy: opts.orderBy,
            includeTotal: opts.includeTotal,
            cursor: opts.cursor,
            createdAfter: toDate(opts.createdAfter),
            createdBefore: toDate(opts.createdBefore),
          },
          init,
        ),
      { idempotent: true },
    );
  }

  get(id: number): Promise<DeadLetter> {
    return this.t.call("getDeadLetter", (init) => this.api.getDeadLetter({ id }, init), {
      idempotent: true,
    });
  }

  /**
   * Replay a dead-lettered job as a fresh, detached job; returns the new job
   * id. Each entry replays at most once (a second replay is a 409
   * {@link ConflictError}).
   */
  async replay(id: number): Promise<string> {
    const res = await this.t.call("replayDeadLetter", (init) =>
      this.api.replayDeadLetter({ id }, init),
    );
    return res.job_id;
  }
}

/** Engine introspection: counters and registered task handlers. */
export class SystemResource {
  private readonly api: SystemApi;
  constructor(private readonly t: Transport) {
    this.api = new SystemApi(t.config);
  }

  stats(): Promise<StatsSnapshot> {
    return this.t.call("getStats", (init) => this.api.getStats(init), { idempotent: true });
  }

  /** Names of the task handlers registered on the server. */
  async tasks(): Promise<string[]> {
    const res = await this.t.call("listTasks", (init) => this.api.listTasks(init), {
      idempotent: true,
    });
    return res.tasks;
  }
}

/**
 * The QueueFlow client.
 *
 * ```ts
 * const qf = new QueueFlow({ baseUrl: "http://localhost:8000", token: "dev" });
 * const job = await qf.jobs.create({ task: "echo", payload: { hi: 1 } });
 * const done = await qf.jobs.waitFor(job.id);
 * ```
 */
export class QueueFlow {
  readonly jobs: JobsResource;
  readonly workflows: WorkflowsResource;
  readonly worker: WorkerResource;
  readonly cron: CronResource;
  readonly dlq: DlqResource;
  readonly system: SystemResource;
  private readonly transport: Transport;
  private readonly health_: HealthApi;

  constructor(options: QueueFlowOptions) {
    const transport = new Transport(options);
    this.transport = transport;
    this.jobs = new JobsResource(transport);
    this.workflows = new WorkflowsResource(transport);
    this.worker = new WorkerResource(transport);
    this.cron = new CronResource(transport);
    this.dlq = new DlqResource(transport);
    this.system = new SystemResource(transport);
    this.health_ = new HealthApi(transport.config);
  }

  /** Liveness probe (`GET /health`). Same timeout/retry/error policy as every other call. */
  health(): Promise<HealthStatus> {
    return this.transport.call("getHealth", (init) => this.health_.getHealth(init), {
      idempotent: true,
    });
  }

  /** Readiness probe (`GET /ready`). Same timeout/retry/error policy as every other call. */
  ready(): Promise<ReadyStatus> {
    return this.transport.call("getReady", (init) => this.health_.getReady(init), {
      idempotent: true,
    });
  }
}

function toJobConfigRequest(input: CreateJobInput): JobConfigRequest | undefined {
  const config: JobConfigRequest = {};
  if (input.priority !== undefined) config.priority = input.priority;
  if (input.maxRetries !== undefined) config.max_retries = input.maxRetries;
  if (input.timeout !== undefined) config.timeout = input.timeout;
  if (input.queue !== undefined) config.queue = input.queue;
  if (input.retryBackoff !== undefined) config.retry_backoff = input.retryBackoff;
  if (input.retryDelaySecs !== undefined) config.retry_delay_secs = input.retryDelaySecs;
  if (input.retryMaxDelaySecs !== undefined) config.retry_max_delay_secs = input.retryMaxDelaySecs;
  if (input.jitterFactor !== undefined) config.jitter_factor = input.jitterFactor;
  return Object.keys(config).length ? config : undefined;
}

function toCreateJobRequest(input: CreateJobInput): CreateJobRequest {
  const req: CreateJobRequest = { task_name: input.task };
  if (input.payload) req.payload = input.payload;
  const config = toJobConfigRequest(input);
  if (config) req.config = config;
  if (input.runAt !== undefined) {
    req.run_at = input.runAt instanceof Date ? input.runAt : new Date(input.runAt);
  }
  return req;
}

async function poll<T>(
  fetchOne: () => Promise<T>,
  isTerminal: (value: T) => boolean,
  id: string,
  kind: string,
  opts: WaitOptions,
): Promise<T> {
  const timeoutMs = opts.timeoutMs ?? 60_000;
  const intervalMs = opts.intervalMs ?? 500;
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (opts.signal?.aborted) throw new AbortError(`waitFor(${kind} ${id}) aborted`);
    const value = await fetchOne();
    if (isTerminal(value)) return value;
    if (Date.now() + intervalMs > deadline) {
      throw new TimeoutError(
        `${kind} ${id} did not reach a terminal state within ${timeoutMs}ms`,
        id,
      );
    }
    await sleep(intervalMs, opts.signal);
  }
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener("abort", () => {
      clearTimeout(timer);
      resolve();
    }, { once: true });
  });
}

function backoffMs(attempt: number): number {
  return Math.min(2_000, 100 * 2 ** attempt) + Math.floor(Math.random() * 100);
}
