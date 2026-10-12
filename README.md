# @queueflow/sdk

Ergonomic TypeScript/JavaScript client for [QueueFlow](https://queueflow.dev) — a
PostgreSQL-native distributed job queue and workflow engine.

- **Typed end-to-end** — request/response shapes mirror the server's OpenAPI 3.1 spec.
- **Ergonomic** — `qf.jobs.create({ task, payload })`, a `waitFor()` poller, and a workflow builder DSL.
- **Typed errors** — `NotFoundError`, `UnauthorizedError`, `BadRequestError`, `TimeoutError`, …
- **Zero runtime dependencies** — uses the built-in `fetch` (Node ≥ 18), with retries and timeouts.
- **Dual ESM + CJS**, ships its own `.d.ts`.

> Built as a thin hand-written facade (`src/`) over a generated core (`core/`: models + transport
> from the [OpenAPI spec](https://github.com/elision-labs/queueflow-core/blob/main/spec/openapi.yaml)).
> The core is regenerated and never drifts from the server; the facade adds the ergonomics codegen
> cannot. See [Architecture](#architecture).

## Install

```bash
npm install @queueflow/sdk
```

## Quick start

```ts
import { QueueFlow, wf } from "@queueflow/sdk";

const qf = new QueueFlow({
  baseUrl: "http://localhost:8000",
  token: process.env.QUEUEFLOW_TOKEN ?? "dev",
});

// Enqueue a job and wait for the result.
const job = await qf.jobs.create({
  task: "echo",
  payload: { hello: "world" },
  maxRetries: 3,
  timeout: 30, // seconds
});

const done = await qf.jobs.waitFor(job.id);
console.log(done.status, done.result);

// Declare and run a DAG workflow.
const workflow = await qf.workflows.create(
  wf("etl")
    .step("extract", "echo")
    .step("transform", "echo", { after: ["extract"] })
    .step("load", "echo", { after: ["transform"], onFailure: "halt" }),
);

const finished = await qf.workflows.waitFor(workflow.id);
console.log(finished.status, finished.context);
```

## API

### Client

```ts
const qf = new QueueFlow({
  baseUrl,            // required
  token,              // required — any non-empty token on the dev server
  workerToken,        // credential for qf.worker routes (defaults to token; dev mode only)
  timeoutMs,          // per-request timeout (default 30_000)
  maxRetries,         // retries for idempotent calls on network/5xx (default 2)
  fetch,              // inject a custom fetch (tests, proxies)
});

await qf.health();    // GET /health
await qf.ready();     // GET /ready
```

Methods marked **0.5.0** below call endpoints added in QueueFlow server 0.5.0 (bulk cancel,
bulk replay, queue pause/resume, `whoami`, and the `taskName` / `replayed` list filters); an
older server answers them with a 404. Methods and fields marked **0.6.0** need server 0.6.0
(workflow retry and events, and the `queue`, `when`, `fan_out`, `workflow` and `timeout_secs`
workflow fields); an older server rejects the new request fields with a 400.

### Jobs — `qf.jobs`

| Method | Description |
| --- | --- |
| `create(input)` | Enqueue a job, returns the created `Job`. |
| `enqueue(input)` | Enqueue and return just the new job id (no follow-up fetch). |
| `createBatch(inputs)` | Enqueue up to 1000 jobs at once. |
| `get(id)` | Fetch a job. |
| `list(opts?)` | List jobs (`status`, `queue`, `taskName` (0.5.0), `limit`, `offset`, `orderBy`, `cursor`, `createdAfter`, `createdBefore`). |
| `cancel(id)` | Cancel a job. |
| `cancelMany(filter?)` | **0.5.0** Cancel every non-terminal job matching the filter, oldest first, up to `limit`; returns the count. |
| `waitFor(id, opts?)` | Poll until `completed` / `failed` / `cancelled`. |
| `watch(id, opts?)` | Async-iterate the job's status changes (SSE); ends at a terminal state. |

`input` is `{ task, payload?, priority?, maxRetries?, timeout?, queue?, retryBackoff?,
retryDelaySecs?, retryMaxDelaySecs?, jitterFactor?, idempotencyKey?, runAt? }`.

List responses carry `next_cursor` when there are more pages; pass it back as
`cursor` for keyset pagination (cheaper than deep `offset`).

`cancelMany(filter)` takes `{ queue?, taskName?, status?, createdAfter?, createdBefore?, limit? }`.
`status` must be `pending`, `retrying` or `running` (all three when omitted); a terminal status is
a `BadRequestError`, since history is never rewritten. `limit` defaults to 100 and is capped by
the server's `--max-batch`; a result below `limit` means the matching set is exhausted, so loop
until then to drain a large set:

```ts
// Cancel everything still waiting on a queue.
while ((await qf.jobs.cancelMany({ queue: "billing", status: "pending", limit: 500 })) === 500) {}
```

### Workflows — `qf.workflows`

| Method | Description |
| --- | --- |
| `create(builderOrBody)` | Create a workflow from a `wf()` builder or a raw request. |
| `get(id)` · `list(opts?)` · `cancel(id)` | Fetch / list / cancel. |
| `retry(id)` | **0.6.0** Re-run a `failed`, `partially_failed` or `cancelled` workflow from where it stopped. |
| `diagram(id)` | Mermaid (`graph TD`) diagram of the DAG. |
| `steps(id)` | Runtime state of every step, in declaration order (see below). |
| `waitFor(id, opts?)` | Poll until a terminal workflow state. |
| `watch(id, opts?)` | **0.6.0** Async-iterate `WorkflowEvent`s (SSE); ends after the terminal one. |

The `Workflow` record carries, since **0.6.0**, `error_message` (why it ended `failed` or
`cancelled`), `timeout_secs` and `deadline_at` (the whole-workflow budget and when it runs out),
`retry_count`, and `parent_workflow_id` / `parent_step_name` on a child spawned by a
sub-workflow step.

#### Step state — `steps(id)`

Each entry is `{ name, status, job_id?, error?, started_at?, completed_at?, child_workflow_id?,
fan_out? }`. `status` is `pending`, `running`, `completed`, `failed`, `cancelled` or `skipped`;
`job_id` is the job executing the step (the first child, for a fan-out). The rest are **0.6.0**:

| Field | Meaning |
| --- | --- |
| `error` | Why the step failed, was skipped (`condition not met` for a false `when`) or was cancelled. |
| `started_at` · `completed_at` | `Date`s: when the step was scheduled and when it reached a terminal status. |
| `child_workflow_id` | The child workflow of a sub-workflow step, once spawned. Pass it to `get()` / `steps()`. |
| `fan_out` | `{ total, completed, failed }` progress of a fan-out step's children. |

#### Retry — `retry(id)` (**0.6.0**)

Every step that did not complete (failed, cancelled, or skipped by a failure policy) goes back to
`pending` and is scheduled again as its dependencies allow; completed steps and
condition-skipped steps keep their results and do not run again. The workflow becomes `running`,
its `error_message` clears, `retry_count` increments, and the deadline, if any, is re-armed from
the original `timeout_secs`. Retrying a running or completed workflow throws `ConflictError`
(409). Fix the cause first: a retry with the same inputs fails the same way.

```ts
const failed = await qf.workflows.waitFor(id);
if (failed.status === "failed") {
  console.error(failed.error_message);
  await qf.workflows.retry(id);
  const second = await qf.workflows.waitFor(id); // second.retry_count === 1
}
```

#### Events — `watch(id)` (**0.6.0**)

Streams `GET /api/v1/workflows/{id}/events` over Server-Sent Events, in the same style as
`qf.jobs.watch()`. Each event is a `WorkflowEvent`: `{ workflow_id, status, error_message?,
steps }`, where `steps` is the same array `steps(id)` returns. The first event is the current
state, one follows whenever the workflow or any step changes, and the iterator ends after the
terminal event (the server also closes the stream after 15 minutes; reconnect to keep following).
Pass `{ signal }` to stop early.

```ts
for await (const event of qf.workflows.watch(id)) {
  const running = event.steps.filter((s) => s.status === "running").map((s) => s.name);
  console.log(event.status, running.join(", "));
}
```

### Workflow builder — `wf()`

```ts
import { wf } from "@queueflow/sdk";

const dag = wf("order_123")
  .step("validate", "validate_order")
  .step("pay", "process_payment", { after: ["validate"] })
  .step("ship", "create_shipment", { after: ["pay"], onFailure: "continue" })
  .context({ source: "web" });
// .build() runs locally first: duplicate names, dangling deps, and cycles throw early.
```

`step(name, task, options?)` takes `{ after?, payload?, config?, onFailure?, onSuccess?,
metadata?, queue?, when?, fanOut? }`. The builder produces the server's `CreateWorkflowRequest`
(`build()`), and `qf.workflows.create()` accepts the builder directly.

#### Queues, conditions, fan-out, sub-workflows, timeouts (**0.6.0**)

| Addition | Meaning |
| --- | --- |
| `queue` | Queue for the step's job(s); the server's default queue when absent. Run your own `qf.worker.run(queue, …)` against it. |
| `when` | A `Condition` over the context, evaluated when the step becomes ready. False skips the step (`condition not met`); its dependents still run, without a result for it. |
| `fanOut: { path, itemKey?, maxItems? }` | Run the task once per element of the array at `path` in the context. Each child gets the element under `itemKey` (default `item`) and its position under `_index`; the step's result is `{ results: [...] }` in element order. |
| `.subWorkflow(name, child, options?)` | A step that runs an inline child workflow (another `wf()` builder or a raw body) instead of a task. `options` is `StepOptions` without `fanOut`. |
| `.timeout(secs)` | Whole-workflow budget. When it runs out every unfinished step is cancelled and the workflow fails with `workflow timed out after Ns`; a retry re-arms it. |

A `Condition` is a leaf `{ path, op?, value? }` or a branch `{ all: [...] }`, `{ any: [...] }`,
`{ not: {...} }`. `path` is dotted (`validate.ok` is the `ok` key of the `validate` step's result;
numeric segments index arrays). `op` is one of `eq` (default with a `value`), `ne`, `gt`, `gte`,
`lt`, `lte`, `exists` (default without a `value`), `contains`, `in`. A missing path never errors;
it compares as absent.

```ts
const fulfilment = wf("fulfilment")
  .step("pick", "pick_items", { queue: "warehouse" })
  .step("pack", "pack_parcel", { queue: "warehouse", after: ["pick"] })
  .timeout(1800);

const order = wf("order_123")
  .context({ source: "web" })
  .step("validate", "validate_order", { queue: "orders", payload: { order_id: 123 } })
  .step("fraud", "fraud_check", { queue: "orders", after: ["validate"] })
  .step("review", "queue_for_review", {
    queue: "orders",
    after: ["fraud"],
    when: { any: [
      { path: "fraud.score", op: "gt", value: 0.8 },
      { all: [{ path: "validate.total_cents", op: "gte", value: 100000 },
              { not: { path: "customer.trusted", value: true } }] },
    ] },
  })
  .step("resize", "resize_image", {
    queue: "images",
    after: ["validate"],
    fanOut: { path: "validate.images", itemKey: "image" },
    onFailure: "continue",
  })
  .subWorkflow("fulfil", fulfilment, { after: ["fraud", "review"], payload: { warehouse: "eu-1" } })
  .timeout(3600);

const run = await qf.workflows.create(order);
```

Inside a handler, a fan-out child reads `job.payload.image` and `job.payload._index`; a
sub-workflow's steps read the parent's results under `job.payload._context._parent.<step>`
and the step payload at `job.payload._context.warehouse`. Downstream of `fulfil`, the child's
final context is `_context.fulfil`, so `_context.fulfil.pack` is the `pack` step's result.

`build()` also rejects, before any network call: a step with neither a task nor a sub-workflow,
a fan-out on a sub-workflow step, a `fanOut` without a `path`, a malformed `when` (a leaf needs a
`path`; `op`s other than `exists` need a `value`; a branch holds exactly one of `all` / `any` /
`not`), and a `timeout` that is not a whole number of seconds between 1 and 30 days. Errors
name the step, as the server's do.

### System — `qf.system`

```ts
await qf.system.stats();   // engine counters
await qf.system.tasks();   // registered task handler names
await qf.system.queues();  // live per-queue backlog: pending, scheduled, running, oldest pending age, paused
await qf.system.whoami();  // 0.5.0: { tenant, access: "read_write" | "read_only" } for this token

await qf.system.pauseQueue("billing");   // 0.5.0: { queue, paused: true, running, pending }
await qf.system.resumeQueue("billing");  // 0.5.0: { queue, paused: false, running, pending }
```

`queues()` (`GET /api/v1/queues`) lists the live backlog per queue so you can decide whether to
add workers. Since server 0.5.0 it lists every queue the tenant has enqueued to (with zero counts
once drained) plus any queue with non-terminal jobs, and each entry carries a `paused` flag.

`pauseQueue(name)` stops claims of this tenant's jobs on that queue: pending jobs wait, running
jobs finish, so the queue is drained once the returned `running` reaches zero. `resumeQueue(name)`
lets claims flow again and wakes parked workers. Both are idempotent and return the queue's live
counts with its new flag; an invalid queue name is a `BadRequestError`.

### Worker — `qf.worker`

Run task handlers in this process against a remote QueueFlow server:

```ts
const controller = new AbortController();
process.on("SIGTERM", () => controller.abort());

const { unfinished } = await qf.worker.run(
  "default",
  {
    "send-email": async (job, ctx) => {
      // ctx.signal aborts if the job is cancelled mid-run, the lease is lost,
      // or the worker gives up waiting for this handler while stopping.
      await sendEmail(job.payload, { signal: ctx.signal });
      return { sent: true };
    },
  },
  { concurrency: 4, signal: controller.signal, drainTimeoutMs: 30_000 },
);
```

Options (all optional):

| Option | Default | Meaning |
| --- | --- | --- |
| `concurrency` | `1` | Handlers running at once. Each lease asks for as many jobs as there are free slots; no lease is issued while every slot is busy. |
| `leaseSecs` | `30` | Lease duration per job. Every in-flight job is heartbeated at half this interval. |
| `waitSecs` | `20` | Long-poll wait when the queue is empty. |
| `signal` | none | Stop signal (see below). |
| `drainTimeoutMs` | `30000` | How long to wait for in-flight handlers after `signal` aborts. |
| `onError` | throttled `console.warn` | Called on transient lease errors (network, 5xx). |

Stop and drain: aborting `signal` cancels the in-flight long poll immediately,
so stopping never waits out `waitSecs`. Handlers already running get up to
`drainTimeoutMs` to finish and report their outcome. Any handler still running
after that has its `ctx.signal` aborted and is left unreported: its lease
expires and the server redelivers the job. `run()` resolves once the drain is
over, with `unfinished` listing those abandoned jobs (empty when everything
finished in time).

The runtime heartbeats each job at half the lease interval, stops reporting
when a heartbeat shows the lease was lost (the job is no longer `running`, or
the server answers 409), and leaves retries, backoff and dead-lettering to the
server. Delivery is at-least-once, so make handlers idempotent. Configure the
server's `--worker-token` and pass it as `workerToken`; `run()` throws on
401/403 rather than spinning. Lower-level calls (`lease`, `heartbeat`,
`complete`, `fail`) are also exposed; `lease()` accepts a `signal` too.

### Cron — `qf.cron`

| Method | Description |
| --- | --- |
| `create({ name, schedule, task, payload?, queue? })` | Register a recurring enqueue (5-field crontab, UTC). |
| `get(id)` · `list(opts?)` · `delete(id)` | Fetch / list / delete. |
| `pause(id)` · `resume(id)` | Stop firings / resume at the next future occurrence. |

### Dead letters — `qf.dlq`

| Method | Description |
| --- | --- |
| `list(opts?)` · `get(id)` | Inspect terminally-failed jobs. `list` also takes `taskName` and `replayed` (`false` = not yet replayed, `true` = already replayed) since 0.5.0. |
| `replay(id)` | Re-run one as a fresh job (at most once; a second replay is a 409). |
| `replayMany(filter?)` | **0.5.0** Replay every unreplayed entry matching `{ queue?, taskName?, createdAfter?, createdBefore?, limit? }`, oldest first. |

`replayMany` returns `{ results, replayed }`: one entry per dead letter considered, each with the
fresh `job_id` or an `error` explaining why it could not be replayed (for example the original job
was purged by retention), and `replayed` counting the successes. Already-replayed entries are
skipped, so repeating the call with the same filter is harmless. `limit` defaults to 100 and is
capped by the server's `--max-batch`.

### Errors

All SDK errors extend `QueueFlowError`:

```ts
import { NotFoundError, ApiError } from "@queueflow/sdk";

try {
  await qf.jobs.get("missing");
} catch (err) {
  if (err instanceof NotFoundError) { /* 404 */ }
  else if (err instanceof ApiError) { console.error(err.status, err.body); }
  else throw err;
}
```

`ApiError` subclasses: `BadRequestError` (400), `UnauthorizedError` (401),
`ForbiddenError` (403), `NotFoundError` (404), `ConflictError` (409). Network/abort
failures throw `ConnectionError`; an exhausted `waitFor` throws `TimeoutError`, and a
caller-aborted one throws `AbortError`.

## Try it against a real server

A complete, runnable Express integration that uses this SDK lives in
[`../queueflow-nodejs-example`](../queueflow-nodejs-example). With
Docker + Rust + Node installed it brings up Postgres, the QueueFlow server, builds
this SDK, and runs an end-to-end smoke test in one command:

```bash
cd ../queueflow-nodejs-example
make demo                 # stack up + SDK build + smoke test
make app                  # run the example API on :3000
make down                 # stop the server + remove the Postgres container
```

Override ports if the defaults are taken, passing the **same** values to each
command (`make demo PG_PORT=5440 API_PORT=8055`, then `make app API_PORT=8055`,
`make down API_PORT=8055 PG_PORT=5440`). See that example's README for endpoint
docs, hitting the engine directly, teardown, and troubleshooting.

## Conformance tests

`test/live.mjs` runs the facade against a real QueueFlow server (started with the default
`queueflow serve`, i.e. `--mode all`, so the built-in `echo` handler is registered). It creates
and waits on an `echo` job, checks idempotent re-creation, runs a two-step workflow, exercises the
cron and dead-letter endpoints, reads stats, runs `qf.worker.run()` against a dedicated queue,
(against server 0.5.0) checks `whoami`, bulk cancel, queue pause/resume, and bulk dead-letter
replay, and (against server 0.6.0) runs a conditional step that is skipped, a fan-out, a
sub-workflow, a failed-then-retried workflow and the `watch()` event stream, each on its own
queue with its own worker. Every bulk operation and every 0.6.0 workflow is scoped to a queue
name unique to the run, so the suite is safe to point at a shared development server. It is skipped unless `QUEUEFLOW_URL` is set, so `npm test`
and CI stay offline.

```bash
QUEUEFLOW_URL=http://localhost:8000 \
QUEUEFLOW_TOKEN=dev \
QUEUEFLOW_WORKER_TOKEN=worker-secret \
npm run test:live
```

`QUEUEFLOW_TOKEN` is the tenant token (default `dev`). `QUEUEFLOW_WORKER_TOKEN` is the server's
`--worker-token`; it defaults to the tenant token, which only works when the server runs without
one.

## Architecture

This package is a thin hand-written **facade** over a **generated core**:

```
queueflow-sdk-nodejs/
├── core/        generated from the OpenAPI spec (models, per-tag API clients, fetch runtime).
│                Never hand-edited; regenerated with `npm run generate-core`.
└── src/         the hand-written facade (this package's public API)
    ├── client.ts    QueueFlow + jobs/workflows/worker/system, retries, SSE watch() streams
    ├── workflow.ts  the wf() builder with local DAG and step-shape validation
    ├── errors.ts    typed error hierarchy, mapped from the core's runtime errors
    └── json.ts      JSON input types
```

The wire types and transport come from `core/`, so they cannot drift from the server; the facade
adds only what codegen cannot express (`waitFor`, `watch`, the worker loop, the builder, typed
errors). Both layers are bundled together into one dual ESM + CJS package, so consumers never import
from `core/` directly.

### Development

```bash
npm run generate-core   # regenerate core/ from the spec (needs Docker)
npm run typecheck       # tsc over the facade + core
npm run build           # bundle to dist/ (ESM + CJS + .d.ts) via tsup
```

Regenerate `core/` whenever the server's OpenAPI spec changes, then run `typecheck` to confirm the
facade still matches.

## Requirements

- Node.js ≥ 18 (for the global `fetch`).
- A running QueueFlow server — see [queueflow-core](https://github.com/elision-labs/queueflow-core).

## License

[MIT](./LICENSE)
