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

### Jobs — `qf.jobs`

| Method | Description |
| --- | --- |
| `create(input)` | Enqueue a job, returns the created `Job`. |
| `enqueue(input)` | Enqueue and return just the new job id (no follow-up fetch). |
| `createBatch(inputs)` | Enqueue up to 1000 jobs at once. |
| `get(id)` | Fetch a job. |
| `list(opts?)` | List jobs (`status`, `queue`, `limit`, `offset`, `orderBy`, `cursor`). |
| `cancel(id)` | Cancel a job. |
| `waitFor(id, opts?)` | Poll until `completed` / `failed` / `cancelled`. |
| `watch(id, opts?)` | Async-iterate the job's status changes (SSE); ends at a terminal state. |

`input` is `{ task, payload?, priority?, maxRetries?, timeout?, queue?, retryBackoff?,
retryDelaySecs?, retryMaxDelaySecs?, jitterFactor?, idempotencyKey?, runAt? }`.

List responses carry `next_cursor` when there are more pages; pass it back as
`cursor` for keyset pagination (cheaper than deep `offset`).

### Workflows — `qf.workflows`

| Method | Description |
| --- | --- |
| `create(builderOrBody)` | Create a workflow from a `wf()` builder or a raw request. |
| `get(id)` · `list(opts?)` · `cancel(id)` | Fetch / list / cancel. |
| `diagram(id)` | Mermaid (`graph TD`) diagram of the DAG. |
| `waitFor(id, opts?)` | Poll until a terminal workflow state. |

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

### System — `qf.system`

```ts
await qf.system.stats();  // engine counters
await qf.system.tasks();  // registered task handler names
```

### Worker — `qf.worker`

Run task handlers in this process against a remote QueueFlow server:

```ts
await qf.worker.run("default", {
  "send-email": async (job, ctx) => {
    // ctx.signal aborts if the job is cancelled mid-run or the lease is lost.
    await sendEmail(job.payload);
    return { sent: true };
  },
});
```

`run()` leases one job at a time, heartbeats at half the lease interval, stops
reporting when the lease is lost, and applies the server's retry policy on
errors. Delivery is at-least-once — make handlers idempotent. Configure the
server's `--worker-token` and pass it as `workerToken`; `run()` throws on
401/403 rather than spinning. Lower-level calls (`lease`, `heartbeat`,
`complete`, `fail`) are also exposed.

### Cron — `qf.cron`

| Method | Description |
| --- | --- |
| `create({ name, schedule, task, payload?, queue? })` | Register a recurring enqueue (5-field crontab, UTC). |
| `get(id)` · `list(opts?)` · `delete(id)` | Fetch / list / delete. |
| `pause(id)` · `resume(id)` | Stop firings / resume at the next future occurrence. |

### Dead letters — `qf.dlq`

| Method | Description |
| --- | --- |
| `list(opts?)` · `get(id)` | Inspect terminally-failed jobs. |
| `replay(id)` | Re-run one as a fresh job (at most once; a second replay is a 409). |

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

## Architecture

This package is a thin hand-written **facade** over a **generated core**:

```
queueflow-sdk-nodejs/
├── core/        generated from the OpenAPI spec (models, per-tag API clients, fetch runtime).
│                Never hand-edited; regenerated with `npm run generate-core`.
└── src/         the hand-written facade (this package's public API)
    ├── client.ts    QueueFlow + jobs/workflows/worker/system, retries, SSE watch()
    ├── workflow.ts  the wf() builder with local DAG validation
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
