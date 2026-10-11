/**
 * Tests for the hand-written facade (`src/`), not the generated core.
 *
 * They run against the built `dist/`, because Node's built-in TypeScript
 * support is strip-only and the facade uses parameter properties, which need a
 * real transform. `npm test` builds first for that reason.
 *
 * Every HTTP interaction goes through an injected `fetch`, so the suite is
 * offline and deterministic: no live QueueFlow, no timers longer than a few ms.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  QueueFlow,
  wf,
  WorkflowBuilder,
  WorkflowValidationError,
  ApiError,
  NotFoundError,
  TimeoutError,
  AbortError,
} from "../dist/index.js";

// --- helpers ---------------------------------------------------------------

const JOB_FIXTURE = {
  id: "job-1",
  task_name: "echo",
  queue_name: "default",
  retry_count: 0,
  created_at: "2026-01-01T00:00:00Z",
  scheduled_at: "2026-01-01T00:00:00Z",
  config: {
    max_retries: 3,
    priority: 0,
    retry_delay_secs: 1,
    retry_max_delay_secs: 60,
    timeout_secs: 300,
  },
};

const jsonResponse = (body, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });

/**
 * A fetch stub that replays `responses` in order (repeating the last one) and
 * records every call. Responses are cloned per call because a `Response` body
 * can only be consumed once, and a polling test reads the same entry
 * repeatedly.
 */
function stubFetch(responses) {
  const calls = [];
  const fetchImpl = async (input, init) => {
    const url = typeof input === "string" ? input : input.url;
    calls.push({ url, method: init?.method ?? "GET", headers: init?.headers, body: init?.body });
    const next = responses[Math.min(calls.length - 1, responses.length - 1)];
    return typeof next === "function" ? next() : next.clone();
  };
  return { fetchImpl, calls };
}

const clientWith = (responses, opts = {}) => {
  const { fetchImpl, calls } = stubFetch(responses);
  const qf = new QueueFlow({
    baseUrl: "http://qf.test",
    token: "test-token",
    fetch: fetchImpl,
    ...opts,
  });
  return { qf, calls };
};

// --- constructor -----------------------------------------------------------

test("constructor requires baseUrl and token", () => {
  assert.throws(() => new QueueFlow({ token: "t", fetch: async () => {} }), /baseUrl/);
  assert.throws(
    () => new QueueFlow({ baseUrl: "http://qf.test", fetch: async () => {} }),
    /token/,
  );
});

test("constructor strips trailing slashes from baseUrl", async () => {
  const { fetchImpl, calls } = stubFetch([jsonResponse(JOB_FIXTURE)]);
  const qf = new QueueFlow({
    baseUrl: "http://qf.test///",
    token: "t",
    fetch: fetchImpl,
  });
  await qf.jobs.get("job-1");
  // A doubled slash would produce a 404 against a strict router.
  assert.ok(!calls[0].url.includes("//jobs"), `url has a doubled slash: ${calls[0].url}`);
  assert.ok(calls[0].url.startsWith("http://qf.test/"), calls[0].url);
});

// --- WorkflowBuilder validation --------------------------------------------

test("WorkflowBuilder rejects an empty name at construction", () => {
  assert.throws(() => wf(""), WorkflowValidationError);
});

test("WorkflowBuilder rejects structurally invalid DAGs", () => {
  const cases = [
    ["no steps", () => wf("etl").build(), /has no steps/],
    [
      "duplicate step name",
      () => wf("etl").step("a", "task").step("a", "other").build(),
      /duplicate step name "a"/,
    ],
    [
      "dangling dependency",
      () => wf("etl").step("a", "task", { after: ["ghost"] }).build(),
      /depends on unknown step "ghost"/,
    ],
    [
      "direct cycle",
      () =>
        wf("etl")
          .step("a", "task", { after: ["b"] })
          .step("b", "task", { after: ["a"] })
          .build(),
      /dependency cycle/,
    ],
    [
      "self cycle",
      () => wf("etl").step("a", "task", { after: ["a"] }).build(),
      /dependency cycle/,
    ],
    [
      "long cycle",
      () =>
        wf("etl")
          .step("a", "task", { after: ["c"] })
          .step("b", "task", { after: ["a"] })
          .step("c", "task", { after: ["b"] })
          .build(),
      /dependency cycle/,
    ],
  ];

  for (const [name, build, pattern] of cases) {
    assert.throws(build, pattern, `case: ${name}`);
    assert.throws(build, WorkflowValidationError, `case: ${name} (wrong error type)`);
  }
});

test("WorkflowBuilder accepts a diamond DAG", () => {
  const req = wf("etl")
    .step("extract", "fetch")
    .step("left", "normalize", { after: ["extract"] })
    .step("right", "enrich", { after: ["extract"] })
    .step("load", "upsert", { after: ["left", "right"] })
    .build();

  assert.equal(req.name, "etl");
  assert.equal(req.steps.length, 4);
  assert.deepEqual(req.steps[3].depends_on, ["left", "right"]);
  // An unconstrained step must not carry an empty depends_on array.
  assert.equal(req.steps[0].depends_on, undefined);
});

test("WorkflowBuilder merges context and metadata across calls", () => {
  const req = wf("etl")
    .step("a", "task")
    .context({ run: "2026-06-07" })
    .context({ tenant: "acme" })
    .metadata({ owner: "data" })
    .build();

  assert.deepEqual(req.context, { run: "2026-06-07", tenant: "acme" });
  assert.deepEqual(req.metadata, { owner: "data" });
});

test("wf() is equivalent to new WorkflowBuilder()", () => {
  assert.ok(wf("etl") instanceof WorkflowBuilder);
});

// --- jobs ------------------------------------------------------------------

test("jobs.create enqueues then fetches the full record", async () => {
  const { qf, calls } = clientWith([
    jsonResponse({ job_id: "job-1" }),
    jsonResponse({ ...JOB_FIXTURE, status: "pending" }),
  ]);

  const job = await qf.jobs.create({ taskName: "echo", payload: { hi: 1 } });

  assert.equal(job.id, "job-1");
  assert.equal(calls.length, 2);
  assert.equal(calls[0].method, "POST");
  assert.equal(calls[1].method, "GET");
  assert.deepEqual(JSON.parse(calls[0].body).payload, { hi: 1 });
});

test("requests carry the bearer token", async () => {
  const { qf, calls } = clientWith([jsonResponse({ ...JOB_FIXTURE, status: "pending" })]);
  await qf.jobs.get("job-1");
  const headers = new Headers(calls[0].headers);
  assert.equal(headers.get("Authorization"), "Bearer test-token");
});

test("jobs.waitFor polls until a terminal status", async () => {
  const { qf, calls } = clientWith([
    jsonResponse({ ...JOB_FIXTURE, status: "pending" }),
    jsonResponse({ ...JOB_FIXTURE, status: "running" }),
    jsonResponse({ ...JOB_FIXTURE, status: "completed" }),
  ]);

  const job = await qf.jobs.waitFor("job-1", { intervalMs: 1, timeoutMs: 5_000 });

  assert.equal(job.status, "completed");
  assert.equal(calls.length, 3, "must stop at the first terminal status");
});

test("jobs.waitFor returns failed and cancelled rather than waiting them out", async () => {
  for (const status of ["failed", "cancelled"]) {
    const { qf } = clientWith([jsonResponse({ ...JOB_FIXTURE, status })]);
    const job = await qf.jobs.waitFor("job-1", { intervalMs: 1, timeoutMs: 1_000 });
    assert.equal(job.status, status);
  }
});

test("jobs.waitFor times out while a job is still retrying", async () => {
  // `retrying` is deliberately not terminal.
  const { qf } = clientWith([jsonResponse({ ...JOB_FIXTURE, status: "retrying" })]);

  await assert.rejects(
    () => qf.jobs.waitFor("job-1", { intervalMs: 5, timeoutMs: 30 }),
    (err) => {
      assert.ok(err instanceof TimeoutError, `expected TimeoutError, got ${err?.name}`);
      assert.match(err.message, /did not reach a terminal state/);
      return true;
    },
  );
});

test("jobs.waitFor honours an already-aborted signal without fetching", async () => {
  const { qf, calls } = clientWith([jsonResponse({ ...JOB_FIXTURE, status: "pending" })]);

  await assert.rejects(
    () => qf.jobs.waitFor("job-1", { signal: AbortSignal.abort(), intervalMs: 1 }),
    AbortError,
  );
  assert.equal(calls.length, 0, "an aborted wait must not issue a request");
});

// --- error mapping ---------------------------------------------------------

test("HTTP status codes map to typed errors", async () => {
  const { qf } = clientWith([jsonResponse({ error: "no such job" }, 404)]);

  await assert.rejects(
    () => qf.jobs.get("missing"),
    (err) => {
      assert.ok(err instanceof NotFoundError, `expected NotFoundError, got ${err?.name}`);
      assert.ok(err instanceof ApiError);
      assert.equal(err.status, 404);
      return true;
    },
  );
});

// --- transport retry -------------------------------------------------------

test("idempotent reads retry on a retryable 5xx and then succeed", async () => {
  let n = 0;
  const { fetchImpl, calls } = stubFetch([
    () => {
      n += 1;
      return n === 1
        ? jsonResponse({ error: "bad gateway" }, 502)
        : jsonResponse({ ...JOB_FIXTURE, status: "completed" });
    },
  ]);
  const qf = new QueueFlow({
    baseUrl: "http://qf.test",
    token: "t",
    fetch: fetchImpl,
    maxRetries: 2,
  });

  const job = await qf.jobs.get("job-1");

  assert.equal(job.status, "completed");
  assert.equal(calls.length, 2, "should have retried exactly once");
});

test("a 400 is not retried", async () => {
  const { qf, calls } = clientWith([jsonResponse({ error: "bad request" }, 400)], {
    maxRetries: 3,
  });

  await assert.rejects(() => qf.jobs.get("job-1"), ApiError);
  assert.equal(calls.length, 1, "client errors must not be retried");
});

test("retries are capped by maxRetries", async () => {
  const { qf, calls } = clientWith([jsonResponse({ error: "unavailable" }, 503)], {
    maxRetries: 2,
  });

  await assert.rejects(() => qf.jobs.get("job-1"), ApiError);
  assert.equal(calls.length, 3, "1 initial attempt + 2 retries");
});

test("non-idempotent writes are not retried", async () => {
  const { qf, calls } = clientWith([jsonResponse({ error: "unavailable" }, 503)], {
    maxRetries: 3,
  });

  // No idempotencyKey => the enqueue is not safe to replay.
  await assert.rejects(() => qf.jobs.enqueue({ taskName: "echo" }), ApiError);
  assert.equal(calls.length, 1, "a write without an idempotency key must be sent once");
});

// --- list filters ----------------------------------------------------------

const CRON_LIST_FIXTURE = { crons: [], has_more: false, limit: 50, offset: 0 };
const DLQ_LIST_FIXTURE = { dead_letters: [], has_more: false, limit: 50, offset: 0 };

test("cron.list forwards the status and queue filters", async () => {
  const { qf, calls } = clientWith([jsonResponse(CRON_LIST_FIXTURE)]);

  await qf.cron.list({ status: "enabled", queue: "billing", limit: 10 });

  const url = new URL(calls[0].url);
  assert.equal(url.pathname, "/api/v1/cron");
  assert.equal(url.searchParams.get("status"), "enabled");
  assert.equal(url.searchParams.get("queue"), "billing");
  assert.equal(url.searchParams.get("limit"), "10");
});

test("dlq.list forwards the status filter", async () => {
  const { qf, calls } = clientWith([jsonResponse(DLQ_LIST_FIXTURE)]);

  await qf.dlq.list({ status: "failed", queue: "billing" });

  const url = new URL(calls[0].url);
  assert.equal(url.pathname, "/api/v1/dlq");
  assert.equal(url.searchParams.get("status"), "failed");
  assert.equal(url.searchParams.get("queue"), "billing");
});

test("list filters left unset are not sent as query parameters", async () => {
  const { qf, calls } = clientWith([jsonResponse(CRON_LIST_FIXTURE)]);

  await qf.cron.list();

  const url = new URL(calls[0].url);
  assert.equal([...url.searchParams.keys()].length, 0, url.search);
});

// --- worker runtime --------------------------------------------------------

const leasedJob = (id, task = "work") => ({
  ...JOB_FIXTURE,
  id,
  task_name: task,
  status: "running",
  payload: { id },
});

/**
 * A fetch stub that speaks the worker protocol: leases hand out `jobs` in
 * order (at most `max_jobs` per call), an empty queue long-polls until the
 * request's abort signal fires (or `wait_secs` elapses), heartbeats answer
 * with `heartbeat` (a status string, or an HTTP status number for an error),
 * and complete/fail are recorded.
 */
function workerServer(jobs, { heartbeat = "running" } = {}) {
  const pending = [...jobs];
  const seen = { leases: [], heartbeats: 0, completes: [], fails: [] };
  const fetchImpl = async (input, init) => {
    const url = new URL(typeof input === "string" ? input : input.url);
    const body = init?.body ? JSON.parse(init.body) : {};
    if (url.pathname.endsWith("/lease")) {
      seen.leases.push(body);
      const take = Math.min(body.max_jobs ?? 1, pending.length);
      const leased = pending.splice(0, take).map((job) => ({ job, lease_token: `tok-${job.id}` }));
      if (leased.length === 0) {
        await new Promise((resolve, reject) => {
          const timer = setTimeout(resolve, (body.wait_secs ?? 0) * 1_000);
          init?.signal?.addEventListener(
            "abort",
            () => {
              clearTimeout(timer);
              reject(new DOMException("The operation was aborted", "AbortError"));
            },
            { once: true },
          );
        });
      }
      return jsonResponse({ jobs: leased });
    }
    if (url.pathname.endsWith("/heartbeat")) {
      seen.heartbeats += 1;
      return typeof heartbeat === "number"
        ? jsonResponse({ error: "lease lost" }, heartbeat)
        : jsonResponse({ status: heartbeat });
    }
    if (url.pathname.endsWith("/complete")) {
      seen.completes.push({ id: url.pathname.split("/").at(-2), ...body });
      return new Response(null, { status: 204 });
    }
    if (url.pathname.endsWith("/fail")) {
      seen.fails.push({ id: url.pathname.split("/").at(-2), ...body });
      return new Response(null, { status: 204 });
    }
    if (url.pathname === "/api/v1/queues") {
      return jsonResponse({
        queues: [{ queue: "default", pending: 2, scheduled: 1, running: 1, oldest_pending_age_secs: 7 }],
      });
    }
    throw new Error(`unexpected request ${init?.method ?? "GET"} ${url.pathname}`);
  };
  const qf = new QueueFlow({
    baseUrl: "http://qf.test",
    token: "tenant",
    workerToken: "worker",
    fetch: fetchImpl,
    maxRetries: 0,
  });
  return { qf, seen };
}

/** Resolve once `predicate()` holds (polled every ms), or fail after 5s. */
async function until(predicate, what) {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 1));
  }
}

test("worker.run with concurrency runs handlers in parallel and never exceeds the cap", async () => {
  const { qf, seen } = workerServer(["a", "b", "c", "d", "e"].map((id) => leasedJob(id)));
  const controller = new AbortController();
  let active = 0;
  let maxActive = 0;
  const started = [];
  const release = new Map();
  const handled = [];

  const worker = qf.worker.run(
    "q",
    {
      work: async (job) => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        started.push(job.id);
        await new Promise((resolve) => release.set(job.id, resolve));
        active -= 1;
        handled.push(job.id);
        return { done: job.id };
      },
    },
    { concurrency: 2, waitSecs: 20, leaseSecs: 30, signal: controller.signal },
  );

  // Two slots fill from a single lease asking for exactly two jobs...
  await until(() => started.length === 2, "two handlers to start");
  assert.deepEqual(seen.leases, [{ max_jobs: 2, lease_secs: 30, wait_secs: 20 }]);
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(started.length, 2, "a third handler must not start while both slots are busy");
  assert.equal(seen.leases.length, 1, "no lease is issued while every slot is busy");

  // ...and finishing one frees exactly one slot, leased with max_jobs: 1.
  release.get("a")();
  await until(() => started.length === 3, "a third handler to start");
  assert.equal(seen.leases.at(-1).max_jobs, 1);
  assert.equal(active, 2);

  for (const id of ["b", "c", "d", "e"]) {
    await until(() => release.has(id), `job ${id} to start`);
    release.get(id)();
  }
  await until(() => seen.completes.length === 5, "all completes");
  controller.abort();
  const result = await worker;

  assert.equal(maxActive, 2, "concurrency cap exceeded");
  assert.deepEqual(result, { unfinished: [] });
  assert.deepEqual(handled.sort(), ["a", "b", "c", "d", "e"]);
  assert.deepEqual(
    seen.completes.map((c) => c.result).sort((x, y) => x.done.localeCompare(y.done)),
    ["a", "b", "c", "d", "e"].map((id) => ({ done: id })),
  );
  assert.equal(seen.fails.length, 0);
});

test("worker.run defaults to concurrency 1", async () => {
  const { qf, seen } = workerServer([leasedJob("a")]);
  const controller = new AbortController();
  const worker = qf.worker.run("q", { work: async () => ({}) }, {
    waitSecs: 20,
    signal: controller.signal,
  });
  await until(() => seen.completes.length === 1, "the job to complete");
  controller.abort();
  await worker;
  assert.equal(seen.leases[0].max_jobs, 1);
});

test("worker.run stops immediately during a long poll instead of waiting out waitSecs", async () => {
  const { qf, seen } = workerServer([]);
  const controller = new AbortController();
  const worker = qf.worker.run("q", {}, { waitSecs: 20, signal: controller.signal });

  await until(() => seen.leases.length === 1, "the long poll to start");
  const t0 = Date.now();
  controller.abort();
  const result = await worker;

  assert.ok(Date.now() - t0 < 1_000, "stop must not wait for the 20s long poll");
  assert.deepEqual(result, { unfinished: [] });
  assert.equal(seen.leases.length, 1, "no further lease after stop");
});

test("worker.run resolves at once when the signal is already aborted", async () => {
  const { qf, seen } = workerServer([leasedJob("a")]);
  const result = await qf.worker.run("q", { work: async () => ({}) }, {
    signal: AbortSignal.abort(),
  });
  assert.deepEqual(result, { unfinished: [] });
  assert.equal(seen.leases.length, 0);
});

test("worker.run drains in-flight handlers after stop and reports their outcomes", async () => {
  const { qf, seen } = workerServer([leasedJob("a")]);
  const controller = new AbortController();
  let finish;
  const worker = qf.worker.run(
    "q",
    {
      work: async () => {
        await new Promise((resolve) => (finish = resolve));
        return { ok: true };
      },
    },
    { waitSecs: 20, signal: controller.signal, drainTimeoutMs: 5_000 },
  );

  await until(() => finish !== undefined, "the handler to start");
  controller.abort();
  // The worker is draining; the handler finishes well inside the window.
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(seen.completes.length, 0, "the handler has not finished yet");
  finish();
  const result = await worker;

  assert.deepEqual(result.unfinished, []);
  assert.equal(seen.completes.length, 1);
  assert.deepEqual(seen.completes[0].result, { ok: true });
});

test("worker.run gives up on handlers that outlive drainTimeoutMs and reports them", async () => {
  const { qf, seen } = workerServer([leasedJob("slow"), leasedJob("quick")]);
  const controller = new AbortController();
  let slowSignal;
  const worker = qf.worker.run(
    "q",
    {
      work: async (job, ctx) => {
        if (job.id === "quick") return { quick: true };
        slowSignal = ctx.signal;
        // Never finishes on its own; only stops when told to.
        await new Promise((resolve) => ctx.signal.addEventListener("abort", resolve, { once: true }));
        return { should: "not be reported" };
      },
    },
    { concurrency: 2, waitSecs: 20, signal: controller.signal, drainTimeoutMs: 50 },
  );

  await until(() => seen.completes.length === 1 && slowSignal !== undefined, "quick to finish");
  const t0 = Date.now();
  controller.abort();
  const result = await worker;

  assert.ok(Date.now() - t0 < 2_000, "drain must give up after drainTimeoutMs");
  assert.equal(result.unfinished.length, 1);
  assert.equal(result.unfinished[0].id, "slow");
  assert.ok(slowSignal.aborted, "an abandoned handler's ctx.signal must abort");
  // Give the abandoned handler a tick to return: its outcome must not be reported.
  await new Promise((r) => setTimeout(r, 10));
  assert.equal(seen.completes.length, 1);
  assert.equal(seen.fails.length, 0);
});

test("worker.run aborts the handler and reports nothing when a heartbeat shows the lease was lost", async () => {
  for (const heartbeat of ["cancelled", 409]) {
    const { qf, seen } = workerServer([leasedJob("a")], { heartbeat });
    const controller = new AbortController();
    let aborted = false;
    const worker = qf.worker.run(
      "q",
      {
        work: async (job, ctx) => {
          // leaseSecs 2 => a heartbeat after 1s, which reports the lease lost.
          await new Promise((resolve) => ctx.signal.addEventListener("abort", resolve, { once: true }));
          aborted = true;
          return { late: true };
        },
      },
      { leaseSecs: 2, waitSecs: 20, signal: controller.signal },
    );

    await until(() => aborted, `lost lease (${heartbeat}) to abort the handler`);
    controller.abort();
    const result = await worker;

    assert.deepEqual(result.unfinished, [], `heartbeat=${heartbeat}`);
    assert.ok(seen.heartbeats >= 1, `heartbeat=${heartbeat}: expected a heartbeat`);
    assert.equal(seen.completes.length, 0, `heartbeat=${heartbeat}: the server owns the outcome`);
    assert.equal(seen.fails.length, 0, `heartbeat=${heartbeat}`);
  }
});

test("worker.run keeps a heartbeat per in-flight job", async () => {
  const { qf, seen } = workerServer([leasedJob("a"), leasedJob("b")]);
  const controller = new AbortController();
  const release = [];
  const worker = qf.worker.run(
    "q",
    { work: () => new Promise((resolve) => release.push(() => resolve({}))) },
    { concurrency: 2, leaseSecs: 2, waitSecs: 20, signal: controller.signal },
  );
  await until(() => release.length === 2, "both handlers to start");
  // One tick (1s) with two jobs in flight => two heartbeats.
  await until(() => seen.heartbeats >= 2, "two heartbeats");
  for (const r of release) r();
  controller.abort();
  await worker;
  assert.equal(seen.completes.length, 2);
});

test("system.queues returns the per-queue backlog", async () => {
  const { qf } = workerServer([]);
  const queues = await qf.system.queues();
  assert.deepEqual(queues, [
    { queue: "default", pending: 2, scheduled: 1, running: 1, oldest_pending_age_secs: 7 },
  ]);
});
