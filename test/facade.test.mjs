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
