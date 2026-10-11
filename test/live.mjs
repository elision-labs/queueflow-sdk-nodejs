/**
 * Live conformance test: runs the facade against a REAL QueueFlow server.
 *
 * Skipped unless QUEUEFLOW_URL is set. The server must run in `--mode all`
 * with the built-in `echo` handler registered (the default `queueflow serve`).
 *
 *   QUEUEFLOW_URL           base URL, e.g. http://localhost:8000
 *   QUEUEFLOW_TOKEN         tenant bearer token (default "dev")
 *   QUEUEFLOW_WORKER_TOKEN  worker-protocol token (defaults to QUEUEFLOW_TOKEN,
 *                           which only works when the server has no --worker-token)
 *
 * Run with `npm run test:live`. This file is deliberately not matched by the
 * `npm test` glob so CI stays offline.
 */

import test from "node:test";
import assert from "node:assert/strict";

import { QueueFlow, wf, NotFoundError } from "../dist/index.js";

const URL_ = process.env.QUEUEFLOW_URL;
const TOKEN = process.env.QUEUEFLOW_TOKEN ?? "dev";
const WORKER_TOKEN = process.env.QUEUEFLOW_WORKER_TOKEN ?? TOKEN;
const skip = URL_ ? false : "QUEUEFLOW_URL is not set";

const suffix = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const WAIT = { timeoutMs: 60_000, intervalMs: 250 };

const qf = URL_
  ? new QueueFlow({ baseUrl: URL_, token: TOKEN, workerToken: WORKER_TOKEN })
  : null;

test("live: server is reachable and lists the echo task", { skip }, async () => {
  const health = await qf.health();
  assert.ok(health, "health() returned nothing");
  const tasks = await qf.system.tasks();
  assert.ok(tasks.includes("echo"), `echo handler not registered: ${tasks.join(", ")}`);
});

test("live: echo job completes and echoes its payload", { skip }, async () => {
  const payload = { hello: "world", n: 42, nested: { ok: true } };
  const idempotencyKey = `sdk-live-${suffix}`;

  const created = await qf.jobs.create({ task: "echo", payload, idempotencyKey });
  assert.equal(created.task_name, "echo");
  assert.equal(created.idempotency_key, idempotencyKey);

  const fetched = await qf.jobs.get(created.id);
  assert.equal(fetched.id, created.id);

  const done = await qf.jobs.waitFor(created.id, WAIT);
  assert.equal(done.status, "completed", `error: ${done.error_message}`);
  // The built-in echo handler returns the payload plus `echoed: true`.
  assert.deepEqual(done.result, { ...payload, echoed: true });

  // Re-submitting the same idempotency key returns the original job.
  const again = await qf.jobs.create({ task: "echo", payload, idempotencyKey });
  assert.equal(again.id, created.id);
});

test("live: two-step echo workflow completes", { skip }, async () => {
  const workflow = await qf.workflows.create(
    wf(`sdk-live-wf-${suffix}`)
      .step("first", "echo", { payload: { step: 1 } })
      .step("second", "echo", { after: ["first"], payload: { step: 2 } }),
  );
  assert.equal(workflow.steps.length, 2);

  const finished = await qf.workflows.waitFor(workflow.id, WAIT);
  assert.equal(finished.status, "completed");

  const steps = await qf.workflows.steps(workflow.id);
  assert.equal(steps.length, 2);
  for (const step of steps) assert.equal(step.status, "completed", step.name);
});

test("live: cron create, list, pause, resume, delete", { skip }, async () => {
  const name = `sdk-live-cron-${suffix}`;
  // Fires once a year; the schedule never triggers during the test.
  const cron = await qf.cron.create({ name, schedule: "0 0 1 1 *", task: "echo", payload: { from: "cron" } });
  assert.equal(cron.name, name);
  assert.equal(cron.enabled, true);

  const listed = await qf.cron.list({ limit: 100 });
  assert.ok(listed.crons.some((c) => c.id === cron.id), "new cron missing from list");

  await qf.cron.pause(cron.id);
  assert.equal((await qf.cron.get(cron.id)).enabled, false);

  await qf.cron.resume(cron.id);
  assert.equal((await qf.cron.get(cron.id)).enabled, true);

  await qf.cron.delete(cron.id);
  await assert.rejects(() => qf.cron.get(cron.id), NotFoundError);
});

test("live: dead-letter list and stats", { skip }, async () => {
  const dlq = await qf.dlq.list({ limit: 10 });
  assert.ok(Array.isArray(dlq.dead_letters));
  assert.equal(typeof dlq.has_more, "boolean");

  const stats = await qf.system.stats();
  assert.equal(typeof stats.jobs_created, "number");
  assert.ok(stats.jobs_created >= 1, "this test created at least one job");
});

test("live: worker runtime leases and completes a job on a dedicated queue", { skip }, async () => {
  // A queue the server's in-process workers never poll, and a task name only
  // this test's worker knows, so the job can only be completed by qf.worker.run.
  const queue = `sdk-live-q-${suffix}`;
  const task = `sdk-live-task-${suffix}`;
  const payload = { order: 7 };

  const job = await qf.jobs.create({ task, payload, queue });
  assert.equal(job.queue_name, queue);
  assert.equal(job.status, "pending");

  const controller = new AbortController();
  const handled = [];
  const worker = qf.worker.run(
    queue,
    {
      [task]: async (leased) => {
        handled.push(leased.id);
        return { ...leased.payload, handled: true };
      },
    },
    { leaseSecs: 10, waitSecs: 2, signal: controller.signal },
  );

  let done;
  let result;
  try {
    done = await qf.jobs.waitFor(job.id, WAIT);
  } finally {
    controller.abort();
    result = await worker;
  }

  assert.deepEqual(handled, [job.id]);
  assert.deepEqual(result, { unfinished: [] });
  assert.equal(done.status, "completed", `error: ${done.error_message}`);
  assert.deepEqual(done.result, { ...payload, handled: true });
});

test("live: worker runtime runs jobs concurrently up to the configured cap", { skip }, async () => {
  const queue = `sdk-live-cq-${suffix}`;
  const task = `sdk-live-ctask-${suffix}`;
  const count = 4;

  const jobs = await Promise.all(
    Array.from({ length: count }, (_, i) => qf.jobs.create({ task, payload: { i }, queue })),
  );

  const controller = new AbortController();
  let active = 0;
  let maxActive = 0;
  const started = new Set();
  const worker = qf.worker.run(
    queue,
    {
      [task]: async (leased) => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        started.add(leased.id);
        // Long enough that the four leases overlap in time.
        await new Promise((r) => setTimeout(r, 750));
        active -= 1;
        return { ...leased.payload, handled: true };
      },
    },
    { concurrency: count, leaseSecs: 10, waitSecs: 2, signal: controller.signal },
  );

  let finished;
  let result;
  try {
    finished = await Promise.all(jobs.map((job) => qf.jobs.waitFor(job.id, WAIT)));
  } finally {
    const t0 = Date.now();
    controller.abort();
    result = await worker;
    // Stop must interrupt the long poll rather than wait out waitSecs.
    assert.ok(Date.now() - t0 < 1_500, "stop waited for the long poll");
  }

  assert.deepEqual(result, { unfinished: [] });
  assert.equal(started.size, count);
  assert.ok(maxActive > 1, `handlers never overlapped (max active ${maxActive})`);
  assert.ok(maxActive <= count, `concurrency cap exceeded (${maxActive} > ${count})`);
  for (const job of finished) {
    assert.equal(job.status, "completed", `job ${job.id}: ${job.error_message}`);
    assert.equal(job.result.handled, true);
  }

  const queues = await qf.system.queues();
  assert.ok(Array.isArray(queues));
  assert.ok(!queues.some((q) => q.queue === queue), "a drained queue must not be listed");
});
