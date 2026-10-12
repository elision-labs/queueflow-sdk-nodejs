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

import { QueueFlow, wf, NotFoundError, BadRequestError, ConflictError, NonRetryableError } from "../dist/index.js";

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

  // Since server 0.5.0 every queue the tenant has enqueued to is listed, so a
  // drained queue shows up with an empty backlog rather than disappearing.
  const queues = await qf.system.queues();
  assert.ok(Array.isArray(queues));
  const drained = queues.find((q) => q.queue === queue);
  assert.ok(drained, "a queue this tenant enqueued to must be listed");
  assert.equal(drained.pending, 0);
  assert.equal(drained.scheduled, 0);
  assert.equal(drained.running, 0);
  assert.equal(drained.paused, false);
});

// --- server 0.5.0: identity, bulk cancel, queue control, bulk replay --------
//
// The server and its database may be shared with other test runs, so every
// bulk operation below is scoped to a queue only this run uses.

test("live: whoami reports the token's tenant and access level", { skip }, async () => {
  const me = await qf.system.whoami();
  assert.equal(typeof me.tenant, "string");
  assert.ok(me.tenant.length > 0, "tenant must not be empty");
  // The live suite creates jobs, so its token must be read-write.
  assert.equal(me.access, "read_write");
});

test("live: bulk cancel is scoped by filter, honours limit, and rejects terminal statuses", { skip }, async () => {
  // A queue no worker polls, so the jobs stay pending until cancelled.
  const queue = `node-live-cancel-${suffix}`;
  const taskA = `sdk-live-cancel-a-${suffix}`;
  const taskB = `sdk-live-cancel-b-${suffix}`;

  const a1 = await qf.jobs.create({ task: taskA, queue, payload: { n: 1 } });
  const a2 = await qf.jobs.create({ task: taskA, queue, payload: { n: 2 } });
  const b1 = await qf.jobs.create({ task: taskB, queue, payload: { n: 3 } });

  // The new task_name filter on list.
  const onlyA = await qf.jobs.list({ queue, taskName: taskA, limit: 10 });
  assert.deepEqual(onlyA.jobs.map((j) => j.id).sort(), [a1.id, a2.id].sort());
  const onlyB = await qf.jobs.list({ queue, taskName: taskB, limit: 10 });
  assert.deepEqual(onlyB.jobs.map((j) => j.id), [b1.id]);

  // Terminal statuses are refused: history is never rewritten.
  await assert.rejects(() => qf.jobs.cancelMany({ queue, status: "completed" }), BadRequestError);

  // A filter that matches nothing cancels nothing.
  assert.equal(await qf.jobs.cancelMany({ queue, taskName: `nope-${suffix}` }), 0);

  // Capped by limit and scoped to the task: exactly one taskA job goes. (The
  // two were created within the same instant, so "oldest first" may pick
  // either; the test only relies on the count and the task scope.)
  assert.equal(await qf.jobs.cancelMany({ queue, taskName: taskA, limit: 1 }), 1);
  const statuses = await Promise.all([a1, a2].map(async (j) => (await qf.jobs.get(j.id)).status));
  assert.deepEqual(statuses.slice().sort(), ["cancelled", "pending"], `taskA statuses: ${statuses}`);
  assert.equal((await qf.jobs.get(b1.id)).status, "pending");

  // The rest of the queue, then nothing is left to cancel.
  assert.equal(await qf.jobs.cancelMany({ queue, status: "pending" }), 2);
  for (const j of [a1, a2, b1]) assert.equal((await qf.jobs.get(j.id)).status, "cancelled", j.id);
  assert.equal(await qf.jobs.cancelMany({ queue }), 0);
});

test("live: pausing a queue parks its jobs until it is resumed", { skip }, async () => {
  const queue = `node-live-pause-${suffix}`;
  const task = `sdk-live-pause-task-${suffix}`;
  const lease = { maxJobs: 5, leaseSecs: 10, waitSecs: 1 };

  await assert.rejects(() => qf.system.pauseQueue("bad name"), BadRequestError);

  const job = await qf.jobs.create({ task, queue, payload: { parked: true } });
  try {
    const paused = await qf.system.pauseQueue(queue);
    assert.equal(paused.queue, queue);
    assert.equal(paused.paused, true);
    assert.equal(paused.pending, 1);
    assert.equal(paused.running, 0);

    // Idempotent: pausing again reports the same state.
    assert.equal((await qf.system.pauseQueue(queue)).paused, true);

    // The backlog view flags the queue as paused.
    const listed = (await qf.system.queues()).find((q) => q.queue === queue);
    assert.ok(listed, "paused queue with a pending job must be listed");
    assert.equal(listed.paused, true);
    assert.equal(listed.pending, 1);

    // No claims while paused: a lease comes back empty once its wait elapses.
    assert.deepEqual(await qf.worker.lease(queue, lease), []);
    assert.equal((await qf.jobs.get(job.id)).status, "pending");

    const resumed = await qf.system.resumeQueue(queue);
    assert.equal(resumed.paused, false);
    assert.equal(resumed.pending, 1);
    assert.equal((await qf.system.resumeQueue(queue)).paused, false);

    // Claims flow again.
    const leased = await qf.worker.lease(queue, lease);
    assert.equal(leased.length, 1);
    assert.equal(leased[0].job.id, job.id);
    await qf.worker.complete(leased[0], { parked: false });
    assert.equal((await qf.jobs.waitFor(job.id, WAIT)).status, "completed");
  } finally {
    // Never leave this run's queue paused or its job pending on a shared server.
    await qf.system.resumeQueue(queue).catch(() => {});
    await qf.jobs.cancelMany({ queue }).catch(() => {});
  }
});

test("live: dead-letter filters and bulk replay on a dedicated queue", { skip }, async () => {
  const queue = `node-live-dlq-${suffix}`;
  const task = `sdk-live-dlq-task-${suffix}`;

  // No retries and a non-retryable failure: the job dead-letters at once.
  const job = await qf.jobs.create({ task, queue, maxRetries: 0, payload: { attempt: 1 } });
  const [lease] = await qf.worker.lease(queue, { maxJobs: 1, leaseSecs: 10, waitSecs: 2 });
  assert.ok(lease, "the job must be leasable");
  assert.equal(lease.job.id, job.id);
  await qf.worker.fail(lease, "live test: dead-letter me", { retryable: false });
  assert.equal((await qf.jobs.waitFor(job.id, WAIT)).status, "failed");

  try {
    // The new task_name and replayed filters on list.
    const unreplayed = await qf.dlq.list({ queue, replayed: false, limit: 10 });
    const entry = unreplayed.dead_letters.find((d) => d.job_id === job.id);
    assert.ok(entry, "dead letter for the failed job must be listed as unreplayed");
    assert.equal(entry.task_name, task);
    const byTask = await qf.dlq.list({ queue, taskName: task, limit: 10 });
    assert.ok(byTask.dead_letters.some((d) => d.id === entry.id));
    const otherTask = await qf.dlq.list({ queue, taskName: `nope-${suffix}`, limit: 10 });
    assert.equal(otherTask.dead_letters.length, 0);
    const alreadyReplayed = await qf.dlq.list({ queue, replayed: true, limit: 10 });
    assert.ok(!alreadyReplayed.dead_letters.some((d) => d.id === entry.id));

    // Bulk replay, scoped to this run's queue.
    const res = await qf.dlq.replayMany({ queue, taskName: task });
    assert.equal(res.replayed, 1);
    assert.equal(res.results.length, 1);
    assert.equal(res.results[0].dead_letter_id, entry.id);
    assert.equal(typeof res.results[0].job_id, "string");
    assert.ok(!res.results[0].error, `unexpected error: ${res.results[0].error}`);

    const fresh = await qf.jobs.get(res.results[0].job_id);
    assert.equal(fresh.task_name, task);
    assert.notEqual(fresh.id, job.id);

    // The entry has moved from the unreplayed to the replayed set...
    const after = await qf.dlq.list({ queue, replayed: true, limit: 10 });
    assert.ok(after.dead_letters.some((d) => d.id === entry.id));
    const stillUnreplayed = await qf.dlq.list({ queue, replayed: false, limit: 10 });
    assert.ok(!stillUnreplayed.dead_letters.some((d) => d.id === entry.id));

    // ...so a second bulk replay finds nothing to do.
    const again = await qf.dlq.replayMany({ queue });
    assert.equal(again.replayed, 0);
    assert.deepEqual(again.results, []);
  } finally {
    // The replayed job is pending on a queue nobody polls; do not leave it behind.
    await qf.jobs.cancelMany({ queue }).catch(() => {});
  }
});

// --- server 0.6.0: workflows v2 -------------------------------------------
//
// Every step below names this run's own queue, and this run's own worker is
// the only one polling it, so the shared server's in-process handlers never
// see these jobs. Each test starts a worker with just the handlers it needs
// and stops it before returning.

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Run `fn` while a worker with `handlers` polls `queue`; stop it afterwards. */
async function withWorker(queue, handlers, fn) {
  const controller = new AbortController();
  const worker = qf.worker.run(queue, handlers, {
    concurrency: 4,
    leaseSecs: 10,
    waitSecs: 2,
    signal: controller.signal,
  });
  try {
    return await fn();
  } finally {
    controller.abort();
    const result = await worker;
    assert.deepEqual(result.unfinished, [], "worker stopped with jobs still in flight");
  }
}

/** The handler's view of the workflow context, minus the injected key itself. */
const contextOf = (job) => job.payload._context ?? {};

test("live: a false `when` skips the step with `condition not met` and its dependents still run", { skip }, async () => {
  const queue = `node-live-${suffix}-cond`;
  const task = `node-live-cond-task-${suffix}`;
  const seen = [];

  await withWorker(queue, {
    [task]: async (job) => {
      seen.push(job.payload.step);
      return { step: job.payload.step, upstream: Object.keys(contextOf(job)).sort() };
    },
  }, async () => {
    const workflow = await qf.workflows.create(
      wf(`node-live-cond-${suffix}`)
        .step("validate", task, { queue, payload: { step: "validate", total: 5 } })
        .step("review", task, {
          queue,
          after: ["validate"],
          payload: { step: "review" },
          when: { path: "validate.total", op: "gt", value: 100 },
        })
        .step("confirm", task, { queue, after: ["validate", "review"], payload: { step: "confirm" } }),
    );
    assert.equal(workflow.steps[1].when.op, "gt");
    assert.equal(workflow.steps[0].queue, queue);

    // Follow the run over SSE while it completes; the stream must end on its own.
    const events = [];
    for await (const event of qf.workflows.watch(workflow.id)) events.push(event);
    assert.ok(events.length >= 1, "at least the current state is streamed");
    for (const event of events) assert.equal(event.workflow_id, workflow.id);
    const last = events[events.length - 1];
    assert.equal(last.status, "completed", `error: ${last.error_message}`);
    assert.deepEqual(last.steps.map((s) => s.name), ["validate", "review", "confirm"]);

    const finished = await qf.workflows.waitFor(workflow.id, WAIT);
    assert.equal(finished.status, "completed", `error: ${finished.error_message}`);
    assert.deepEqual(seen.sort(), ["confirm", "validate"], "the skipped step never ran");
    // The dependent ran without a result for the skipped step in its context.
    assert.deepEqual(finished.context.confirm.upstream, ["validate"]);
    assert.equal(finished.context.review, undefined);

    const steps = await qf.workflows.steps(workflow.id);
    const byName = Object.fromEntries(steps.map((s) => [s.name, s]));
    assert.equal(byName.validate.status, "completed");
    assert.equal(byName.review.status, "skipped");
    assert.equal(byName.review.error, "condition not met");
    assert.equal(byName.review.job_id ?? undefined, undefined, "a condition-skipped step has no job");
    assert.equal(byName.confirm.status, "completed");
    for (const name of ["validate", "confirm"]) {
      assert.ok(byName[name].started_at instanceof Date, `${name}.started_at`);
      assert.ok(byName[name].completed_at instanceof Date, `${name}.completed_at`);
      assert.equal(typeof byName[name].job_id, "string", `${name}.job_id`);
    }
  });
});

test("live: a fan-out runs one child per element and collects results in element order", { skip }, async () => {
  const queue = `node-live-${suffix}-fan`;
  const square = `node-live-square-${suffix}`;
  const sum = `node-live-sum-${suffix}`;
  const items = [1, 2, 3, 4];

  await withWorker(queue, {
    [square]: async (job) => {
      // Finish in reverse order so element order cannot be completion order.
      await sleep((items.length - job.payload._index) * 150);
      return { sq: job.payload.n * job.payload.n, index: job.payload._index };
    },
    [sum]: async (job) => ({
      total: contextOf(job).square.results.reduce((acc, r) => acc + r.sq, 0),
    }),
  }, async () => {
    const workflow = await qf.workflows.create(
      wf(`node-live-fan-${suffix}`)
        .context({ items })
        .step("square", square, { queue, fanOut: { path: "items", itemKey: "n" } })
        .step("sum", sum, { queue, after: ["square"] }),
    );
    assert.equal(workflow.steps[0].fan_out.path, "items");
    assert.equal(workflow.steps[0].fan_out.item_key, "n");

    const finished = await qf.workflows.waitFor(workflow.id, WAIT);
    assert.equal(finished.status, "completed", `error: ${finished.error_message}`);
    assert.deepEqual(
      finished.context.square,
      { results: items.map((n, index) => ({ sq: n * n, index })) },
    );
    assert.equal(finished.context.sum.total, 30);

    const steps = await qf.workflows.steps(workflow.id);
    const fan = steps.find((s) => s.name === "square");
    assert.equal(fan.status, "completed");
    assert.deepEqual(fan.fan_out, { total: items.length, completed: items.length, failed: 0 });
    assert.equal(typeof fan.job_id, "string", "job_id points at the first child");
    assert.equal(steps.find((s) => s.name === "sum").fan_out ?? undefined, undefined);

    // Every child is an ordinary job on this run's queue, tagged with its position.
    const children = await qf.jobs.list({ queue, taskName: square, limit: 50 });
    assert.equal(children.jobs.length, items.length);
    const positions = children.jobs.map((j) => j.metadata.fan_out_index).sort();
    assert.deepEqual(positions, [0, 1, 2, 3]);
    for (const job of children.jobs) {
      assert.equal(job.metadata.fan_out_total, items.length);
      assert.equal(job.workflow_id, workflow.id);
      assert.equal(job.status, "completed");
    }
    assert.ok(children.jobs.some((j) => j.id === fan.job_id));
  });
});

test("live: a sub-workflow step spawns a child workflow linked to its parent", { skip }, async () => {
  const queue = `node-live-${suffix}-sub`;
  const charge = `node-live-charge-${suffix}`;
  const pick = `node-live-pick-${suffix}`;
  const pack = `node-live-pack-${suffix}`;
  const done = `node-live-done-${suffix}`;

  await withWorker(queue, {
    [charge]: async (job) => ({ charge_id: `ch_${job.payload.amount}` }),
    [pick]: async (job) => {
      const ctx = contextOf(job);
      // The child's context: its own context, the step payload, the parent under `_parent`.
      return { picked: true, warehouse: ctx.warehouse, label: ctx.label, parent_charge: ctx._parent.charge.charge_id };
    },
    [pack]: async (job) => ({ packed: contextOf(job).pick.picked === true }),
    [done]: async (job) => ({ shipped: contextOf(job).fulfil.pack.packed === true }),
  }, async () => {
    const child = wf("fulfilment")
      .context({ label: "express" })
      .step("pick", pick, { queue })
      .step("pack", pack, { queue, after: ["pick"] })
      .timeout(600);
    const workflow = await qf.workflows.create(
      wf(`node-live-sub-${suffix}`)
        .step("charge", charge, { queue, payload: { amount: 7 } })
        .subWorkflow("fulfil", child, { after: ["charge"], payload: { warehouse: "eu-1" } })
        .step("done", done, { queue, after: ["fulfil"] })
        .timeout(900),
    );
    assert.equal(workflow.timeout_secs, 900);
    assert.ok(workflow.deadline_at instanceof Date);
    // Server 0.6.0 stores a sub-workflow step's task_name as "" rather than omitting it.
    assert.ok(!workflow.steps[1].task_name, `unexpected task_name ${JSON.stringify(workflow.steps[1].task_name)}`);
    assert.equal(workflow.steps[1].workflow.name, "fulfilment");
    assert.equal(workflow.parent_workflow_id ?? undefined, undefined);

    const finished = await qf.workflows.waitFor(workflow.id, WAIT);
    assert.equal(finished.status, "completed", `error: ${finished.error_message}`);
    // The step's result is the child's final context.
    assert.equal(finished.context.fulfil.pick.picked, true);
    assert.equal(finished.context.fulfil.pick.warehouse, "eu-1");
    assert.equal(finished.context.fulfil.pick.label, "express");
    assert.equal(finished.context.fulfil.pick.parent_charge, "ch_7");
    assert.equal(finished.context.fulfil.pack.packed, true);
    assert.equal(finished.context.done.shipped, true);

    const steps = await qf.workflows.steps(workflow.id);
    const fulfil = steps.find((s) => s.name === "fulfil");
    assert.equal(fulfil.status, "completed");
    assert.equal(typeof fulfil.child_workflow_id, "string");

    const childRun = await qf.workflows.get(fulfil.child_workflow_id);
    assert.equal(childRun.name, "fulfilment");
    assert.equal(childRun.status, "completed");
    assert.equal(childRun.parent_workflow_id, workflow.id);
    assert.equal(childRun.parent_step_name, "fulfil");
    assert.equal(childRun.timeout_secs, 600);
    assert.equal(childRun.context._parent.charge.charge_id, "ch_7");
    const childSteps = await qf.workflows.steps(childRun.id);
    assert.deepEqual(childSteps.map((s) => [s.name, s.status]), [["pick", "completed"], ["pack", "completed"]]);
  });
});

test("live: retry re-runs the failed step and its cancelled dependents, keeping completed ones", { skip }, async () => {
  const queue = `node-live-${suffix}-retry`;
  const first = `node-live-first-${suffix}`;
  const flaky = `node-live-flaky-${suffix}`;
  const last = `node-live-last-${suffix}`;
  let failNext = true;
  const runs = { first: 0, flaky: 0, last: 0 };

  await withWorker(queue, {
    [first]: async () => ({ n: ++runs.first }),
    [flaky]: async () => {
      runs.flaky += 1;
      if (failNext) throw new NonRetryableError("live test: fail once");
      return { n: runs.flaky };
    },
    [last]: async () => ({ n: ++runs.last }),
  }, async () => {
    const workflow = await qf.workflows.create(
      wf(`node-live-retry-${suffix}`)
        .step("first", first, { queue })
        .step("flaky", flaky, { queue, after: ["first"] })
        .step("last", last, { queue, after: ["flaky"] })
        .timeout(600),
    );
    assert.equal(workflow.retry_count, 0);

    const failed = await qf.workflows.waitFor(workflow.id, WAIT);
    assert.equal(failed.status, "failed");
    assert.match(failed.error_message, /fail once/);
    const before = Object.fromEntries((await qf.workflows.steps(workflow.id)).map((s) => [s.name, s]));
    assert.equal(before.first.status, "completed");
    assert.equal(before.flaky.status, "failed");
    assert.match(before.flaky.error, /fail once/);
    assert.equal(before.last.status, "cancelled");
    assert.equal(before.last.job_id ?? undefined, undefined, "a halted dependent never got a job");
    assert.deepEqual(runs, { first: 1, flaky: 1, last: 0 });

    failNext = false;
    await qf.workflows.retry(workflow.id);

    const finished = await qf.workflows.waitFor(workflow.id, WAIT);
    assert.equal(finished.status, "completed", `error: ${finished.error_message}`);
    assert.equal(finished.retry_count, 1);
    assert.equal(finished.error_message ?? undefined, undefined, "a retry clears the error");
    assert.ok(finished.deadline_at instanceof Date);
    assert.ok(finished.deadline_at > failed.deadline_at, "the deadline is re-armed");
    // The completed step kept its result and did not run again.
    assert.deepEqual(runs, { first: 1, flaky: 2, last: 1 });
    assert.deepEqual(finished.context, { first: { n: 1 }, flaky: { n: 2 }, last: { n: 1 } });

    const after = Object.fromEntries((await qf.workflows.steps(workflow.id)).map((s) => [s.name, s]));
    for (const name of ["first", "flaky", "last"]) assert.equal(after[name].status, "completed", name);
    assert.equal(after.first.job_id, before.first.job_id, "completed steps keep their job");
    assert.notEqual(after.flaky.job_id, before.flaky.job_id, "the failed step got a fresh job");
    assert.equal(after.flaky.error ?? undefined, undefined);
    assert.equal(typeof after.last.job_id, "string");

    // Retrying a workflow that is not failed, partially failed or cancelled is a 409.
    await assert.rejects(() => qf.workflows.retry(workflow.id), ConflictError);
  });
});
