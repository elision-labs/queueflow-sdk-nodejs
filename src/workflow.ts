/**
 * A small, typed builder for declaring workflow DAGs.
 *
 * ```ts
 * const dag = wf("etl")
 *   .step("extract", "fetch_orders")
 *   .step("transform", "normalize", { after: ["extract"] })
 *   .step("load", "upsert", { after: ["transform"], onFailure: "halt" })
 *   .context({ run: "2026-06-07" });
 *
 * await qf.workflows.create(dag); // create() accepts a builder directly
 * ```
 *
 * The server validates the DAG too (cycles => 400), but `build()` catches the
 * common mistakes locally — duplicate names, dangling `after`, cycles, and the
 * 0.6.0 step-shape rules (a task or a sub-workflow, never a fan-out on a
 * sub-workflow step) — with a clearer error before a network round-trip.
 */

import { QueueFlowError } from "./errors";
import type { Json, JsonObject } from "./json";
import type {
  ConditionOp,
  CreateWorkflowRequest,
  JobConfig,
  OnFailure,
  OnSuccess,
  StepCondition,
  WorkflowStep,
} from "../core/src/models/index";

/**
 * A predicate over the workflow context, evaluated when the step becomes
 * ready (server 0.6.0). Exactly one of `path`, `all`, `any` or `not` is set.
 *
 * A leaf names a dotted `path` (step results live under their step name, so
 * `validate.ok` is the `ok` key of the `validate` step's result; numeric
 * segments index arrays) and compares the value there with `value` using
 * `op`. `op` defaults to `eq` when `value` is given and to `exists` when it
 * is not. A missing path never errors: it compares as absent.
 *
 * This is the wire `StepCondition` with `value` typed as JSON rather than
 * `object`, so primitives (`0.8`, `true`, `"eu"`) type-check.
 */
export interface Condition {
  path?: string;
  op?: ConditionOp;
  value?: Json;
  /** True when every member is true. */
  all?: Condition[];
  /** True when at least one member is true. */
  any?: Condition[];
  not?: Condition;
}

/** Run a step's task once per element of an array in the context (server 0.6.0). */
export interface FanOutOptions {
  /** Dotted path into the context, for example `fetch.images`. */
  path: string;
  /** Payload key each element is passed under (default `item`). */
  itemKey?: string;
  /** Upper bound on elements; more fails the step before any child runs (default and ceiling 10000). */
  maxItems?: number;
}

/** Options for a single workflow step. */
export interface StepOptions {
  /** Names of steps that must complete before this one runs. */
  after?: string[];
  /** JSON payload passed to the task handler. */
  payload?: JsonObject;
  /** Per-step execution config (retries, timeout, priority…). */
  config?: JobConfig | null;
  /** Failure policy: `halt` (default), `skip`, or `continue`. */
  onFailure?: OnFailure;
  onSuccess?: OnSuccess;
  metadata?: JsonObject;
  /** Queue for this step's job(s); the server's default queue when absent (server 0.6.0). */
  queue?: string;
  /**
   * Gate the step on the context: when false the step is skipped with the
   * reason `condition not met` and its dependents still run (server 0.6.0).
   */
  when?: Condition;
  /** Run the task once per element of a context array (server 0.6.0). */
  fanOut?: FanOutOptions;
}

/** Options for a sub-workflow step: everything in {@link StepOptions} except `fanOut`. */
export type SubWorkflowStepOptions = Omit<StepOptions, "fanOut">;

/** Thrown by `build()` when the declared DAG is structurally invalid. */
export class WorkflowValidationError extends QueueFlowError {
  constructor(message: string) {
    super(message);
    this.name = "WorkflowValidationError";
  }
}

/** The server rejects a whole-workflow timeout over 30 days. */
const MAX_TIMEOUT_SECS = 30 * 24 * 60 * 60;

export class WorkflowBuilder {
  private readonly steps: WorkflowStep[] = [];
  /** Sub-workflow children awaiting `build()`, parallel to `steps`. */
  private readonly children: (WorkflowBuilder | CreateWorkflowRequest | undefined)[] = [];
  private _context?: JsonObject;
  private _metadata?: JsonObject;
  private _timeoutSecs?: number;

  constructor(public readonly name: string) {
    if (!name) throw new WorkflowValidationError("workflow name is required");
  }

  /** Add a step that runs `taskName`, optionally gated on other steps. */
  step(name: string, taskName: string, options: StepOptions = {}): this {
    const step: WorkflowStep = { name, task_name: taskName };
    this.applyOptions(step, options);
    this.steps.push(step);
    this.children.push(undefined);
    return this;
  }

  /**
   * Add a step that runs an inline child workflow instead of a task (server
   * 0.6.0). `child` is another builder or a raw request body; a builder is
   * validated when this workflow is built. The child's context is its own
   * declared context, then this step's `payload`, then the parent's whole
   * context under `_parent`; the step's result is the child's final context.
   */
  subWorkflow(
    name: string,
    child: WorkflowBuilder | CreateWorkflowRequest,
    options: SubWorkflowStepOptions = {},
  ): this {
    const step: WorkflowStep = { name };
    this.applyOptions(step, options);
    // Built in build() so a child builder stays editable until then and its
    // validation errors surface alongside the parent's.
    this.steps.push(step);
    this.children.push(child);
    return this;
  }

  /**
   * Copy the camelCase options onto the wire step. Takes the full
   * {@link StepOptions} so a `fanOut` smuggled onto a sub-workflow step from
   * untyped code still reaches `build()` and is rejected there.
   */
  private applyOptions(step: WorkflowStep, options: StepOptions): void {
    if (options.after && options.after.length) step.depends_on = options.after;
    if (options.payload) step.payload = options.payload;
    if (options.config !== undefined) step.config = options.config;
    if (options.onFailure) step.on_failure = options.onFailure;
    if (options.onSuccess) step.on_success = options.onSuccess;
    if (options.metadata) step.metadata = options.metadata;
    if (options.queue !== undefined) step.queue = options.queue;
    // `Condition` is the wire shape with a JSON-typed `value`; the generated
    // serializer passes `value` through untouched.
    if (options.when) step.when = options.when as StepCondition;
    if (options.fanOut) {
      step.fan_out = { path: options.fanOut.path };
      if (options.fanOut.itemKey !== undefined) step.fan_out.item_key = options.fanOut.itemKey;
      if (options.fanOut.maxItems !== undefined) step.fan_out.max_items = options.fanOut.maxItems;
    }
  }

  /** Seed the shared workflow context (merged into downstream `_context`). */
  context(context: JsonObject): this {
    this._context = { ...this._context, ...context };
    return this;
  }

  metadata(metadata: JsonObject): this {
    this._metadata = { ...this._metadata, ...metadata };
    return this;
  }

  /**
   * Whole-workflow time budget in seconds (server 0.6.0). When it runs out
   * every unfinished step is cancelled and the workflow fails with
   * `workflow timed out after Ns`. A retry re-arms it.
   */
  timeout(secs: number): this {
    if (!Number.isInteger(secs) || secs <= 0 || secs > MAX_TIMEOUT_SECS) {
      throw new WorkflowValidationError(
        `workflow timeout must be a whole number of seconds between 1 and ${MAX_TIMEOUT_SECS}, got ${secs}`,
      );
    }
    this._timeoutSecs = secs;
    return this;
  }

  /** Validate the DAG and produce the request body sent to the API. */
  build(): CreateWorkflowRequest {
    if (this.steps.length === 0) {
      throw new WorkflowValidationError(
        `workflow "${this.name}" has no steps`,
      );
    }
    const steps = this.steps.map((step, i) => {
      const child = this.children[i];
      if (!child) return step;
      return {
        ...step,
        workflow: child instanceof WorkflowBuilder ? child.build() : child,
      };
    });
    validateSteps(steps);
    const req: CreateWorkflowRequest = {
      name: this.name,
      steps,
    };
    if (this._context) req.context = this._context;
    if (this._metadata) req.metadata = this._metadata;
    if (this._timeoutSecs !== undefined) req.timeout_secs = this._timeoutSecs;
    return req;
  }
}

function validateSteps(steps: WorkflowStep[]): void {
  const names = new Set<string>();
  for (const step of steps) {
    if (names.has(step.name)) {
      throw new WorkflowValidationError(
        `duplicate step name "${step.name}"`,
      );
    }
    names.add(step.name);
  }
  for (const step of steps) {
    for (const dep of step.depends_on ?? []) {
      if (!names.has(dep)) {
        throw new WorkflowValidationError(
          `step "${step.name}" depends on unknown step "${dep}"`,
        );
      }
    }
    validateShape(step);
  }
  assertAcyclic(steps);
}

/** The per-step rules the server added in 0.6.0, checked before the round-trip. */
function validateShape(step: WorkflowStep): void {
  const hasTask = typeof step.task_name === "string" && step.task_name.length > 0;
  const hasWorkflow = step.workflow != null;
  if (!hasTask && !hasWorkflow) {
    throw new WorkflowValidationError(
      `step "${step.name}" needs a task name or a sub-workflow`,
    );
  }
  if (hasWorkflow && step.fan_out != null) {
    throw new WorkflowValidationError(
      `step "${step.name}" cannot both fan out and run a sub-workflow`,
    );
  }
  if (step.fan_out != null && !step.fan_out.path) {
    throw new WorkflowValidationError(
      `step "${step.name}": fan_out needs a path`,
    );
  }
  if (step.when != null) validateCondition(step.when, `step "${step.name}": when`);
}

/** Structural check of a condition tree: one of path/all/any/not, and an `op` that has what it needs. */
function validateCondition(cond: StepCondition, where: string): void {
  const kinds = [cond.path != null, cond.all != null, cond.any != null, cond.not != null].filter(
    Boolean,
  ).length;
  if (kinds !== 1) {
    throw new WorkflowValidationError(
      `${where}: a condition needs exactly one of path, all, any or not`,
    );
  }
  if (cond.path != null) {
    if (cond.path === "") {
      throw new WorkflowValidationError(`${where}: path must not be empty`);
    }
    const hasValue = cond.value !== undefined && cond.value !== null;
    if (cond.op != null && cond.op !== "exists" && !hasValue) {
      throw new WorkflowValidationError(`${where}: op ${cond.op} needs a value`);
    }
    return;
  }
  if (cond.op != null || cond.value !== undefined) {
    throw new WorkflowValidationError(
      `${where}: op and value belong on a leaf with a path`,
    );
  }
  for (const member of cond.all ?? cond.any ?? []) validateCondition(member, where);
  if (cond.not != null) validateCondition(cond.not, where);
}

/** Depth-first cycle detection over `depends_on` edges. */
function assertAcyclic(steps: WorkflowStep[]): void {
  const byName = new Map(steps.map((s) => [s.name, s]));
  const visiting = new Set<string>();
  const done = new Set<string>();
  const stack: string[] = [];

  const visit = (name: string): void => {
    if (done.has(name)) return;
    if (visiting.has(name)) {
      const cycle = [...stack.slice(stack.indexOf(name)), name].join(" -> ");
      throw new WorkflowValidationError(`dependency cycle: ${cycle}`);
    }
    visiting.add(name);
    stack.push(name);
    for (const dep of byName.get(name)?.depends_on ?? []) visit(dep);
    stack.pop();
    visiting.delete(name);
    done.add(name);
  };

  for (const step of steps) visit(step.name);
}

/** Start a new workflow definition. Shorthand for `new WorkflowBuilder(name)`. */
export function wf(name: string): WorkflowBuilder {
  return new WorkflowBuilder(name);
}
