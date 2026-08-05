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
 * common mistakes locally — duplicate names, dangling `after`, and cycles —
 * with a clearer error before a network round-trip.
 */

import { QueueFlowError } from "./errors";
import type { JsonObject } from "./json";
import type {
  CreateWorkflowRequest,
  JobConfig,
  OnFailure,
  OnSuccess,
  WorkflowStep,
} from "../core/src/models/index";

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
}

/** Thrown by `build()` when the declared DAG is structurally invalid. */
export class WorkflowValidationError extends QueueFlowError {
  constructor(message: string) {
    super(message);
    this.name = "WorkflowValidationError";
  }
}

export class WorkflowBuilder {
  private readonly steps: WorkflowStep[] = [];
  private _context?: JsonObject;
  private _metadata?: JsonObject;

  constructor(public readonly name: string) {
    if (!name) throw new WorkflowValidationError("workflow name is required");
  }

  /** Add a step that runs `taskName`, optionally gated on other steps. */
  step(name: string, taskName: string, options: StepOptions = {}): this {
    const step: WorkflowStep = { name, task_name: taskName };
    if (options.after && options.after.length) step.depends_on = options.after;
    if (options.payload) step.payload = options.payload;
    if (options.config !== undefined) step.config = options.config;
    if (options.onFailure) step.on_failure = options.onFailure;
    if (options.onSuccess) step.on_success = options.onSuccess;
    if (options.metadata) step.metadata = options.metadata;
    this.steps.push(step);
    return this;
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

  /** Validate the DAG and produce the request body sent to the API. */
  build(): CreateWorkflowRequest {
    if (this.steps.length === 0) {
      throw new WorkflowValidationError(
        `workflow "${this.name}" has no steps`,
      );
    }
    this.validate();
    const req: CreateWorkflowRequest = {
      name: this.name,
      steps: this.steps,
    };
    if (this._context) req.context = this._context;
    if (this._metadata) req.metadata = this._metadata;
    return req;
  }

  private validate(): void {
    const names = new Set<string>();
    for (const step of this.steps) {
      if (names.has(step.name)) {
        throw new WorkflowValidationError(
          `duplicate step name "${step.name}"`,
        );
      }
      names.add(step.name);
    }
    for (const step of this.steps) {
      for (const dep of step.depends_on ?? []) {
        if (!names.has(dep)) {
          throw new WorkflowValidationError(
            `step "${step.name}" depends on unknown step "${dep}"`,
          );
        }
      }
    }
    this.assertAcyclic();
  }

  /** Depth-first cycle detection over `depends_on` edges. */
  private assertAcyclic(): void {
    const byName = new Map(this.steps.map((s) => [s.name, s]));
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

    for (const step of this.steps) visit(step.name);
  }
}

/** Start a new workflow definition. Shorthand for `new WorkflowBuilder(name)`. */
export function wf(name: string): WorkflowBuilder {
  return new WorkflowBuilder(name);
}
