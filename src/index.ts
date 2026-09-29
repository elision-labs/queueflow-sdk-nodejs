/**
 * `@queueflow/sdk` — ergonomic TypeScript client for QueueFlow.
 *
 * Public surface = hand-written ergonomics (this `src/` facade) + the wire
 * types re-exported from the generated `../core`. The transport, models, and
 * (de)serialization live in `../core` and are regenerated from the OpenAPI
 * spec; they are never hand-edited.
 *
 * @see https://queueflow.dev
 */

export {
  QueueFlow,
  JobsResource,
  WorkflowsResource,
  WorkerResource,
  CronResource,
  DlqResource,
  SystemResource,
} from "./client";
export type {
  QueueFlowOptions,
  CreateJobInput,
  CreateCronInput,
  ListOptions,
  WaitOptions,
  WorkerContext,
  WorkerHandler,
} from "./client";

export { wf, WorkflowBuilder, WorkflowValidationError } from "./workflow";
export type { StepOptions } from "./workflow";

export {
  QueueFlowError,
  ApiError,
  BadRequestError,
  UnauthorizedError,
  ForbiddenError,
  NotFoundError,
  ConflictError,
  ConnectionError,
  TimeoutError,
  AbortError,
} from "./errors";

export type { Json, JsonObject } from "./json";

// Wire types come straight from the generated core — single source of truth.
export type {
  Job,
  JobConfig,
  JobStatus,
  BackoffStrategy,
  Workflow,
  WorkflowStep,
  WorkflowStatus,
  OnFailure,
  OnSuccess,
  LeasedJob,
  StatsSnapshot,
  CronSchedule,
  ListCronsResponse,
  DeadLetter,
  ListDeadLettersResponse,
  ListJobsResponse,
  ListWorkflowsResponse,
  CreateBatchJobsResponse,
  WorkflowDiagramResponse,
  HealthStatus,
  ReadyStatus,
} from "../core/src/models/index";
