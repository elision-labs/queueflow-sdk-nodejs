/**
 * Typed error hierarchy for `@queueflow/sdk`.
 *
 * The error classes have no OpenAPI representation, so they stay hand-written.
 * Values are produced by mapping the generated core's runtime errors
 * (`ResponseError` for non-2xx, `FetchError` for transport failures) into this
 * hierarchy, preserving the exact public surface of the previous SDK.
 */

import { ResponseError, FetchError } from "../core/src/runtime";

/** Base class for every error thrown by the SDK. */
export class QueueFlowError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message);
    this.name = "QueueFlowError";
    if (options?.cause !== undefined) {
      (this as { cause?: unknown }).cause = options.cause;
    }
  }
}

/** A non-2xx HTTP response from the API. */
export class ApiError extends QueueFlowError {
  /** HTTP status code. */
  readonly status: number;
  /** Server-provided error body, when present. */
  readonly body?: unknown;
  /** The operation that failed, for debugging. */
  readonly request: { method: string; path: string };

  constructor(args: {
    status: number;
    message: string;
    body?: unknown;
    request: { method: string; path: string };
  }) {
    super(args.message);
    this.name = "ApiError";
    this.status = args.status;
    this.body = args.body;
    this.request = args.request;
  }
}

/** 400 — invalid request (e.g. a workflow dependency cycle). */
export class BadRequestError extends ApiError {
  constructor(a: ConstructorParameters<typeof ApiError>[0]) {
    super(a);
    this.name = "BadRequestError";
  }
}
/** 401 — missing or invalid bearer token. */
export class UnauthorizedError extends ApiError {
  constructor(a: ConstructorParameters<typeof ApiError>[0]) {
    super(a);
    this.name = "UnauthorizedError";
  }
}
/** 403 — authenticated, but the resource belongs to another tenant. */
export class ForbiddenError extends ApiError {
  constructor(a: ConstructorParameters<typeof ApiError>[0]) {
    super(a);
    this.name = "ForbiddenError";
  }
}
/** 404 — no such job/workflow. */
export class NotFoundError extends ApiError {
  constructor(a: ConstructorParameters<typeof ApiError>[0]) {
    super(a);
    this.name = "NotFoundError";
  }
}
/** 409 — conflicting state (e.g. cancel a finished job, stale lease token). */
export class ConflictError extends ApiError {
  constructor(a: ConstructorParameters<typeof ApiError>[0]) {
    super(a);
    this.name = "ConflictError";
  }
}

/** A network failure, DNS error, or aborted request (no HTTP response). */
export class ConnectionError extends QueueFlowError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "ConnectionError";
  }
}

/** A `waitFor` poll that exceeded its deadline. */
export class TimeoutError extends QueueFlowError {
  /** The id of the resource being polled. */
  readonly id: string;
  constructor(message: string, id: string) {
    super(message);
    this.name = "TimeoutError";
    this.id = id;
  }
}

/** Map an HTTP status to the most specific error subclass. */
function errorForStatus(args: ConstructorParameters<typeof ApiError>[0]): ApiError {
  switch (args.status) {
    case 400:
      return new BadRequestError(args);
    case 401:
      return new UnauthorizedError(args);
    case 403:
      return new ForbiddenError(args);
    case 404:
      return new NotFoundError(args);
    case 409:
      return new ConflictError(args);
    default:
      return new ApiError(args);
  }
}

/**
 * Convert a value thrown by the generated core into the SDK's typed error.
 * `ResponseError` carries the raw, still-unread `Response`; `FetchError` wraps
 * a transport failure.
 */
export async function toQueueFlowError(
  err: unknown,
  label: string,
): Promise<QueueFlowError> {
  if (err instanceof ResponseError) {
    const res = err.response;
    let body: unknown;
    let message = `${res.status} ${res.statusText}`;
    try {
      const text = await res.text();
      if (text) {
        body = JSON.parse(text);
        const e = body as { error?: unknown };
        if (e && typeof e.error === "string") message = e.error;
      }
    } catch {
      /* non-JSON error body; keep the status-line message */
    }
    return errorForStatus({
      status: res.status,
      message,
      body,
      request: { method: label, path: "" },
    });
  }
  if (err instanceof FetchError) {
    return new ConnectionError(`Network error during ${label}`, {
      cause: err.cause,
    });
  }
  if (err instanceof QueueFlowError) return err;
  return new QueueFlowError(`Unexpected error during ${label}`, { cause: err });
}
