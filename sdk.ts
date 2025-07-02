// QueueFlow TypeScript SDK - High-performance TypeScript client for QueueFlow
// src/index.ts

import axios, { AxiosInstance, AxiosError, AxiosRequestConfig } from 'axios';
import { EventEmitter } from 'events';
import pLimit from 'p-limit';
import pRetry from 'p-retry';
import { URL } from 'url';

// Version
export const VERSION = '1.0.0';

// Default configuration values
export const DEFAULT_TIMEOUT = 30000; // 30 seconds
export const DEFAULT_MAX_RETRIES = 3;
export const DEFAULT_RETRY_DELAY = 1000; // 1 second
export const DEFAULT_USER_AGENT = `QueueFlow-Node-SDK/${VERSION}`;
export const DEFAULT_QUEUE = 'default';

// Job Status enum
export enum JobStatus {
  Pending = 'pending',
  Running = 'running',
  Completed = 'completed',
  Failed = 'failed',
  Retrying = 'retrying',
  Cancelled = 'cancelled',
}

// Workflow Status enum
export enum WorkflowStatus {
  Created = 'created',
  Running = 'running',
  Completed = 'completed',
  Failed = 'failed',
  Cancelled = 'cancelled',
}

// Error types
export class QueueFlowError extends Error {
  constructor(
    message: string,
    public code?: string,
    public status?: number
  ) {
    super(message);
    this.name = 'QueueFlowError';
  }
}

export class ValidationError extends QueueFlowError {
  constructor(message: string, public field?: string) {
    super(message, 'VALIDATION_ERROR');
    this.name = 'ValidationError';
  }
}

export class RateLimitError extends QueueFlowError {
  constructor(message: string, public retryAfter: number) {
    super(message, 'RATE_LIMIT_ERROR', 429);
    this.name = 'RateLimitError';
  }
}

export class AuthenticationError extends QueueFlowError {
  constructor(message: string) {
    super(message, 'AUTHENTICATION_ERROR', 401);
    this.name = 'AuthenticationError';
  }
}

export class TimeoutError extends QueueFlowError {
  constructor(message: string, public timeout: number) {
    super(message, 'TIMEOUT_ERROR');
    this.name = 'TimeoutError';
  }
}

// Interfaces
export interface JobConfig {
  priority?: number;      // -10 to 10
  maxRetries?: number;    // 0 to 10
  retryDelay?: number;    // seconds
  timeout?: number;       // seconds
  queue?: string;
}

export interface WorkflowStep {
  name: string;
  taskName: string;
  payload: Record<string, any>;
  dependsOn?: string[];
  config?: JobConfig;
}

export interface Job {
  id: string;
  queueName: string;
  taskName: string;
  status: JobStatus;
  createdAt: Date;
  startedAt?: Date;
  completedAt?: Date;
  errorMessage?: string;
  retryCount: number;
  workflowId?: string;
  result?: Record<string, any>;
}

export interface Workflow {
  id: string;
  name: string;
  status: WorkflowStatus;
  createdAt: Date;
  startedAt?: Date;
  completedAt?: Date;
  steps: WorkflowStepInfo[];
  context: Record<string, any>;
}

export interface WorkflowStepInfo {
  name: string;
  taskName: string;
  status: string;
  jobId?: string;
  errorMessage?: string;
}

export interface SystemStats {
  jobs: {
    pending: number;
    running: number;
    completed: number;
    failed: number;
    total: number;
  };
  workflows: {
    running: number;
    completed: number;
    failed: number;
    total: number;
  };
  workers: {
    active: number;
    total: number;
  };
  queues: Array<{
    name: string;
    size: number;
    processing: number;
  }>;
}

export interface HealthStatus {
  status: string;
  database: string;
  activeWorkers: number;
  timestamp: Date;
}

export interface ListResponse<T> {
  items: T[];
  total: number;
  limit: number;
  offset: number;
  hasMore: boolean;
}

export interface BatchJobRequest {
  taskName: string;
  payload: Record<string, any>;
  config?: JobConfig;
}

export interface ClientConfig {
  timeout?: number;
  maxRetries?: number;
  retryDelay?: number;
  userAgent?: string;
  defaultQueue?: string;
  // HTTP client options
  maxSockets?: number;
  keepAlive?: boolean;
  keepAliveMsecs?: number;
  // Advanced options
  enableCircuitBreaker?: boolean;
  circuitBreakerOptions?: CircuitBreakerOptions;
  enableMetrics?: boolean;
  logger?: Logger;
}

export interface Logger {
  debug(message: string, meta?: any): void;
  info(message: string, meta?: any): void;
  warn(message: string, meta?: any): void;
  error(message: string, meta?: any): void;
}

// Circuit Breaker
export interface CircuitBreakerOptions {
  failureThreshold: number;
  resetTimeout: number;
  monitorInterval?: number;
}

export enum CircuitState {
  Closed = 'closed',
  Open = 'open',
  HalfOpen = 'half-open',
}

export class CircuitBreaker extends EventEmitter {
  private failureCount = 0;
  private lastFailureTime?: Date;
  private state: CircuitState = CircuitState.Closed;
  private halfOpenRequests = 0;
  private readonly maxHalfOpenRequests = 3;

  constructor(private options: CircuitBreakerOptions) {
    super();
  }

  async execute<T>(fn: () => Promise<T>): Promise<T> {
    const currentState = this.getState();

    if (currentState === CircuitState.Open) {
      throw new QueueFlowError('Circuit breaker is open', 'CIRCUIT_OPEN');
    }

    if (currentState === CircuitState.HalfOpen) {
      if (this.halfOpenRequests >= this.maxHalfOpenRequests) {
        throw new QueueFlowError('Circuit breaker half-open limit reached', 'CIRCUIT_HALF_OPEN_LIMIT');
      }
      this.halfOpenRequests++;
    }

    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    } finally {
      if (currentState === CircuitState.HalfOpen) {
        this.halfOpenRequests--;
      }
    }
  }

  private getState(): CircuitState {
    if (this.failureCount >= this.options.failureThreshold) {
      const timeSinceLastFailure = this.lastFailureTime
        ? Date.now() - this.lastFailureTime.getTime()
        : 0;

      if (timeSinceLastFailure > this.options.resetTimeout) {
        this.state = CircuitState.HalfOpen;
      } else {
        this.state = CircuitState.Open;
      }
    } else {
      this.state = CircuitState.Closed;
    }

    return this.state;
  }

  private onSuccess(): void {
    if (this.state === CircuitState.HalfOpen) {
      this.reset();
    }
  }

  private onFailure(): void {
    this.failureCount++;
    this.lastFailureTime = new Date();
    this.emit('failure', { count: this.failureCount });
  }

  private reset(): void {
    this.failureCount = 0;
    this.lastFailureTime = undefined;
    this.state = CircuitState.Closed;
    this.emit('reset');
  }

  getStats() {
    return {
      state: this.state,
      failureCount: this.failureCount,
      lastFailureTime: this.lastFailureTime,
    };
  }
}

// Metrics Collector
export interface MetricsCollector {
  incrementCounter(name: string, tags?: Record<string, string>): void;
  recordHistogram(name: string, value: number, tags?: Record<string, string>): void;
  recordGauge(name: string, value: number, tags?: Record<string, string>): void;
}

// Simple in-memory metrics collector
export class InMemoryMetrics implements MetricsCollector {
  private counters = new Map<string, number>();
  private histograms = new Map<string, number[]>();
  private gauges = new Map<string, number>();

  incrementCounter(name: string, tags?: Record<string, string>): void {
    const key = this.getKey(name, tags);
    this.counters.set(key, (this.counters.get(key) || 0) + 1);
  }

  recordHistogram(name: string, value: number, tags?: Record<string, string>): void {
    const key = this.getKey(name, tags);
    const values = this.histograms.get(key) || [];
    values.push(value);
    this.histograms.set(key, values);
  }

  recordGauge(name: string, value: number, tags?: Record<string, string>): void {
    const key = this.getKey(name, tags);
    this.gauges.set(key, value);
  }

  private getKey(name: string, tags?: Record<string, string>): string {
    if (!tags) return name;
    const tagStr = Object.entries(tags)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}:${v}`)
      .join(',');
    return `${name}{${tagStr}}`;
  }

  getStats() {
    return {
      counters: Object.fromEntries(this.counters),
      histograms: Object.fromEntries(this.histograms),
      gauges: Object.fromEntries(this.gauges),
    };
  }
}

// Main Client Class
export class QueueFlowClient extends EventEmitter {
  private axios: AxiosInstance;
  private config: Required<ClientConfig>;
  private circuitBreaker?: CircuitBreaker;
  private metrics?: MetricsCollector;
  private logger?: Logger;
  private concurrencyLimit: any;

  constructor(
    private baseURL: string,
    private apiKey: string,
    config?: ClientConfig
  ) {
    super();

    // Validate inputs
    if (!baseURL) {
      throw new ValidationError('Base URL is required');
    }
    if (!apiKey) {
      throw new ValidationError('API key is required');
    }

    // Normalize base URL and add API version
    this.baseURL = baseURL.replace(/\/$/, '') + '/api/v1';

    // Set default config
    this.config = {
      timeout: config?.timeout ?? DEFAULT_TIMEOUT,
      maxRetries: config?.maxRetries ?? DEFAULT_MAX_RETRIES,
      retryDelay: config?.retryDelay ?? DEFAULT_RETRY_DELAY,
      userAgent: config?.userAgent ?? DEFAULT_USER_AGENT,
      defaultQueue: config?.defaultQueue ?? DEFAULT_QUEUE,
      maxSockets: config?.maxSockets ?? 50,
      keepAlive: config?.keepAlive ?? true,
      keepAliveMsecs: config?.keepAliveMsecs ?? 1000,
      enableCircuitBreaker: config?.enableCircuitBreaker ?? false,
      circuitBreakerOptions: config?.circuitBreakerOptions ?? {
        failureThreshold: 5,
        resetTimeout: 60000,
      },
      enableMetrics: config?.enableMetrics ?? false,
      logger: config?.logger,
    };

    // Create axios instance with optimized settings
    this.axios = axios.create({
      baseURL: this.baseURL,
      timeout: this.config.timeout,
      headers: {
        'Authorization': `Bearer ${this.apiKey}`,
        'Content-Type': 'application/json',
        'User-Agent': this.config.userAgent,
        'Accept': 'application/json',
      },
      // HTTP agent options for connection pooling
      httpAgent: this.createHttpAgent(),
      httpsAgent: this.createHttpsAgent(),
      // Disable automatic decompression for better performance
      decompress: true,
      maxContentLength: Infinity,
      maxBodyLength: Infinity,
    });

    // Setup interceptors
    this.setupInterceptors();

    // Initialize optional components
    if (this.config.enableCircuitBreaker) {
      this.circuitBreaker = new CircuitBreaker(this.config.circuitBreakerOptions);
      this.circuitBreaker.on('failure', (data) => {
        this.emit('circuit:failure', data);
      });
      this.circuitBreaker.on('reset', () => {
        this.emit('circuit:reset');
      });
    }

    if (this.config.enableMetrics) {
      this.metrics = new InMemoryMetrics();
    }

    this.logger = this.config.logger;

    // Set up concurrency limiter for bulk operations
    this.concurrencyLimit = pLimit(10); // Max 10 concurrent requests
  }

  private createHttpAgent() {
    const http = require('http');
    return new http.Agent({
      keepAlive: this.config.keepAlive,
      keepAliveMsecs: this.config.keepAliveMsecs,
      maxSockets: this.config.maxSockets,
      maxFreeSockets: Math.floor(this.config.maxSockets / 2),
      timeout: this.config.timeout,
    });
  }

  private createHttpsAgent() {
    const https = require('https');
    return new https.Agent({
      keepAlive: this.config.keepAlive,
      keepAliveMsecs: this.config.keepAliveMsecs,
      maxSockets: this.config.maxSockets,
      maxFreeSockets: Math.floor(this.config.maxSockets / 2),
      timeout: this.config.timeout,
    });
  }

  private setupInterceptors(): void {
    // Request interceptor for metrics
    this.axios.interceptors.request.use(
      (config) => {
        if (this.logger) {
          this.logger.debug('Making API request', {
            method: config.method,
            url: config.url,
            baseURL: config.baseURL,
          });
        }
        // Add request timestamp for metrics
        (config as any).metadata = { startTime: Date.now() };
        return config;
      },
      (error) => Promise.reject(error)
    );

    // Response interceptor for metrics and error handling
    this.axios.interceptors.response.use(
      (response) => {
        const duration = Date.now() - (response.config as any).metadata?.startTime || 0;
        
        if (this.metrics) {
          this.metrics.recordHistogram('api.request.duration', duration, {
            method: response.config.method || 'unknown',
            status: response.status.toString(),
            path: new URL(response.config.url || '', response.config.baseURL).pathname,
          });
        }

        if (this.logger) {
          this.logger.debug('API request completed', {
            method: response.config.method,
            url: response.config.url,
            status: response.status,
            duration,
          });
        }

        return response;
      },
      (error: AxiosError) => {
        const duration = Date.now() - ((error.config as any)?.metadata?.startTime || 0);
        
        if (this.metrics) {
          this.metrics.incrementCounter('api.request.errors', {
            method: error.config?.method || 'unknown',
            status: error.response?.status?.toString() || 'network_error',
          });
        }

        if (this.logger) {
          this.logger.error('API request failed', {
            method: error.config?.method,
            url: error.config?.url,
            status: error.response?.status,
            error: error.message,
            duration,
          });
        }

        return Promise.reject(this.handleAxiosError(error));
      }
    );
  }

  private handleAxiosError(error: AxiosError): Error {
    if (error.response) {
      const status = error.response.status;
      const data = error.response.data as any;

      switch (status) {
        case 401:
          return new AuthenticationError(data?.message || 'Authentication failed');
        case 429:
          const retryAfter = parseInt(error.response.headers['retry-after'] || '60');
          return new RateLimitError(data?.message || 'Rate limit exceeded', retryAfter);
        case 400:
          return new ValidationError(data?.message || 'Validation error', data?.field);
        default:
          return new QueueFlowError(
            data?.message || error.message,
            data?.code,
            status
          );
      }
    } else if (error.code === 'ECONNABORTED') {
      return new TimeoutError('Request timeout', this.config.timeout);
    } else {
      return new QueueFlowError(error.message, 'NETWORK_ERROR');
    }
  }

  private async makeRequest<T>(
    method: string,
    path: string,
    data?: any,
    config?: AxiosRequestConfig
  ): Promise<T> {
    const requestFn = async () => {
      const response = await this.axios.request<T>({
        method,
        url: path,
        data,
        ...config,
      });
      return response.data;
    };

    // Use circuit breaker if enabled
    if (this.circuitBreaker) {
      return this.circuitBreaker.execute(requestFn);
    }

    // Use retry logic
    return pRetry(requestFn, {
      retries: this.config.maxRetries,
      minTimeout: this.config.retryDelay,
      maxTimeout: this.config.retryDelay * 10,
      onFailedAttempt: (error) => {
        if (this.logger) {
          this.logger.warn(`Retry attempt ${error.attemptNumber} failed`, {
            error: error.message,
            retriesLeft: error.retriesLeft,
          });
        }
      },
      retryOptions: {
        // Don't retry on validation errors
        shouldRetry: (error: any) => {
          return !(error instanceof ValidationError || error instanceof AuthenticationError);
        },
      },
    } as any);
  }

  // Job Methods
  async createJob(
    taskName: string,
    payload: Record<string, any> = {},
    config?: JobConfig
  ): Promise<string> {
    if (!taskName) {
      throw new ValidationError('Task name is required');
    }

    const startTime = Date.now();
    
    try {
      const jobConfig: JobConfig = {
        priority: config?.priority ?? 0,
        maxRetries: config?.maxRetries ?? 3,
        retryDelay: config?.retryDelay ?? 60,
        timeout: config?.timeout ?? 300,
        queue: config?.queue ?? this.config.defaultQueue,
      };

      const response = await this.makeRequest<{ job_id: string }>('POST', '/jobs', {
        task_name: taskName,
        payload,
        config: {
          priority: jobConfig.priority,
          max_retries: jobConfig.maxRetries,
          timeout: jobConfig.timeout,
          queue: jobConfig.queue,
        }
      });

      if (this.metrics) {
        this.metrics.incrementCounter('jobs.created', { task: taskName });
        this.metrics.recordHistogram('jobs.create.duration', Date.now() - startTime, {
          task: taskName,
        });
      }

      this.emit('job:created', { jobId: response.job_id, taskName });

      return response.job_id;
    } catch (error) {
      if (this.metrics) {
        this.metrics.incrementCounter('jobs.create.errors', { task: taskName });
      }
      throw error;
    }
  }

  async getJob(jobId: string): Promise<Job> {
    if (!jobId) {
      throw new ValidationError('Job ID is required');
    }

    const response = await this.makeRequest<Job>('GET', `/jobs/${encodeURIComponent(jobId)}`);
    
    // Convert date strings to Date objects
    return {
      ...response,
      createdAt: new Date(response.createdAt),
      startedAt: response.startedAt ? new Date(response.startedAt) : undefined,
      completedAt: response.completedAt ? new Date(response.completedAt) : undefined,
    };
  }

  async cancelJob(jobId: string): Promise<boolean> {
    if (!jobId) {
      throw new ValidationError('Job ID is required');
    }

    const response = await this.makeRequest<{ success: boolean }>(
      'POST',
      `/jobs/${encodeURIComponent(jobId)}/cancel`
    );

    if (response.success) {
      this.emit('job:cancelled', { jobId });
    }

    return response.success;
  }

  async waitForJob(
    jobId: string,
    options: {
      timeout?: number;
      pollInterval?: number;
    } = {}
  ): Promise<Job> {
    const timeout = options.timeout ?? 300000; // 5 minutes default
    const pollInterval = options.pollInterval ?? 2000; // 2 seconds default

    const startTime = Date.now();

    return new Promise((resolve, reject) => {
      const checkJob = async () => {
        try {
          const job = await this.getJob(jobId);

          if (this.isTerminalJobStatus(job.status)) {
            if (this.metrics) {
              this.metrics.recordHistogram('jobs.wait.duration', Date.now() - startTime, {
                status: job.status,
              });
            }
            resolve(job);
            return;
          }

          if (Date.now() - startTime > timeout) {
            reject(new TimeoutError(`Job ${jobId} did not complete within timeout`, timeout));
            return;
          }

          setTimeout(checkJob, pollInterval);
        } catch (error) {
          reject(error);
        }
      };

      checkJob();
    });
  }

  private isTerminalJobStatus(status: JobStatus): boolean {
    return [JobStatus.Completed, JobStatus.Failed, JobStatus.Cancelled].includes(status);
  }

  // Workflow Methods
  async createWorkflow(name: string, steps: WorkflowStep[]): Promise<string> {
    if (!name) {
      throw new ValidationError('Workflow name is required');
    }

    if (!steps || steps.length === 0) {
      throw new ValidationError('Workflow must have at least one step');
    }

    // Validate steps
    const stepNames = new Set<string>();
    for (const step of steps) {
      if (!step.name) {
        throw new ValidationError('Step name is required');
      }
      if (!step.taskName) {
        throw new ValidationError('Step task name is required');
      }
      if (stepNames.has(step.name)) {
        throw new ValidationError(`Duplicate step name: ${step.name}`);
      }
      stepNames.add(step.name);

      // Validate dependencies
      if (step.dependsOn) {
        for (const dep of step.dependsOn) {
          if (!steps.some(s => s.name === dep)) {
            throw new ValidationError(`Step ${step.name} depends on unknown step: ${dep}`);
          }
        }
      }
    }

    const response = await this.makeRequest<{ workflow_id: string }>('POST', '/workflows', {
      name,
      steps: steps.map(step => ({
        name: step.name,
        task_name: step.taskName,
        payload: step.payload,
        depends_on: step.dependsOn,
        config: step.config ? {
          priority: step.config.priority,
          max_retries: step.config.maxRetries,
          timeout: step.config.timeout,
          queue: step.config.queue
        } : undefined
      }))
    });

    this.emit('workflow:created', { workflowId: response.workflow_id, name });

    return response.workflow_id;
  }

  async getWorkflow(workflowId: string): Promise<Workflow> {
    if (!workflowId) {
      throw new ValidationError('Workflow ID is required');
    }

    const response = await this.makeRequest<Workflow>(
      'GET',
      `/workflows/${encodeURIComponent(workflowId)}`
    );

    // Convert date strings to Date objects
    return {
      ...response,
      createdAt: new Date(response.createdAt),
      startedAt: response.startedAt ? new Date(response.startedAt) : undefined,
      completedAt: response.completedAt ? new Date(response.completedAt) : undefined,
    };
  }

  async cancelWorkflow(workflowId: string): Promise<boolean> {
    if (!workflowId) {
      throw new ValidationError('Workflow ID is required');
    }

    const response = await this.makeRequest<{ success: boolean }>(
      'POST',
      `/workflows/${encodeURIComponent(workflowId)}/cancel`
    );

    if (response.success) {
      this.emit('workflow:cancelled', { workflowId });
    }

    return response.success;
  }

  async waitForWorkflow(
    workflowId: string,
    options: {
      timeout?: number;
      pollInterval?: number;
    } = {}
  ): Promise<Workflow> {
    const timeout = options.timeout ?? 600000; // 10 minutes default
    const pollInterval = options.pollInterval ?? 5000; // 5 seconds default

    const startTime = Date.now();

    return new Promise((resolve, reject) => {
      const checkWorkflow = async () => {
        try {
          const workflow = await this.getWorkflow(workflowId);

          if (this.isTerminalWorkflowStatus(workflow.status)) {
            resolve(workflow);
            return;
          }

          if (Date.now() - startTime > timeout) {
            reject(new TimeoutError(`Workflow ${workflowId} did not complete within timeout`, timeout));
            return;
          }

          setTimeout(checkWorkflow, pollInterval);
        } catch (error) {
          reject(error);
        }
      };

      checkWorkflow();
    });
  }

  private isTerminalWorkflowStatus(status: WorkflowStatus): boolean {
    return [WorkflowStatus.Completed, WorkflowStatus.Failed, WorkflowStatus.Cancelled].includes(status);
  }

  // Batch Operations
  async createJobsBatch(jobs: BatchJobRequest[]): Promise<string[]> {
    if (!jobs || jobs.length === 0) {
      throw new ValidationError('Batch cannot be empty');
    }

    if (jobs.length > 100) {
      throw new ValidationError('Batch size cannot exceed 100 jobs');
    }

    // Validate each job
    jobs.forEach((job, index) => {
      if (!job.taskName) {
        throw new ValidationError(`Job at index ${index} missing task name`);
      }
    });

    const response = await this.makeRequest<{ job_ids: string[] }>('POST', '/jobs/batch', {
      jobs: jobs.map(job => ({
        task_name: job.taskName,
        payload: job.payload,
        config: job.config ? {
          priority: job.config.priority,
          max_retries: job.config.maxRetries,
          timeout: job.config.timeout,
          queue: job.config.queue
        } : undefined
      }))
    });

    if (this.metrics) {
      this.metrics.incrementCounter('jobs.batch.created', { count: jobs.length.toString() });
    }

    return response.job_ids;
  }

  // Advanced: Create jobs batch with progress
  async createJobsBatchWithProgress(
    jobs: BatchJobRequest[],
    onProgress?: (progress: {
      total: number;
      completed: number;
      failed: number;
      percent: number;
    }) => void
  ): Promise<{ jobIds: string[]; errors: Error[] }> {
    const chunkSize = 25;
    const chunks: BatchJobRequest[][] = [];
    
    for (let i = 0; i < jobs.length; i += chunkSize) {
      chunks.push(jobs.slice(i, i + chunkSize));
    }

    const results: string[] = [];
    const errors: Error[] = [];
    let completed = 0;
    let failed = 0;

    // Process chunks with concurrency limit
    await Promise.all(
      chunks.map((chunk, index) =>
        this.concurrencyLimit(async () => {
          try {
            const jobIds = await this.createJobsBatch(chunk);
            results.push(...jobIds);
            completed += chunk.length;
          } catch (error) {
            errors.push(error as Error);
            failed += chunk.length;
          }

          if (onProgress) {
            onProgress({
              total: jobs.length,
              completed,
              failed,
              percent: ((completed + failed) / jobs.length) * 100,
            });
          }
        })
      )
    );

    return { jobIds: results, errors };
  }

  // List Operations
  async listJobs(options: {
    limit?: number;
    offset?: number;
    status?: JobStatus;
  } = {}): Promise<ListResponse<Job>> {
    const params = new URLSearchParams();
    
    if (options.limit !== undefined) {
      params.append('limit', Math.min(Math.max(options.limit, 1), 1000).toString());
    }
    if (options.offset !== undefined) {
      params.append('offset', Math.max(options.offset, 0).toString());
    }
    if (options.status) {
      params.append('status', options.status);
    }

    const response = await this.makeRequest<{
      jobs: Job[];
      total: number;
      limit: number;
      offset: number;
    }>('GET', `/jobs?${params.toString()}`);

    return {
      items: response.jobs.map(job => ({
        ...job,
        createdAt: new Date(job.createdAt),
        startedAt: job.startedAt ? new Date(job.startedAt) : undefined,
        completedAt: job.completedAt ? new Date(job.completedAt) : undefined,
      })),
      total: response.total,
      limit: response.limit,
      offset: response.offset,
      hasMore: response.offset + response.jobs.length < response.total,
    };
  }

  async listWorkflows(options: {
    limit?: number;
    offset?: number;
    status?: WorkflowStatus;
  } = {}): Promise<ListResponse<Workflow>> {
    const params = new URLSearchParams();
    
    if (options.limit !== undefined) {
      params.append('limit', Math.min(Math.max(options.limit, 1), 1000).toString());
    }
    if (options.offset !== undefined) {
      params.append('offset', Math.max(options.offset, 0).toString());
    }
    if (options.status) {
      params.append('status', options.status);
    }

    const response = await this.makeRequest<{
      workflows: Workflow[];
      total: number;
      limit: number;
      offset: number;
    }>('GET', `/workflows?${params.toString()}`);

    return {
      items: response.workflows.map(workflow => ({
        ...workflow,
        createdAt: new Date(workflow.createdAt),
        startedAt: workflow.startedAt ? new Date(workflow.startedAt) : undefined,
        completedAt: workflow.completedAt ? new Date(workflow.completedAt) : undefined,
      })),
      total: response.total,
      limit: response.limit,
      offset: response.offset,
      hasMore: response.offset + response.workflows.length < response.total,
    };
  }

  // Streaming Operations
  async *streamJobs(options: {
    status?: JobStatus;
    batchSize?: number;
  } = {}): AsyncGenerator<Job, void, unknown> {
    const batchSize = options.batchSize ?? 100;
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const response = await this.listJobs({
        limit: batchSize,
        offset,
        status: options.status,
      });

      for (const job of response.items) {
        yield job;
      }

      hasMore = response.hasMore;
      offset += batchSize;
    }
  }

  // System Operations
  async getStats(): Promise<SystemStats> {
    return this.makeRequest<SystemStats>('GET', '/stats');
  }

  async getHealth(): Promise<HealthStatus> {
    const response = await this.makeRequest<HealthStatus>('GET', '/health');
    return {
      ...response,
      timestamp: new Date(response.timestamp),
    };
  }

  async testConnection(): Promise<boolean> {
    try {
      await this.getHealth();
      return true;
    } catch {
      return false;
    }
  }

  // Metrics and monitoring
  getMetrics(): any {
    if (!this.metrics || !(this.metrics instanceof InMemoryMetrics)) {
      return null;
    }
    return this.metrics.getStats();
  }

  getCircuitBreakerStats(): any {
    return this.circuitBreaker?.getStats() || null;
  }

  // Cleanup
  destroy(): void {
    // Clean up axios instance
    if (this.axios.defaults.httpAgent) {
      this.axios.defaults.httpAgent.destroy();
    }
    if (this.axios.defaults.httpsAgent) {
      this.axios.defaults.httpsAgent.destroy();
    }

    // Remove all event listeners
    this.removeAllListeners();

    if (this.circuitBreaker) {
      this.circuitBreaker.removeAllListeners();
    }
  }
}

// Factory functions
export function createClient(baseURL: string, apiKey: string, config?: ClientConfig): QueueFlowClient {
  return new QueueFlowClient(baseURL, apiKey, config);
}

export function createClientFromEnv(config?: ClientConfig): QueueFlowClient {
  const baseURL = process.env.QUEUEFLOW_API_URL;
  const apiKey = process.env.QUEUEFLOW_API_KEY;

  if (!baseURL) {
    throw new ValidationError('QUEUEFLOW_API_URL environment variable is not set');
  }
  if (!apiKey) {
    throw new ValidationError('QUEUEFLOW_API_KEY environment variable is not set');
  }

  return new QueueFlowClient(baseURL, apiKey, config);
}

// Export helper functions
export function isTerminalJobStatus(status: JobStatus): boolean {
  return [JobStatus.Completed, JobStatus.Failed, JobStatus.Cancelled].includes(status);
}

export function isTerminalWorkflowStatus(status: WorkflowStatus): boolean {
  return [WorkflowStatus.Completed, WorkflowStatus.Failed, WorkflowStatus.Cancelled].includes(status);
}

// WorkflowBuilder helper class
export class WorkflowBuilder {
  private steps: WorkflowStep[] = [];
  private stepIndex = new Map<string, number>();

  addStep(
    name: string,
    taskName: string,
    payload: Record<string, any> = {},
    options?: {
      dependsOn?: string | string[];
      config?: JobConfig;
    }
  ): WorkflowBuilder {
    if (this.stepIndex.has(name)) {
      throw new ValidationError(`Step with name '${name}' already exists`);
    }

    const dependsOn = options?.dependsOn
      ? Array.isArray(options.dependsOn)
        ? options.dependsOn
        : [options.dependsOn]
      : undefined;

    // Validate dependencies exist
    if (dependsOn) {
      for (const dep of dependsOn) {
        if (!this.stepIndex.has(dep)) {
          throw new ValidationError(`Dependency '${dep}' does not exist`);
        }
      }
    }

    this.steps.push({
      name,
      taskName,
      payload,
      dependsOn,
      config: options?.config,
    });

    this.stepIndex.set(name, this.steps.length - 1);
    return this;
  }

  build(): WorkflowStep[] {
    if (this.steps.length === 0) {
      throw new ValidationError('Workflow must have at least one step');
    }
    return [...this.steps];
  }

  // Helper to create parallel steps
  addParallelSteps(
    steps: Array<{
      name: string;
      taskName: string;
      payload?: Record<string, any>;
      config?: JobConfig;
    }>,
    options?: {
      dependsOn?: string | string[];
    }
  ): WorkflowBuilder {
    const dependsOn = options?.dependsOn
      ? Array.isArray(options.dependsOn)
        ? options.dependsOn
        : [options.dependsOn]
      : undefined;

    for (const step of steps) {
      this.addStep(step.name, step.taskName, step.payload || {}, {
        dependsOn,
        config: step.config,
      });
    }

    return this;
  }

  // Helper to create sequential steps
  addSequentialSteps(
    steps: Array<{
      name: string;
      taskName: string;
      payload?: Record<string, any>;
      config?: JobConfig;
    }>,
    options?: {
      startDependsOn?: string | string[];
    }
  ): WorkflowBuilder {
    let previousStep = options?.startDependsOn;

    for (const step of steps) {
      this.addStep(step.name, step.taskName, step.payload || {}, {
        dependsOn: previousStep,
        config: step.config,
      });
      previousStep = step.name;
    }

    return this;
  }

  // Visualize workflow as mermaid diagram
  toMermaid(): string {
    const lines = ['graph TD'];
    
    for (const step of this.steps) {
      lines.push(`    ${step.name}[${step.taskName}]`);
      
      if (step.dependsOn) {
        for (const dep of step.dependsOn) {
          lines.push(`    ${dep} --> ${step.name}`);
        }
      }
    }

    return lines.join('\n');
  }
}

// Re-export types
export type { AxiosError };