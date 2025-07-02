# QueueFlow Node.js SDK

The official TypeScript SDK for QueueFlow distributed job queue system.

## Installation

```bash
npm install @queueflow/sdk
# or
yarn add @queueflow/sdk
```

## Quick Start

### TypeScript/ES6

```typescript
import { QueueFlowClient } from '@queueflow/sdk';

// Create client
const client = new QueueFlowClient('http://localhost:8080', 'your-api-key');

async function main() {
  // Create a job
  const job = await client.createJob({
    taskName: 'process_data',
    payload: { userId: 123, action: 'send_email' },
    config: {
      priority: 'high',
      retries: 3,
      timeout: 300000
    }
  });

  console.log(`Job created: ${job.id}`);

  // Get job status
  const status = await client.getJob(job.id);
  console.log(`Job status: ${status.status}`);
}

main().catch(console.error);
```

### CommonJS

```javascript
const { QueueFlowClient } = require('@queueflow/sdk');

const client = new QueueFlowClient('http://localhost:8080', 'your-api-key');

async function main() {
  const job = await client.createJob({
    taskName: 'process_data',
    payload: { userId: 123 }
  });
  
  console.log(`Job created: ${job.id}`);
}

main();
```

## Features

- ✅ Full TypeScript support with type definitions
- ✅ Promise-based async/await API
- ✅ Create and manage jobs
- ✅ Batch job operations
- ✅ Job status monitoring
- ✅ Workflow support
- ✅ Automatic retries with exponential backoff
- ✅ Request/response interceptors
- ✅ AbortController support for cancellation

## API Reference

### Client Configuration

```typescript
import { QueueFlowClient, QueueFlowConfig } from '@queueflow/sdk';

const config: QueueFlowConfig = {
  timeout: 30000,
  retries: 3,
  retryDelay: 1000,
  headers: {
    'Custom-Header': 'value'
  }
};

const client = new QueueFlowClient('http://localhost:8080', 'api-key', config);
```

### Jobs

```typescript
// Create a job
const job = await client.createJob({
  taskName: 'send_email',
  payload: { to: 'user@example.com', subject: 'Hello' },
  config: {
    priority: 'high',
    retries: 3,
    timeout: 60000,
    delay: 5000,
    queue: 'emails'
  }
});

// Get job status
const job = await client.getJob(jobId);

// Cancel job
await client.cancelJob(jobId);

// List jobs
const jobs = await client.listJobs({
  status: 'pending',
  limit: 50,
  offset: 0
});
```

### Batches

```typescript
// Create batch
const batch = await client.createBatch({
  jobs: [
    { taskName: 'task1', payload: { data: 'value1' } },
    { taskName: 'task2', payload: { data: 'value2' } }
  ]
});

// Get batch status
const batch = await client.getBatch(batchId);
```

### Workflows

```typescript
// Create workflow
const workflow = await client.createWorkflow({
  name: 'data_pipeline',
  steps: [
    {
      name: 'extract',
      taskName: 'extract_data',
      payload: { source: 'database' }
    },
    {
      name: 'transform',
      taskName: 'transform_data',
      dependsOn: ['extract'],
      payload: { format: 'json' }
    }
  ]
});

// Get workflow status
const workflow = await client.getWorkflow(workflowId);
```

## TypeScript Types

```typescript
interface Job {
  id: string;
  taskName: string;
  payload: Record<string, any>;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
  createdAt: string;
  updatedAt: string;
  result?: Record<string, any>;
  error?: string;
}

interface JobConfig {
  priority?: 'low' | 'normal' | 'high' | 'critical';
  retries?: number;
  timeout?: number;
  delay?: number;
  queue?: string;
}

interface CreateJobRequest {
  taskName: string;
  payload: Record<string, any>;
  config?: JobConfig;
}
```

## Error Handling

```typescript
import { QueueFlowError, NotFoundError, ValidationError } from '@queueflow/sdk';

try {
  const job = await client.createJob({
    taskName: 'process_data',
    payload: { userId: 123 }
  });
} catch (error) {
  if (error instanceof NotFoundError) {
    console.error('Resource not found:', error.message);
  } else if (error instanceof ValidationError) {
    console.error('Validation error:', error.message);
  } else if (error instanceof QueueFlowError) {
    console.error('QueueFlow error:', error.message);
  } else {
    console.error('Unknown error:', error);
  }
}
```

## Request Cancellation

```typescript
// Using AbortController
const controller = new AbortController();

const jobPromise = client.createJob({
  taskName: 'long_task',
  payload: { data: 'value' }
}, { signal: controller.signal });

// Cancel the request after 5 seconds
setTimeout(() => controller.abort(), 5000);

try {
  const job = await jobPromise;
} catch (error) {
  if (error.name === 'AbortError') {
    console.log('Request was cancelled');
  }
}
```

## Interceptors

```typescript
// Request interceptor
client.interceptors.request.use((config) => {
  config.headers['X-Request-ID'] = generateRequestId();
  return config;
});

// Response interceptor
client.interceptors.response.use(
  (response) => {
    console.log('Request successful:', response.status);
    return response;
  },
  (error) => {
    console.error('Request failed:', error.message);
    throw error;
  }
);
```

## Examples

### Polling for Job Completion

```typescript
async function waitForJob(jobId: string): Promise<Job> {
  while (true) {
    const job = await client.getJob(jobId);
    
    if (job.status === 'completed') {
      return job;
    } else if (job.status === 'failed') {
      throw new Error(`Job failed: ${job.error}`);
    }
    
    // Wait 5 seconds before polling again
    await new Promise(resolve => setTimeout(resolve, 5000));
  }
}
```

### Batch Processing with Progress

```typescript
async function processBatch(jobs: CreateJobRequest[]): Promise<void> {
  const batch = await client.createBatch({ jobs });
  
  console.log(`Batch created: ${batch.id}`);
  
  while (true) {
    const status = await client.getBatch(batch.id);
    
    const total = status.totalJobs;
    const completed = status.completedJobs;
    const failed = status.failedJobs;
    
    console.log(`Progress: ${completed}/${total} completed, ${failed} failed`);
    
    if (status.status === 'completed') {
      console.log('Batch completed successfully!');
      break;
    } else if (status.status === 'failed') {
      throw new Error('Batch processing failed');
    }
    
    await new Promise(resolve => setTimeout(resolve, 10000));
  }
}
```

## Development

```bash
# Install dependencies
npm install

# Build the project
npm run build

# Run tests
npm test

# Run tests with coverage
npm run test:coverage

# Lint code
npm run lint

# Type checking
npm run type-check
```

## License

MIT License