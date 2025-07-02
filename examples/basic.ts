import {
  QueueFlowClient,
  JobStatus,
  WorkflowStatus,
  JobConfig,
  WorkflowStep,
  WorkflowBuilder,
  createClient,
  createClientFromEnv
} from '../sdk';

async function basicExamples() {
  // Initialize client
  const client = new QueueFlowClient('http://localhost:8080', 'your-api-key');

  try {
    // Example 1: Create a simple job
    const jobId = await client.createJob('send_email', {
      to: 'user@example.com',
      subject: 'Welcome!',
      body: 'Thank you for signing up.'
    });
    console.log(`Created job: ${jobId}`);

    // Example 2: Create a job with configuration
    const jobConfig: JobConfig = {
      priority: 5,
      maxRetries: 5,
      timeout: 600, // 10 minutes
      queue: 'high-priority'
    };

    const jobId2 = await client.createJob('process_data', {
      file_url: 'https://example.com/data.csv',
      format: 'csv'
    }, jobConfig);
    console.log(`Created high-priority job: ${jobId2}`);

    // Example 3: Create batch jobs
    const batchJobs = [
      {
        taskName: 'resize_image',
        payload: { image_url: 'https://example.com/image1.jpg', width: 800, height: 600 }
      },
      {
        taskName: 'resize_image',
        payload: { image_url: 'https://example.com/image2.jpg', width: 800, height: 600 }
      },
      {
        taskName: 'resize_image',
        payload: { image_url: 'https://example.com/image3.jpg', width: 800, height: 600 }
      }
    ];

    const jobIds = await client.createJobsBatch(batchJobs);
    console.log(`Created batch jobs: ${jobIds.join(', ')}`);

    // Example 4: Create a workflow
    const steps: WorkflowStep[] = [
      {
        name: 'download',
        taskName: 'download_file',
        payload: { url: 'https://example.com/video.mp4' }
      },
      {
        name: 'transcode',
        taskName: 'transcode_video',
        payload: { format: 'webm' },
        dependsOn: ['download']
      },
      {
        name: 'thumbnail',
        taskName: 'generate_thumbnail',
        payload: {},
        dependsOn: ['download']
      },
      {
        name: 'upload',
        taskName: 'upload_to_cdn',
        payload: {},
        dependsOn: ['transcode', 'thumbnail']
      }
    ];

    const workflowId = await client.createWorkflow('video_processing', steps);
    console.log(`Created workflow: ${workflowId}`);

    // Example 5: Monitor job status
    const job = await client.getJob(jobId);
    console.log(`Job status: ${job.status}`);
    console.log(`Created at: ${job.createdAt}`);

    // Example 6: Wait for job completion
    try {
      const completedJob = await client.waitForJob(jobId, {
        timeout: 300000, // 5 minutes
        pollInterval: 2000 // 2 seconds
      });
      console.log(`Job completed with status: ${completedJob.status}`);
      if (completedJob.result) {
        console.log('Result:', completedJob.result);
      }
    } catch (error) {
      console.error('Job failed or timed out:', error);
    }

    // Example 7: List jobs with filtering
    const jobsList = await client.listJobs({
      limit: 10,
      status: JobStatus.Pending
    });
    console.log(`Found ${jobsList.items.length} pending jobs (total: ${jobsList.total})`);

    // Example 8: Cancel a job
    const cancelled = await client.cancelJob(jobId2);
    if (cancelled) {
      console.log(`Successfully cancelled job: ${jobId2}`);
    }

    // Example 9: Monitor workflow
    const workflow = await client.getWorkflow(workflowId);
    console.log(`Workflow status: ${workflow.status}`);

    // Example 10: Wait for workflow completion
    try {
      const completedWorkflow = await client.waitForWorkflow(workflowId, {
        timeout: 600000, // 10 minutes
        pollInterval: 5000 // 5 seconds
      });
      console.log(`Workflow completed with status: ${completedWorkflow.status}`);
    } catch (error) {
      console.error('Workflow failed or timed out:', error);
    }

    // Example 11: Stream jobs (for large result sets)
    console.log('Streaming all pending jobs...');
    let count = 0;
    for await (const job of client.streamJobs({ status: JobStatus.Pending })) {
      console.log(`Job ${++count}: ${job.id} - ${job.taskName}`);
      if (count >= 20) break; // Limit for example
    }

    // Example 12: Health check
    const health = await client.getHealth();
    console.log('System health:', health);

    // Example 13: Get statistics
    const stats = await client.getStats();
    console.log('System stats:', stats);

  } finally {
    // Clean up
    client.destroy();
  }
}

async function workflowBuilderExample() {
  const client = createClient('http://localhost:8080', 'your-api-key');

  // Use WorkflowBuilder for complex workflows
  const builder = new WorkflowBuilder(client, 'data_processing_pipeline');

  // Add sequential steps
  builder.addSequentialSteps([
    {
      name: 'fetch_data',
      taskName: 'fetch_from_api',
      payload: { endpoint: 'https://api.example.com/data' }
    },
    {
      name: 'validate',
      taskName: 'validate_data',
      payload: { schema: 'v2' }
    },
    {
      name: 'transform',
      taskName: 'transform_data',
      payload: { format: 'parquet' }
    }
  ]);

  // Add parallel steps after transform
  builder.addParallelSteps([
    {
      name: 'store_warehouse',
      taskName: 'store_to_warehouse',
      payload: { table: 'processed_data' }
    },
    {
      name: 'store_cache',
      taskName: 'store_to_cache',
      payload: { ttl: 3600 }
    }
  ], { dependsOn: 'transform' });

  // Add final step that depends on both storage steps
  builder.addStep('notify', 'send_notification', { channel: 'slack' }, {
    dependsOn: ['store_warehouse', 'store_cache']
  });

  // Build and create workflow
  const steps = builder.build();
  const workflowId = await client.createWorkflow('data_pipeline', steps);
  console.log(`Created workflow with builder: ${workflowId}`);

  // Visualize workflow as Mermaid diagram
  console.log('\nWorkflow diagram:');
  console.log(builder.toMermaid());

  client.destroy();
}

async function eventHandlingExample() {
  const client = new QueueFlowClient('http://localhost:8080', 'your-api-key');

  // Listen to events
  client.on('job:created', ({ jobId, taskName }) => {
    console.log(`Event: Job created - ID: ${jobId}, Task: ${taskName}`);
  });

  client.on('job:cancelled', ({ jobId }) => {
    console.log(`Event: Job cancelled - ID: ${jobId}`);
  });

  client.on('workflow:created', ({ workflowId, name }) => {
    console.log(`Event: Workflow created - ID: ${workflowId}, Name: ${name}`);
  });

  client.on('circuit:failure', (data) => {
    console.log('Event: Circuit breaker failure:', data);
  });

  client.on('circuit:reset', () => {
    console.log('Event: Circuit breaker reset');
  });

  // Create some jobs to trigger events
  const jobId = await client.createJob('test_task', { data: 'test' });
  await client.cancelJob(jobId);

  client.destroy();
}

async function advancedBatchExample() {
  const client = new QueueFlowClient('http://localhost:8080', 'your-api-key');

  // Create large batch with progress monitoring
  const largeBatch = Array.from({ length: 250 }, (_, i) => ({
    taskName: 'process_item',
    payload: { item_id: i + 1, batch: Math.floor(i / 50) + 1 }
  }));

  console.log(`Creating ${largeBatch.length} jobs in batch...`);

  const { jobIds, errors } = await client.createJobsBatchWithProgress(
    largeBatch,
    (progress) => {
      console.log(
        `Progress: ${progress.completed}/${progress.total} ` +
        `(${progress.percent.toFixed(1)}%) - Failed: ${progress.failed}`
      );
    }
  );

  console.log(`Successfully created ${jobIds.length} jobs`);
  if (errors.length > 0) {
    console.error(`Failed to create ${errors.length} jobs:`, errors);
  }

  client.destroy();
}

async function metricsExample() {
  const client = new QueueFlowClient('http://localhost:8080', 'your-api-key', {
    enableMetrics: true
  });

  // Perform various operations
  for (let i = 0; i < 10; i++) {
    await client.createJob('test_task', { index: i });
  }

  await client.listJobs({ limit: 5 });
  await client.getStats();

  // Get metrics
  const metrics = client.getMetrics();
  console.log('Client metrics:', metrics);

  // Get circuit breaker stats
  const cbStats = client.getCircuitBreakerStats();
  console.log('Circuit breaker stats:', cbStats);

  client.destroy();
}

// Run examples
async function main() {
  console.log('=== Basic Examples ===');
  await basicExamples();

  console.log('\n=== Workflow Builder Example ===');
  await workflowBuilderExample();

  console.log('\n=== Event Handling Example ===');
  await eventHandlingExample();

  console.log('\n=== Advanced Batch Example ===');
  await advancedBatchExample();

  console.log('\n=== Metrics Example ===');
  await metricsExample();
}

// Run if executed directly
if (require.main === module) {
  main().catch(console.error);
}

export {
  basicExamples,
  workflowBuilderExample,
  eventHandlingExample,
  advancedBatchExample,
  metricsExample
};