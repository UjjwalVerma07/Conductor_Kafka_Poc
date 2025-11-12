import { Node, Edge } from 'reactflow';
// Converter: turns the ReactFlow graph into a Conductor workflow JSON.
// It standardizes names and auto-wires buckets/keys based on node connections.

interface ConductorTask {
  name: string;
  taskReferenceName: string;
  type?: string;
  inputParameters?: any;
  sink?: string;
}

interface ConductorWorkflow {
  name: string;
  description: string;
  version: number;
  tasks: ConductorTask[];
  inputParameters: any[];
  outputParameters: any;
  schemaVersion: number;
  restartable?: boolean;
  workflowStatusListenerEnabled?: boolean;
  ownerEmail?: string;
}

/** Converts ReactFlow graph to Conductor workflow JSON */
export function convertToConductorJSON(
  nodes: Node[],
  edges: Edge[],
  workflowName: string
): ConductorWorkflow {
  // Build adjacency map for topological sort
  const adjacencyMap = new Map<string, string[]>();
  const inDegree = new Map<string, number>();

  nodes.forEach((node) => {
    adjacencyMap.set(node.id, []);
    inDegree.set(node.id, 0);
  });

  edges.forEach((edge) => {
    adjacencyMap.get(edge.source)?.push(edge.target);
    inDegree.set(edge.target, (inDegree.get(edge.target) || 0) + 1);
  });

  // Topological sort to determine task order
  const sortedNodes = topologicalSort(nodes, adjacencyMap, inDegree);

  // Convert nodes to Conductor tasks
  const tasks: ConductorTask[] = [];
  const outputParameters: any = {};
  const idToNode = new Map(nodes.map((n) => [n.id, n]));
  const incomingById = new Map<string, string[]>();

  edges.forEach((e) => {
    if (!incomingById.has(e.target)) incomingById.set(e.target, []);
    incomingById.get(e.target)!.push(e.source);
  });

  // Track computed outputs per node to feed into subsequent nodes
  const computedOutputs = new Map<string, { bucket: string; key: string; serviceName: string }>();
  let lastPublishOutputKey: string | null = null;

  // Find the first service node (excluding airflowTrigger and eventWait nodes)
  // This will be used to set raw-data input for the first service
  const serviceNodeTypes = new Set(['emailValidation', 'phoneValidation', 'enrichment']);
  let firstServiceNodeId: string | null = null;
  for (const node of sortedNodes) {
    if (serviceNodeTypes.has(node.type as string)) {
      const incoming = incomingById.get(node.id) || [];
      // Check if this node has no incoming service nodes (only eventWait or nothing)
      const hasIncomingService = incoming.some(srcId => {
        const srcNode = idToNode.get(srcId);
        return srcNode && serviceNodeTypes.has(srcNode.type as string);
      });
      if (!hasIncomingService) {
        firstServiceNodeId = node.id;
        break;
      }
    }
  }

  sortedNodes.forEach((node, index) => {
    const isFirstService = node.id === firstServiceNodeId;
    const task = convertNodeToTaskGeneric(node, index, incomingById, computedOutputs, idToNode, isFirstService);
    if (task) {
      tasks.push(task);

      // Collect outputs from wait_for_result tasks
      if (node.type === 'waitForResult') {
        const refName = task.taskReferenceName;
        // Conductor expression language reference to task output
        outputParameters[`${refName}_result`] = '${' + refName + '.output}';
      }

      // Collect outputs for event waits keyed by upstream service name
      if (node.type === 'eventWait') {
        const incoming = incomingById.get(node.id) || [];
        const upstreamId = incoming.length > 0 ? incoming[0] : undefined;
        let upstreamService: string | undefined;
        if (upstreamId) {
          upstreamService = computedOutputs.get(upstreamId)?.serviceName;
          if (!upstreamService) {
            const upstreamNode = idToNode.get(upstreamId);
            if (upstreamNode) {
              upstreamService = canonicalServiceName(upstreamNode.type as string, upstreamNode);
            }
          }
        }
        if (upstreamService) {
          const pretty = prettyServiceKey(upstreamService);
          outputParameters[`${pretty}_result`] = '${' + task.taskReferenceName + '.output}';
        }
      }

      // Track last publish output key (exclude finalize which doesn't produce a new output file)
      const publishTypes = new Set(['emailValidation', 'phoneValidation', 'enrichment']);
      if (publishTypes.has(node.type as string)) {
        const out = computedOutputs.get(node.id);
        if (out?.key) lastPublishOutputKey = out.key;
      }
    }
  });

  // Build workflow with complete metadata
  const workflow: ConductorWorkflow = {
    name: workflowName,
    description: `Generated workflow with ${tasks.length} tasks`,
    version: 2,
    tasks,
    inputParameters: [],
    outputParameters: {
      pipeline_result: lastPublishOutputKey || '',
      workflow_id: '${workflow.workflowId}',
      workflowId: '${workflow.workflowId}',
      final_file: 'enriched_${workflow.instanceId}.csv',
      ...outputParameters,
    },
    schemaVersion: 2,
    restartable: true,
    workflowStatusListenerEnabled: false,
    // Optional: global timeouts per the sample
    // @ts-ignore
    timeoutPolicy: 'ALERT_ONLY' as any,
    // @ts-ignore
    timeoutSeconds: 3600 as any,
    ownerEmail: 'team@company.com',
  };

  return workflow;
}

// Basic Kahn topological sort with geometric tiebreaker for layout stability
function topologicalSort(
  nodes: Node[],
  adjacencyMap: Map<string, string[]>,
  inDegree: Map<string, number>
): Node[] {
  const queue: Node[] = [];
  const sorted: Node[] = [];
  const nodeMap = new Map(nodes.map((n) => [n.id, n]));

  // Find all nodes with no incoming edges
  nodes.forEach((node) => {
    if (inDegree.get(node.id) === 0) {
      queue.push(node);
    }
  });

  // Sort nodes by position (left to right, top to bottom) as tiebreaker
  queue.sort((a, b) => {
    if (Math.abs(a.position.y - b.position.y) < 50) {
      return a.position.x - b.position.x;
    }
    return a.position.y - b.position.y;
  });

  while (queue.length > 0) {
    const node = queue.shift()!;
    sorted.push(node);

    // Reduce in-degree for neighbors
    const neighbors = adjacencyMap.get(node.id) || [];
    neighbors.forEach((neighborId) => {
      const newDegree = (inDegree.get(neighborId) || 0) - 1;
      inDegree.set(neighborId, newDegree);

      if (newDegree === 0) {
        const neighbor = nodeMap.get(neighborId);
        if (neighbor) queue.push(neighbor);
      }
    });
  }

  // If not all nodes are sorted, there's a cycle (shouldn't happen in DAG)
  if (sorted.length !== nodes.length) {
    console.warn('Cycle detected in workflow graph, some nodes may be missing');
  }

  return sorted;
}

// Create a KAFKA_PUBLISH or WAIT task from a node, deriving names from serviceName
function convertNodeToTaskGeneric(
  node: Node,
  index: number,
  incomingById: Map<string, string[]>,
  computedOutputs: Map<string, { bucket: string; key: string; serviceName: string }>,
  idToNode: Map<string, Node>,
  isFirstService: boolean = false
): ConductorTask | null {
  const publishTypes = new Set(['airflowTrigger', 'emailValidation', 'phoneValidation', 'enrichment', 'finalizePipeline']);

  if (publishTypes.has(node.type as string)) {
    const serviceName = canonicalServiceName(node.type as string, node);
    const kebab = toKebab(serviceName);
    const snake = serviceName; // already snake

    // Determine input bucket/key
    const incoming = incomingById.get(node.id) || [];
    let input_bucket = node.data?.inputBucket || `${kebab}-input`;
    let input_key = node.data?.inputKey || 'input.csv';
    let previousStage: string | undefined = undefined;

    // If this is the first service (excluding airflowTrigger), use raw-data as input
    if (isFirstService && node.type !== 'airflowTrigger') {
      input_bucket = 'raw-data';
      input_key = 'customer_data_sample.csv';
    } else {
      // Find a predecessor with computed output
      for (const srcId of incoming) {
        const prevOut = computedOutputs.get(srcId);
        if (prevOut) {
          input_bucket = prevOut.bucket;
          input_key = prevOut.key;
          previousStage = prevOut.serviceName;
          break;
        }
      }
      // If incoming is an EVENT wait, look one step further to the publish node
      if (!previousStage && incoming.length > 0) {
        for (const srcId of incoming) {
          const upstreamNode = idToNode.get(srcId);
          if (upstreamNode && upstreamNode.type === 'eventWait') {
            const prevIncoming = incomingById.get(srcId) || [];
            const publishId = prevIncoming.length > 0 ? prevIncoming[0] : undefined;
            if (publishId) {
              const publishOut = computedOutputs.get(publishId);
              if (publishOut) {
                input_bucket = publishOut.bucket;
                input_key = publishOut.key;
                previousStage = publishOut.serviceName;
                break;
              }
            }
          }
        }
      }
    }

    // Determine output bucket/key for this service
    // Map service → output bucket names to match convention
    const output_bucket =
      snake === 'email_validation' ? 'email-validated'
      : snake === 'phone_validation' ? 'phone-validated'
      : snake === 'enrichment' ? 'enriched'
      : `${kebab}-output`;
    const output_key = `${snake}_${'${workflow.workflowId}'}.csv`;

    // Save for downstream consumers (except airflowTrigger which doesn't produce files)
    if (node.type !== 'airflowTrigger') {
      computedOutputs.set(node.id, { bucket: output_bucket, key: output_key, serviceName: snake });
    }

    // Special handling for finalize node
    if (node.type === 'finalizePipeline') {
      // Prefer upstream publish output as final_* even when an EVENT wait sits in between
      let finalBucket = input_bucket;
      let finalKey = input_key;
      if (incoming.length > 0) {
        const upstreamId = incoming[0];
        const upstreamNode = idToNode.get(upstreamId);
        if (upstreamNode && upstreamNode.type === 'eventWait') {
          const upstreamIncoming = incomingById.get(upstreamId) || [];
          const publishId = upstreamIncoming.length > 0 ? upstreamIncoming[0] : undefined;
          if (publishId) {
            const publishOut = computedOutputs.get(publishId);
            if (publishOut) {
              finalBucket = publishOut.bucket;
              finalKey = publishOut.key;
            }
          }
        }
      }
      // Override topic/key per sample
      return {
        name: 'finalize_pipeline',
        taskReferenceName: 'finalize_pipeline',
        type: 'KAFKA_PUBLISH',
        inputParameters: {
          kafka_request: {
            topic: 'conductor-events',
            bootStrapServers: 'kafka:9092',
            value: {
              workflowId: '${workflow.workflowId}',
              eventType: 'pipeline_completed',
              data: {
                final_bucket: finalBucket,
                final_key: finalKey,
                pipelineStage: 'completed',
                records: 1000,
              },
            },
            key: `pipeline-complete-${'${workflow.workflowId}'}`,
          },
        },
      };
    }

    // Special handling for airflow trigger overrides
    if (node.type === 'airflowTrigger') {
      return {
        name: 'trigger_airflow_dag',
        taskReferenceName: 'trigger_airflow_dag',
        type: 'KAFKA_PUBLISH',
        inputParameters: {
          kafka_request: {
            topic: 'airflow-trigger-requests',
            bootStrapServers: 'kafka:9092',
            value: {
              workflowId: '${workflow.workflowId}',
              taskId: 'airflow_trigger_task',
              eventType: 'airflow_trigger_request',
              data: {
                dag_id: node.data?.dagId || 'example_dag',
                execution_id: node.data?.executionId || 'execution',
                pipelineStage: 'airflow_processing',
              },
            },
            key: `airflow-trigger-${'${workflow.workflowId}'}`,
          },
        },
      };
    }

    const payloadData: any = {
      input_bucket,
      input_key,
      output_bucket,
      output_key,
      pipelineStage: snake,
    };
    if (previousStage) payloadData.previousStage = previousStage;
    // Include records for sample compatibility
    payloadData.records = 1000;

    const task: ConductorTask = {
      name: snake,
      taskReferenceName: snake,
      type: 'KAFKA_PUBLISH',
      inputParameters: {
        kafka_request: {
          topic: `${kebab}-requests`,
          bootStrapServers: 'kafka:9092',
          value: {
            workflowId: '${workflow.workflowId}',
            taskId: `${snake}_task`,
            eventType: `${snake}_request`,
            data: payloadData,
          },
          key: `${kebab}-${'${workflow.workflowId}'}`,
        },
      },
      // Note: sink is NOT added to KAFKA_PUBLISH tasks
      // The sink property belongs only to EVENT wait tasks, not publish tasks
      // Services publish to Kafka, and EVENT tasks wait for completion events
    };

    return task;
  }

  // EVENT wait nodes: emit EVENT tasks with sink derived from upstream service
  if (node.type === 'eventWait') {
    const incoming = incomingById.get(node.id) || [];
    const upstreamId = incoming.length > 0 ? incoming[0] : undefined;
    let upstreamService: string | undefined;
    if (upstreamId) {
      upstreamService = computedOutputs.get(upstreamId)?.serviceName;
      if (!upstreamService) {
        const upstreamNode = idToNode.get(upstreamId);
        if (upstreamNode) {
          upstreamService = canonicalServiceName(upstreamNode.type as string, upstreamNode);
        }
      }
    }
    const ref = upstreamService === 'airflow_processing'
      ? 'wait_for_airflow_completion'
      : upstreamService
        ? `wait_for_${upstreamService}_completion`
        : sanitizeRefName(node.data?.taskName || `wait_event_${index}`);
    const eventTask: any = {
      name: ref,
      taskReferenceName: ref,
      type: 'EVENT',
      // Conductor EVENT tasks use sink and asyncComplete, no inputParameters required
      sink: upstreamService === 'airflow_processing'
        ? 'conductor:airflow_dag_completed'
        : upstreamService
          ? `conductor:${upstreamService}_completed`
          : undefined,
      asyncComplete: true,
    };
    // Remove sink if still undefined to avoid undefined in UI renderers
    if (eventTask.sink === undefined) delete eventTask.sink;
    return eventTask as ConductorTask;
  }

  if (node.type === 'waitForResult') {
    return {
      name: 'wait_for_result',
      taskReferenceName: sanitizeRefName(node.data.taskName || `wait_${index}`),
      inputParameters: {
        topic: node.data.topic || 'default-results',
        workflowId: '${workflow.workflowId}',
      },
    };
  }

  return null;
}

// Resolve the canonical snake_case service name used across fields
function canonicalServiceName(type: string, node: Node): string {
  // Prefer explicit serviceName on node if provided
  if (node.data?.serviceName) return sanitizeRefName(node.data.serviceName);
  switch (type) {
    case 'emailValidation':
      return 'email_validation';
    case 'phoneValidation':
      return 'phone_validation';
    case 'enrichment':
      return 'enrichment';
    case 'finalizePipeline':
      return 'finalize_pipeline';
    case 'airflowTrigger':
      return 'airflow_processing';
    default:
      return sanitizeRefName(node.data?.label || type);
  }
}

// Utility: convert snake_case to kebab-case
function toKebab(snake: string): string {
  return snake.replace(/_/g, '-');
}

// Utility: pretty key for outputParameters (<service>_result)
function prettyServiceKey(service: string): string {
  if (service === 'airflow_processing') return 'airflow';
  if (service.endsWith('_validation')) return service.replace('_validation', '');
  return service;
}

function sanitizeRefName(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '');
}

/**
 * Validates the generated Conductor workflow
 */
export function validateWorkflow(workflow: ConductorWorkflow): {
  valid: boolean;
  errors: string[];
} {
  const errors: string[] = [];

  if (!workflow.name || workflow.name.trim() === '') {
    errors.push('Workflow name is required');
  }

  if (!workflow.tasks || workflow.tasks.length === 0) {
    errors.push('Workflow must have at least one task');
  }

  // Check for duplicate task reference names
  const refNames = new Set<string>();
  workflow.tasks.forEach((task) => {
    if (refNames.has(task.taskReferenceName)) {
      errors.push(`Duplicate task reference name: ${task.taskReferenceName}`);
    }
    refNames.add(task.taskReferenceName);
  });

  // Validate KAFKA_PUBLISH tasks
  workflow.tasks.forEach((task) => {
    if (task.type === 'KAFKA_PUBLISH') {
      const kafkaReq = task.inputParameters?.kafka_request;
      if (!kafkaReq?.topic) {
        errors.push(`KAFKA_PUBLISH task ${task.taskReferenceName} missing topic`);
      }
      if (!kafkaReq?.bootStrapServers) {
        errors.push(
          `KAFKA_PUBLISH task ${task.taskReferenceName} missing bootStrapServers`
        );
      }
    }
  });

  return {
    valid: errors.length === 0,
    errors,
  };
}

