# Conductor Kafka POC - Simple 2-Stage Flow

A proof of concept demonstrating Kafka-based microservice orchestration using Netflix Conductor with a worker-based approach.

## 🎯 What This Does

This POC demonstrates a **simple 2-stage workflow** where:

1. **Client** triggers a Conductor workflow
2. **Worker** publishes a message to Kafka topic `stage1-requests`
3. **Service 1 (email-validator)** consumes from Kafka, processes data, publishes result to `stage1-results`
4. **Worker** waits for result from `stage1-results`, then publishes to `stage2-requests`
5. **Service 2 (phone-validator)** consumes from Kafka, processes data, publishes result to `stage2-results`
6. **Worker** waits for result from `stage2-results` and completes workflow
7. **Final result** is visible in Conductor UI with all task outputs

## 📊 Flow Diagram

```
Client
  │
  ▼
Conductor Workflow Started
  │
  ├─> [Task: publish_to_kafka] ──> Kafka: stage1-requests
  │                                      │
  │                                      ▼
  │                                Service 1: email-validator
  │                                      │
  │                                      ▼
  ├─> [Task: wait_for_result] <── Kafka: stage1-results
  │
  ├─> [Task: publish_to_kafka] ──> Kafka: stage2-requests
  │                                      │
  │                                      ▼
  │                                Service 2: phone-validator
  │                                      │
  │                                      ▼
  └─> [Task: wait_for_result] <── Kafka: stage2-results
        │
        ▼
  Workflow Completed ✓
```

## 🏗️ Architecture

### Components

1. **Conductor Server** - Workflow orchestration engine
2. **Worker** - Python worker handling two task types:
   - `publish_to_kafka` - Publishes messages to Kafka
   - `wait_for_result` - Waits for results from Kafka (polls and matches by workflowId)
3. **Microservices** - Two Kafka consumers:
   - Service 1: Email validator (stage1-requests → stage1-results)
   - Service 2: Phone validator (stage2-requests → stage2-results)
4. **Kafka + Zookeeper** - Message broker
5. **MinIO** - S3-compatible storage (ready for future file processing)

### Why Worker-Based Approach?

✅ **Works with ANY queue** (Kafka, Pulsar, RabbitMQ, SQS, etc.)  
✅ **No special Conductor configuration needed**  
✅ **Full control** over message matching logic  
✅ **Better error handling** - can retry, log, alert  
✅ **Easy to test** - standard Kafka producers/consumers  
✅ **Results visible in Conductor UI** - complete workflow visibility

## 🚀 Quick Start

### 1. Start All Services

```bash
docker-compose up --build
```

This will start:
- Zookeeper (port 2181)
- Kafka (port 9092)
- MinIO (port 9000, console 9001)
- Conductor Server (port 8080, UI port 5000)
- Worker (Conductor task handler)
- Microservices (Kafka consumers)

### 2. Wait for Services to be Ready

Wait ~60 seconds for all services to start and be healthy. You can check:

```bash
# Check Conductor health
curl http://localhost:8080/api/health

# Check Kafka
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

### 3. Run the Test

```bash
./test_simple_flow.sh
```

This will:
- Register task definitions (`publish_to_kafka`, `wait_for_result`)
- Register workflow definition (`simple_two_stage_flow`)
- Start a workflow with test data
- Monitor execution and show results

### 4. View in Conductor UI

Open your browser:
- **Conductor UI**: http://localhost:5000
- Navigate to "Workflow Executions" to see your workflow
- Click on the workflow ID to see task details and outputs

## 📁 Project Structure

```
.
├── docker-compose.yaml           # All services configuration
├── conductor-custom/
│   └── Dockerfile               # Conductor with Kafka support
├── worker/
│   ├── Dockerfile               # Worker container
│   ├── requirements.txt         # Python dependencies
│   └── simple_worker.py         # Worker implementation
├── microservices.py             # Service 1 & 2 implementations
├── workflows/
│   ├── task_definitions.json    # Task definitions
│   └── simple_two_stage.json    # Workflow definition
├── test_simple_flow.sh          # Test script
├── WORKER_FLOW.md               # Full architecture documentation
└── README.md                    # This file
```

## 🔍 How It Works

### Task Definitions

Two SIMPLE tasks that workers execute:

1. **publish_to_kafka**
   ```json
   {
     "topic": "stage1-requests",
     "data": {"input": "some data"}
   }
   ```
   - Worker publishes to Kafka topic
   - Includes workflowId and taskId in message
   - Completes immediately

2. **wait_for_result**
   ```json
   {
     "topic": "stage1-results",
     "timeout": 60
   }
   ```
   - Worker polls Kafka topic
   - Matches messages by workflowId
   - Returns IN_PROGRESS if not found (Conductor retries)
   - Returns COMPLETED when match found

### Workflow Definition

The workflow chains these tasks:
```
publish_to_kafka (stage1) → wait_for_result (stage1) 
  → publish_to_kafka (stage2) → wait_for_result (stage2)
```

Each stage passes data to the next using Conductor's parameter passing:
```json
"${wait_stage1_result_ref.output.result}"
```

### Microservices

Two independent Kafka consumers that:
1. Listen to their respective request topics
2. Filter by stage (optional for multi-stage on same topic)
3. Process data (currently simulated)
4. Publish results to result topics with workflowId/taskId

## 🧪 Testing

### Manual Testing

1. **Register tasks and workflow** (one-time):
   ```bash
   # Register tasks
   curl -X POST http://localhost:8080/api/metadata/taskdefs \
     -H "Content-Type: application/json" \
     -d @workflows/task_definitions.json

   # Register workflow
   curl -X POST http://localhost:8080/api/metadata/workflow \
     -H "Content-Type: application/json" \
     -d @workflows/simple_two_stage.json
   ```

2. **Start a workflow**:
   ```bash
   curl -X POST http://localhost:8080/api/workflow/simple_two_stage_flow \
     -H "Content-Type: application/json" \
     -d '{"input_data": "Test data"}'
   ```

3. **Check workflow status**:
   ```bash
   curl http://localhost:8080/api/workflow/{workflowId}
   ```

### Check Kafka Topics

```bash
# List topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Consume from a topic
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic stage1-requests \
  --from-beginning
```

### Check Logs

```bash
# Worker logs
docker logs -f conductor-worker

# Microservices logs
docker logs -f microservices

# Conductor logs
docker logs -f conductor-server
```

## 🎓 Key Concepts

### Worker-Based vs Event-Based

**This POC uses Worker-Based approach:**
- Workers handle both publishing and waiting
- Standard SIMPLE tasks
- Works with any message queue
- Full control over message matching

**NOT using Event-Based approach:**
- Would require Conductor EVENT tasks
- Needs special Conductor-Kafka integration
- Less flexible for complex matching logic

### Message Format

All Kafka messages include:
```json
{
  "workflowId": "abc-123",
  "taskId": "task-456",
  "data": { ... },
  "result": { ... },
  "status": "success"
}
```

This allows workers to match results to specific workflow instances.

## 🔧 Configuration

### Environment Variables

- `CONDUCTOR_SERVER_URL` - Conductor API URL (default: http://localhost:8080/api)
- `KAFKA_BOOTSTRAP` - Kafka bootstrap servers (default: localhost:9092)

### Adjusting Timeouts

Edit `workflows/task_definitions.json`:
- `timeoutSeconds` - Task timeout
- `retryCount` - Number of retries
- `retryDelaySeconds` - Delay between retries

For wait tasks, increase `retryCount` for longer polling periods.

## 📈 Next Steps

To expand this to the full multi-stage pipeline with MinIO:

1. **Add MinIO Integration**
   - Workers download/upload files from/to MinIO
   - Pass bucket and key information in messages

2. **Add More Stages**
   - Replicate the pattern for stage 3, 4, etc.
   - Each stage: publish → wait → process → publish result

3. **Add Real Processing**
   - Replace simulated processing with actual logic
   - Email validation, phone validation, enrichment, etc.

4. **Add Error Handling**
   - Handle service failures
   - Implement retry logic in microservices
   - Add dead letter queues

5. **Add Monitoring**
   - Metrics collection
   - Alerting on failures
   - Performance tracking

## 📚 Resources

- [Netflix Conductor Docs](https://conductor.netflix.com/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [WORKER_FLOW.md](./WORKER_FLOW.md) - Detailed architecture documentation

## 🐛 Troubleshooting

### Services won't start
- Check Docker is running
- Check ports 2181, 9092, 8080, 5000, 9000, 9001 are available
- Try `docker-compose down -v` to clean volumes

### Workflow stays in RUNNING
- Check worker logs: `docker logs conductor-worker`
- Check microservices logs: `docker logs microservices`
- Verify Kafka topics exist and have messages

### Worker can't connect to Conductor
- Verify Conductor is healthy: `curl http://localhost:8080/api/health`
- Check worker environment variables in docker-compose.yaml

### No messages in Kafka
- Check Kafka is running: `docker ps | grep kafka`
- Check worker published successfully: `docker logs conductor-worker`
- Use Kafka console consumer to verify messages

## 📝 License

MIT License - feel free to use this as a starting point for your projects!

