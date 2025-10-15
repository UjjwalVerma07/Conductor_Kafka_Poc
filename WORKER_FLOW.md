# Worker-Based Approach Flow

## Your Exact Use Case

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        CLIENT TRIGGERS WORKFLOW                          │
│  POST /api/workflow/multi_stage_pipeline                                │
│  Body: { "input_bucket": "my-bucket", "input_key": "data.csv" }        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    STAGE 1: EMAIL VALIDATION                             │
└─────────────────────────────────────────────────────────────────────────┘

Step 1: Conductor Task "publish_file_request"
┌──────────────────┐
│ Conductor Worker │ → Publishes to Kafka
│ (Pipeline Worker)│    Topic: "file-requests"
└──────────────────┘    Message: {workflowId, taskId, stage:1, service:"email-validator"}
         │
         ▼
┌────────────────────┐
│  Kafka Topic:      │
│  "file-requests"   │
└────────────────────┘
         │
         ▼
┌──────────────────────┐
│ Email Validator      │ ← Kafka Consumer (external microservice)
│ (Microservice)       │   - Filters: stage=1
│                      │   - Downloads from MinIO
│ - Validates emails   │   - Processes data
│ - Uploads to MinIO   │   - Uploads result
└──────────────────────┘
         │
         │ Publishes result
         ▼
┌────────────────────────┐
│  Kafka Topic:          │
│  "file-results-        │
│   service1"            │
└────────────────────────┘
         │
         ▼
┌──────────────────┐
│ Conductor Worker │ ← Task: "wait_for_email_validation"
│ (Pipeline Worker)│   - Polls Kafka
│                  │   - Matches workflowId + taskId
│ ✓ Completes Task │   - Returns result to Conductor
└──────────────────┘

         │ Workflow Advances!
         ▼

┌─────────────────────────────────────────────────────────────────────────┐
│                    STAGE 2: PHONE VALIDATION                             │
└─────────────────────────────────────────────────────────────────────────┘

[Same pattern repeats for phone-validator]
- publish_file_request (stage:2)
- phone-validator consumes
- Publishes to "file-results-service2"
- wait_for_phone_validation completes

         │ Workflow Advances!
         ▼

┌─────────────────────────────────────────────────────────────────────────┐
│                    STAGE 3: ENRICHMENT                                   │
└─────────────────────────────────────────────────────────────────────────┘

[Same pattern repeats for enricher]
- publish_file_request (stage:3)
- enricher consumes
- Publishes to "file-results-service3"
- wait_for_enrichment completes

         │ Workflow Completes!
         ▼

┌─────────────────────────────────────────────────────────────────────────┐
│                    WORKFLOW COMPLETED                                    │
│  Output: {                                                               │
│    "stage1_result": {output_key: "intermediate/email-validated.json"},  │
│    "stage2_result": {output_key: "intermediate/phone-validated.json"},  │
│    "final_output_key": "output/final-enriched.json"                     │
│  }                                                                       │
└─────────────────────────────────────────────────────────────────────────┘
```

## Key Components

### 1. Pipeline Worker (`pipeline_worker.py`)
**Handles TWO types of tasks:**

#### A. `publish_file_request` Worker
- Publishes messages to Kafka topic "file-requests"
- Used for ALL stages (1, 2, 3)
- Completes immediately after publishing

#### B. `wait_for_*` Workers
- `wait_for_email_validation` → polls "file-results-service1"
- `wait_for_phone_validation` → polls "file-results-service2"
- `wait_for_enrichment` → polls "file-results-service3"
- **KEY**: Polls Kafka and matches workflowId + taskId
- Returns IN_PROGRESS if no match (Conductor retries)
- Returns COMPLETED when match found

### 2. External Microservices (`mock_pipeline_services.py`)
**Three Kafka consumers:**
- **email-validator**: Consumes from "file-requests" (stage=1)
- **phone-validator**: Consumes from "file-requests" (stage=2)
- **enricher**: Consumes from "file-requests" (stage=3)

**Each service:**
1. Filters messages by stage
2. Downloads file from MinIO
3. Processes data
4. Uploads result to MinIO
5. **Publishes result to Kafka** (with workflowId + taskId)

## How It Works

### Workflow Task → Kafka → External Service → Kafka → Workflow Advances

```python
# Worker publishes to Kafka
publish_worker.execute() → kafka.send("file-requests", {
    workflowId: "abc-123",
    taskId: "task-456", 
    stage: 1
})

# External service processes
email_validator.process() → kafka.send("file-results-service1", {
    workflowInstanceId: "abc-123",
    taskId: "task-456",
    status: "success",
    output_key: "intermediate/email-validated.json"
})

# Worker polls and completes task
wait_worker.execute() → 
    polls kafka.consume("file-results-service1")
    → finds matching message
    → returns COMPLETED with output
    → Conductor advances workflow
```

## Advantages

1. ✅ **Works with ANY queue** (Kafka, Pulsar, RabbitMQ, SQS, etc.)
2. ✅ **No special Conductor configuration needed**
3. ✅ **Full control** over message matching logic
4. ✅ **Better error handling** - can retry, log, alert
5. ✅ **Flexible routing** - can route to different topics based on conditions
6. ✅ **Easy to test** - standard Kafka producers/consumers

## Testing

Run the complete pipeline:
```bash
./test_pipeline.sh
```

This will:
1. Register workflow and tasks
2. Start pipeline worker
3. Start mock external services
4. Trigger workflow with test data
5. Show all 3 stages completing
6. Display final results

## File Structure

```
workflows/multi_stage_pipeline.json   ← Workflow definition
worker/pipeline_worker.py             ← Conductor workers
mock_pipeline_services.py             ← External microservices
test_pipeline.sh                      ← Test script
```

