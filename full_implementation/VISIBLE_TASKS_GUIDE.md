# Making Microservice Processing Visible in Conductor UI

## 🎯 Problem

Currently, your workflow shows only 2 types of tasks in Conductor UI:
- `publish_file_request` - Publishes to Kafka
- `wait_for_file_result` - Waits for result

The actual processing (email validation, phone validation, enrichment) happens invisibly in external microservices.

## ✨ Solution

Add dedicated tasks for each processing stage that external microservices update via HTTP API.

---

## 📊 Architecture Comparison

### **Current Architecture (Tasks Not Visible):**
```
Conductor UI Shows:
┌────────────────────┐
│ publish_request    │ ✅ Visible
└────────────────────┘
         ↓
    [Kafka Topic]
         ↓
┌────────────────────┐
│ Email Service      │ ❌ NOT visible in Conductor
│ (external)         │
└────────────────────┘
         ↓
    [Kafka Topic]
         ↓
┌────────────────────┐
│ wait_for_result    │ ✅ Visible
└────────────────────┘
```

### **New Architecture (All Tasks Visible):**
```
Conductor UI Shows:
┌────────────────────┐
│ publish_request    │ ✅ Visible
└────────────────────┘
         ↓
┌────────────────────┐
│ email_validation   │ ✅ Visible (updated by external service via HTTP)
│ Status: IN_PROGRESS│
│ Progress: 30%      │
└────────────────────┘
         ↓ (HTTP callback updates task)
    [Kafka] → Email Service processes → [HTTP Update to Conductor]
         ↓
┌────────────────────┐
│ email_validation   │ ✅ Visible
│ Status: COMPLETED  │
│ Progress: 100%     │
└────────────────────┘
```

---

## 🔧 What I Created

### 1. **Enhanced Task Definitions** (`task_definitions_v2.json`)

Added 3 new task types:
- `email_validation` - Visible email processing task
- `phone_validation` - Visible phone processing task
- `enrichment` - Visible enrichment processing task

These tasks:
- Show up in Conductor UI
- Display progress and stats
- Are updated by external services via HTTP

### 2. **New Workflow** (`multi_stage_pipeline_v2.json`)

```json
Workflow now has 6 tasks (instead of 6):
1. publish_file_request (stage 1)
2. email_validation        ← NEW! Visible in UI
3. publish_file_request (stage 2)
4. phone_validation        ← NEW! Visible in UI
5. publish_file_request (stage 3)
6. enrichment              ← NEW! Visible in UI
```

**Key change:** Removed `wait_for_file_result` tasks. The processing tasks themselves handle the waiting.

### 3. **Processing Task Worker** (`processing_task_worker.py`)

A new worker that:
- Polls for `email_validation`, `phone_validation`, `enrichment` tasks
- Marks them as `IN_PROGRESS` immediately
- External microservices then update these tasks via HTTP API

### 4. **Enhanced Microservice** (`service_v2.py`)

Modified email validator that:
- Still consumes from Kafka
- **NEW:** Updates Conductor task status via HTTP at key points:
  - When processing starts (IN_PROGRESS, 0%)
  - During processing (IN_PROGRESS, 30%, 70%)
  - When complete (COMPLETED, 100%)
  - On error (FAILED)

---

## 🎨 How It Works

### **Flow for Email Validation Stage:**

```
1. Workflow starts
   └─ Conductor creates "email_validation" task

2. Processing Task Worker polls and picks up task
   └─ Marks it as IN_PROGRESS
   └─ Task now visible in Conductor UI ✅

3. Pipeline Worker publishes to Kafka
   └─ Message includes workflowId for tracking

4. Email Validator Service:
   ├─ Consumes Kafka message
   ├─ Extracts workflowId
   ├─ Calls Conductor API to update task:
   │   GET /api/workflow/{workflowId}  (find task by reference name)
   │   POST /api/tasks (update task status)
   │
   ├─ Updates: "Started processing" (0%)
   ├─ Downloads from MinIO
   ├─ Updates: "Validating" (30%)
   ├─ Processes data
   ├─ Updates: "Uploading" (70%)
   ├─ Uploads to MinIO
   └─ Updates: "Completed" (100%) ✅

5. Conductor marks task as COMPLETED
   └─ Workflow advances to next stage
```

---

## 🚀 How to Use This

### **Option 1: Keep Current Architecture (Simple)**
Continue using your current setup. It works fine, just doesn't show processing details.

### **Option 2: Implement Visible Tasks (Recommended)**

#### **Step 1: Register New Tasks and Workflow**
```bash
cd scripts

# Register the new tasks
python3 register_workflow_v2.py  # You'll need to create this or modify existing one
```

#### **Step 2: Deploy Processing Task Worker**

Add to `docker-compose.yaml`:
```yaml
  processing-task-worker:
    build: ./worker
    container_name: processing-task-worker
    command: python -u processing_task_worker.py
    depends_on:
      conductor-server:
        condition: service_healthy
    environment:
      - CONDUCTOR_SERVER_URL=http://conductor-server:8080/api
    networks:
      - pipeline-net
    restart: unless-stopped
```

#### **Step 3: Update Microservices**

Replace `service.py` with `service_v2.py` in each microservice:
```dockerfile
# In services/email_validator/Dockerfile
COPY service_v2.py service.py  # Use the enhanced version
```

Or create versions for phone and enricher following the same pattern.

#### **Step 4: Add Conductor API URL to Services**

In `docker-compose.yaml`, add to each service:
```yaml
  email-validator:
    environment:
      - CONDUCTOR_SERVER_URL=http://conductor-server:8080/api  # NEW
      - KAFKA_BOOTSTRAP=kafka:9092
      - MINIO_ENDPOINT=minio:9000
      ...
```

---

## 📈 Benefits

### **What You Get:**

1. **👀 Full Visibility**
   - See exactly which stage is processing
   - View progress percentage
   - See processing statistics in real-time

2. **🐛 Better Debugging**
   - Know exactly where failures occur
   - See detailed error messages
   - Track processing time per stage

3. **📊 Better Monitoring**
   - Monitor task duration
   - Set alerts on task failures
   - Track throughput per stage

4. **🎯 Better UX**
   - Users see processing status
   - Progress indicators work
   - Clear failure points

---

## 🔍 Conductor UI View

### **Current View:**
```
Workflow: multi_stage_pipeline
├─ publish_email_validation_ref      [COMPLETED] 2s
└─ wait_email_validation_ref         [COMPLETED] 45s
    ⚠️ No visibility into what happened during those 45 seconds
```

### **New View:**
```
Workflow: multi_stage_pipeline_visible
├─ publish_email_validation_ref      [COMPLETED] 2s
├─ email_validation_processing       [COMPLETED] 43s
│   ├─ Progress: 100%
│   ├─ Records: 30
│   └─ Stats: {valid: 24, invalid: 6}
└─ publish_phone_validation_ref      [IN_PROGRESS]
    ✅ Full transparency!
```

---

## 🛠️ Implementation Details

### **HTTP API Calls from Microservices:**

```python
# 1. Get workflow to find task ID
GET http://conductor:8080/api/workflow/{workflowId}?includeTasks=true

# 2. Find task by referenceTaskName
task_id = find_task_by_reference('email_validation_processing')

# 3. Update task status
POST http://conductor:8080/api/tasks
{
  "workflowInstanceId": "workflow-id",
  "taskId": "task-id",
  "status": "COMPLETED",  # or IN_PROGRESS, FAILED
  "outputData": {
    "status": "success",
    "processing_stats": {...},
    "progress_percentage": 100
  }
}
```

---

## 💡 Customization

You can add more fields to track:
- `estimated_time_remaining`
- `records_validated_per_second`
- `current_batch_number`
- `error_count`
- `warning_count`

Just add them to the task's `outputData` when updating via HTTP.

---

## 🎓 Summary

| Aspect | Current | With Visible Tasks |
|--------|---------|-------------------|
| **Conductor UI** | Shows publish & wait | Shows all processing |
| **Progress Tracking** | No | Yes (percentage) |
| **Error Visibility** | Limited | Detailed |
| **Stats in UI** | No | Yes (real-time) |
| **Debugging** | Check logs | See in UI |
| **Complexity** | Simple | Moderate |

---

## 📝 Next Steps

1. Review the code I provided
2. Decide if you want this level of visibility
3. If yes, I can help you:
   - Create phone_validation and enrichment service_v2 files
   - Update docker-compose.yaml
   - Create registration script for v2 workflow
   - Test the new implementation

Let me know which direction you'd like to go! 🚀

