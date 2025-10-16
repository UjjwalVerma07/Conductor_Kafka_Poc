# Simplified Visible Tasks Implementation

## 📝 Overview

This is a simplified version that makes microservice processing visible in Conductor UI by updating task status **only when processing completes** (no progress tracking).

---

## 🎯 What Changed

### **Before (Current):**
```
Conductor UI Shows:
├─ publish_file_request       ✅ COMPLETED
└─ wait_for_file_result        ✅ COMPLETED
    ⚠️ No visibility into actual processing
```

### **After (With Visible Tasks):**
```
Conductor UI Shows:
├─ publish_file_request        ✅ COMPLETED
├─ email_validation            ✅ COMPLETED (shows stats!)
│   └─ Stats: {valid: 24, invalid: 6, total: 30}
├─ publish_file_request        ✅ COMPLETED
├─ phone_validation            ✅ COMPLETED (shows stats!)
│   └─ Stats: {valid: 24, invalid: 6, total: 30}
├─ publish_file_request        ✅ COMPLETED
└─ enrichment                  ✅ COMPLETED (shows stats!)
    └─ Stats: {premium: 20, standard: 8, basic: 2}
```

---

## 📦 Files Created

### **1. Workflow Files**
- `workflows/task_definitions_v2.json` - Adds 3 new task types
- `workflows/multi_stage_pipeline_v2.json` - New workflow with visible tasks

### **2. Worker**
- `worker/processing_task_worker.py` - Handles processing tasks

### **3. Enhanced Services** (Updates Conductor when done)
- `services/email_validator/service_v2.py`
- `services/phone_validator/service_v2.py`
- `services/enricher/service_v2.py`

---

## 🔄 How It Works

### **Simple Flow:**

1. **Workflow creates processing task** (e.g., `email_validation`)
2. **Processing Task Worker** marks it as `IN_PROGRESS`
3. **Pipeline Worker** publishes to Kafka
4. **Email Service:**
   - Consumes Kafka message
   - Downloads from MinIO
   - Validates emails
   - Uploads to MinIO
   - **Calls Conductor HTTP API** to update task to `COMPLETED` ✅
5. **Conductor** marks task as done and advances workflow

---

## 🚀 How to Deploy

### **Step 1: Update docker-compose.yaml**

Add environment variable to services:

```yaml
  email-validator:
    build: ./services/email_validator
    environment:
      - CONDUCTOR_SERVER_URL=http://conductor-server:8080/api  # ADD THIS
      - KAFKA_BOOTSTRAP=kafka:9092
      - MINIO_ENDPOINT=minio:9000
      # ... rest of config

  phone-validator:
    build: ./services/phone_validator
    environment:
      - CONDUCTOR_SERVER_URL=http://conductor-server:8080/api  # ADD THIS
      - KAFKA_BOOTSTRAP=kafka:9092
      - MINIO_ENDPOINT=minio:9000
      # ... rest of config

  enricher:
    build: ./services/enricher
    environment:
      - CONDUCTOR_SERVER_URL=http://conductor-server:8080/api  # ADD THIS
      - KAFKA_BOOTSTRAP=kafka:9092
      - MINIO_ENDPOINT=minio:9000
      # ... rest of config
```

Add processing task worker:

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

### **Step 2: Update Service Dockerfiles**

For each service, update the Dockerfile to use service_v2.py:

```dockerfile
# services/email_validator/Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install requests  # ADD THIS for HTTP calls

COPY service_v2.py service.py  # Use v2 version
COPY validator.py .

CMD ["python", "-u", "service.py"]
```

**Do this for:** 
- `services/email_validator/Dockerfile`
- `services/phone_validator/Dockerfile`
- `services/enricher/Dockerfile`

### **Step 3: Add requests to requirements.txt**

For each service, add to `requirements.txt`:

```txt
kafka-python==2.0.2
minio==7.2.3
requests==2.31.0    # ADD THIS
```

### **Step 4: Add processing_task_worker.py to worker Dockerfile**

Update `worker/Dockerfile` to support multiple commands:

```dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy both workers
COPY worker.py .
COPY processing_task_worker.py .
COPY minio_client.py .

# Default command (can be overridden in docker-compose)
CMD ["python", "-u", "worker.py"]
```

### **Step 5: Register V2 Workflow**

Create script to register v2:

```python
# scripts/register_workflow_v2.py
import requests
import json

CONDUCTOR_API = 'http://localhost:8080/api'

# Register task definitions
with open('../workflows/task_definitions_v2.json') as f:
    tasks = json.load(f)
    requests.post(f'{CONDUCTOR_API}/metadata/taskdefs', json=tasks)

# Register workflow
with open('../workflows/multi_stage_pipeline_v2.json') as f:
    workflow = json.load(f)
    requests.post(f'{CONDUCTOR_API}/metadata/workflow', json=workflow)

print("✅ Registered v2 workflow!")
```

### **Step 6: Rebuild and Deploy**

```bash
cd full_implementation

# Stop current services
docker-compose down

# Rebuild with changes
docker-compose up --build -d

# Wait for services to start
sleep 30

# Register v2 workflow
cd scripts
python3 register_workflow_v2.py

# Trigger v2 workflow
python3 trigger_workflow_v2.py  # Will need to create this
```

---

## 📊 What You'll See in Conductor UI

### **Task Details for email_validation:**

```json
{
  "status": "COMPLETED",
  "outputData": {
    "status": "success",
    "output_bucket": "intermediate-data",
    "output_key": "stage1-email-validated.json",
    "records_processed": 30,
    "processing_stats": {
      "total_records": 30,
      "valid_emails": 24,
      "invalid_emails": 6,
      "duplicate_emails": 0
    }
  }
}
```

You'll see:
- ✅ Task name: `email_validation`
- ✅ Status: `COMPLETED`
- ✅ Processing stats visible in UI
- ✅ Can click to see full output data

---

## 🎯 Benefits

| Feature | Before | After |
|---------|--------|-------|
| **See Processing Tasks** | ❌ No | ✅ Yes |
| **See Stats in UI** | ❌ No | ✅ Yes |
| **Track Failures** | Limited | Detailed |
| **Monitor Progress** | No | Yes (completion only) |
| **Debugging** | Check logs | See in UI |

---

## 🔧 How Services Update Conductor

Each service calls this function when done:

```python
def update_conductor_task(workflow_id, task_ref_name, status, output_data):
    # 1. Get workflow
    workflow = requests.get(f"{CONDUCTOR_API}/workflow/{workflow_id}").json()
    
    # 2. Find task by reference name
    task_id = find_task_id(workflow, task_ref_name)
    
    # 3. Update task
    requests.post(f"{CONDUCTOR_API}/tasks", json={
        "workflowInstanceId": workflow_id,
        "taskId": task_id,
        "status": "COMPLETED",  # or FAILED
        "outputData": {
            "status": "success",
            "processing_stats": {...},
            "records_processed": 30
        }
    })
```

**That's it!** Simple and clean.

---

## 🐛 Troubleshooting

### **Task stays IN_PROGRESS forever**
- Check service logs: `docker logs email-validator-service`
- Verify CONDUCTOR_SERVER_URL is set correctly
- Check if service can reach Conductor: `docker exec email-validator-service curl http://conductor-server:8080/health`

### **Task not found error**
- Verify task reference names match:
  - `email_validation_processing`
  - `phone_validation_processing`
  - `enrichment_processing`

### **HTTP 404 on update**
- Make sure processing-task-worker is running
- Check that it picked up the task first

---

## 🎉 Summary

This implementation gives you **visible processing tasks in Conductor UI** with minimal overhead:

- ✅ Only 1 HTTP call per service (at completion)
- ✅ No progress tracking (simpler)
- ✅ Full stats visible in UI
- ✅ Better debugging and monitoring
- ✅ Maintains backward compatibility (still publishes to Kafka)

The workflow now shows **exactly what's happening** instead of hiding it behind external services! 🚀

