# Event-Driven Architecture with Conductor and Kafka

## 🎯 Overview

This project demonstrates a **production-ready event-driven architecture** using Conductor as the orchestration engine, Kafka as the event bus, and microservices as event handlers. The architecture eliminates single points of failure by removing the Event Router and enabling direct Conductor-microservice integration.

## 🏗️ Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────────┐
│   Conductor │    │    Kafka    │    │  Microservices  │
│             │    │             │    │                 │
│ KAFKA_PUBLISH│───▶│ Topics:     │───▶│ Email Validator │
│ Tasks with  │    │ - email-    │    │ Phone Validator │
│ Sink Defs   │    │   validation│    │ Enricher        │
│             │    │ - phone-    │    │                 │
│             │◀───│   validation│◀───│                 │
│             │    │ - enrichment│    │                 │
│             │    │ - conductor-│    │                 │
│             │    │   events    │    │                 │
└─────────────┘    └─────────────┘    └─────────────────┘
       │                                        │
       │                                        │
       ▼                                        ▼
┌─────────────┐                        ┌─────────────┐
│   MinIO     │                        │   MinIO     │
│             │                        │             │
│ File Storage│                        │ File Storage│
│ & Processing│                        │ & Processing│
└─────────────┘                        └─────────────┘
```

## ✨ Key Features

- **🚀 Event-Driven**: Asynchronous processing with Kafka events
- **🔄 Direct Integration**: No Event Router (eliminated single point of failure)
- **📁 File-Based Processing**: MinIO for scalable file storage
- **⚡ High Performance**: ~10 second end-to-end processing
- **🛡️ Resilient**: Built-in retry and timeout mechanisms
- **📊 Scalable**: Microservices can scale independently
- **🔍 Observable**: Comprehensive logging and monitoring

## 🏃‍♂️ Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.9+
- Git

### 1. Clone and Setup

```bash
git clone <repository-url>
cd conductor_kafka_poc/event_driven_poc
```

### 2. Start Services

```bash
docker-compose up -d
```

This starts:
- **Zookeeper** (Kafka coordination)
- **Kafka** (Event bus)
- **Conductor Server** (Orchestration engine)
- **MinIO** (File storage)
- **Microservices** (Email Validator, Phone Validator, Enricher)

### 3. Upload Sample Data

```bash
python3 scripts/upload_sample_data.py
```

### 4. Run the Pipeline

```bash
python3 scripts/test_workflow_only.py
```

## 📋 Services

### Core Services

| Service | Port | Purpose |
|---------|------|---------|
| Conductor Server | 8080 | Workflow orchestration |
| Conductor UI | 8080 | Web interface |
| Kafka | 9092 | Event bus |
| MinIO | 9000 | File storage |
| MinIO Console | 9001 | File management UI |

### Microservices

| Service | Purpose | Input Topic | Output Topic |
|---------|---------|-------------|--------------|
| Email Validator | Validates email addresses | `email-validation-requests` | `conductor-events` |
| Phone Validator | Validates phone numbers | `phone-validation-requests` | `conductor-events` |
| Enricher | Enriches data with additional info | `enrichment-requests` | `conductor-events` |

## 🔄 Data Flow

### 1. Workflow Execution
```
Conductor starts workflow
    ↓
KAFKA_PUBLISH task sends event to microservice topic
    ↓
Microservice processes file from MinIO
    ↓
Microservice publishes result to conductor-events
    ↓
Conductor receives completion event
    ↓
Next task in workflow executes
```

### 2. File Processing Pipeline
```
raw-data/customer_data_sample.csv
    ↓ (Email Validator)
email-validated/email_validated_{workflowId}.csv
    ↓ (Phone Validator)
phone-validated/phone_validated_{workflowId}.csv
    ↓ (Enricher)
enriched/enriched_{workflowId}.csv
```

## 📁 Project Structure

```
event_driven_poc/
├── docker-compose.yaml              # Service definitions
├── workflows/
│   └── direct_integration_workflow.json  # Main workflow
├── microservices/
│   ├── email_validator/
│   │   ├── service.py              # Email validation logic
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── phone_validator/
│   │   ├── service.py              # Phone validation logic
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── enricher/
│       ├── service.py              # Data enrichment logic
│       ├── Dockerfile
│       └── requirements.txt
├── scripts/
│   ├── test_workflow_only.py       # Test the pipeline
│   └── upload_sample_data.py       # Upload sample data
└── README.md                       # This file
```

## 🛠️ Configuration

### Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `KAFKA_BROKER_URL` | `kafka:9092` | Kafka broker address |
| `MINIO_ENDPOINT` | `minio:9000` | MinIO server address |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |

### Workflow Configuration

The main workflow is defined in `workflows/direct_integration_workflow.json`:

```json
{
  "name": "direct_integration_workflow",
  "version": 2,
  "tasks": [
    {
      "name": "start_pipeline",
      "type": "KAFKA_PUBLISH",
      "inputParameters": {
        "kafka_request": {
          "topic": "email-validation-requests",
          "value": { ... }
        }
      },
      "sink": "conductor:email_validation_completed"
    }
  ]
}
```

## 🧪 Testing

### Run Complete Pipeline

```bash
python3 scripts/test_workflow_only.py
```

Expected output:
```
✅ Workflow triggered successfully with ID: abc123-def456
📊 Workflow Status: COMPLETED
  - start_pipeline: COMPLETED
  - trigger_phone_validation: COMPLETED
  - trigger_enrichment: COMPLETED
  - finalize_pipeline: COMPLETED
```

### Check Generated Files

```bash
# Access MinIO Console at http://localhost:9001
# Login: minioadmin / minioadmin
# Check buckets: email-validated, phone-validated, enriched
```

### Monitor Logs

```bash
# View microservice logs
docker logs email-validator-event
docker logs phone-validator-event
docker logs enricher-event

# View Conductor logs
docker logs conductor-event
```

## 🔧 Troubleshooting

### Common Issues

1. **Services not starting**
   ```bash
   docker-compose down --volumes
   docker-compose up -d
   ```

2. **Workflow fails with timeout**
   - Check microservice logs
   - Verify Kafka topics exist
   - Ensure MinIO is accessible

3. **Files not appearing in MinIO**
   - Check microservice logs for errors
   - Verify MinIO credentials
   - Check bucket permissions

### Health Checks

```bash
# Check Conductor
curl http://localhost:8080/api/health

# Check Kafka
docker exec kafka-event kafka-topics --bootstrap-server localhost:9092 --list

# Check MinIO
curl http://localhost:9000/minio/health/live
```

## 📊 Performance Metrics

- **End-to-End Processing**: ~10 seconds
- **Throughput**: 1000 records processed
- **Success Rate**: 885/1000 emails valid (88.5%)
- **File Sizes**: 
  - Input: 1000 records
  - Email Validated: 885 records (63KB)
  - Phone Validated: 699 records (49KB)
  - Enriched: 699 records (72KB)

## 🚀 Production Considerations

### Scalability
- Microservices can scale horizontally
- Kafka partitions can be increased
- MinIO can be clustered

### Monitoring
- Add Prometheus metrics
- Implement health checks
- Set up alerting

### Security
- Use proper authentication
- Encrypt data in transit
- Secure MinIO access

### Backup
- Backup MinIO data
- Backup Conductor workflows
- Backup Kafka topics

## 🔄 Migration from Event Router

This architecture eliminates the Event Router by:

1. **Direct Kafka Publishing**: Microservices publish directly to `conductor-events`
2. **Native Sink Support**: KAFKA_PUBLISH tasks include sink definitions
3. **Simplified Flow**: Fewer components, less complexity
4. **Better Performance**: No intermediate routing

### Before (with Event Router)
```
Conductor → Event Router → Kafka → Microservices
         ← Event Router ← Kafka ← Microservices
```

### After (Direct Integration)
```
Conductor → Kafka → Microservices
         ← Kafka ← Microservices
```

## 📚 API Reference

### Conductor API

- **Start Workflow**: `POST /api/workflow/{workflowName}`
- **Get Workflow**: `GET /api/workflow/{workflowId}`
- **Get Task**: `GET /api/task/{taskId}`

### MinIO API

- **Upload File**: `PUT /{bucket}/{object}`
- **Download File**: `GET /{bucket}/{object}`
- **List Objects**: `GET /{bucket}?list-type=2`

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For questions or issues:
1. Check the troubleshooting section
2. Review the logs
3. Create an issue in the repository

---

**Built with ❤️ using Conductor, Kafka, and MinIO**