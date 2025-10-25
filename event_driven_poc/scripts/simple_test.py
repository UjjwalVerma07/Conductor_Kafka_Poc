#!/usr/bin/env python3
"""
Simple Test for Event-Driven Architecture
"""

import json
import time
from kafka import KafkaProducer

# Use localhost explicitly
KAFKA_BOOTSTRAP = 'localhost:9092'

print(f"Connecting to Kafka at: {KAFKA_BOOTSTRAP}")

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    request_timeout_ms=10000,
    retries=3
)

def send_test_event(task_type, workflow_id, task_id):
    """Send a test event to trigger microservices"""
    event = {
        "eventId": f"test_evt_{int(time.time() * 1000)}",
        "eventType": "task.started",
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "source": "test",
        "version": "1.0",
        "data": {
            "workflowId": workflow_id,
            "taskId": task_id,
            "taskType": task_type,
            "status": "STARTED",
            "input": {
                "data": "test_data",
                "records": 100
            }
        }
    }
    
    try:
        # Send to conductor-events topic
        future = producer.send('conductor-events', event)
        producer.flush()
        print(f"✅ Sent test event for {task_type} - workflow: {workflow_id}, task: {task_id}")
        return True
    except Exception as e:
        print(f"❌ Failed to send event for {task_type}: {e}")
        return False

def main():
    """Send test events to trigger microservices"""
    print("🚀 Testing Event-Driven Architecture")
    print(f"📡 Kafka: {KAFKA_BOOTSTRAP}")
    
    # Wait for services to be ready
    print("⏳ Waiting 3 seconds for services to initialize...")
    time.sleep(3)
    
    # Send test events
    test_cases = [
        ("email_validation", "workflow_001", "task_001"),
        ("phone_validation", "workflow_002", "task_002"),
        ("enrichment", "workflow_003", "task_003")
    ]
    
    success_count = 0
    for task_type, workflow_id, task_id in test_cases:
        print(f"📤 Sending test event for {task_type}...")
        if send_test_event(task_type, workflow_id, task_id):
            success_count += 1
        time.sleep(1)  # Wait between events
    
    print(f"\n🎯 Test Results: {success_count}/{len(test_cases)} events sent successfully")
    print("📊 Check Kafka UI at: http://localhost:8081")
    print("📋 Check service logs with: docker-compose -f docker-compose-simple.yaml logs")

if __name__ == '__main__':
    main()
