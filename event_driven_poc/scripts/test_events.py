#!/usr/bin/env python3
"""
Test Event-Driven Architecture
Sends test events to trigger the event-driven microservices
"""

import os
import json
import time
import logging
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')

# Initialize Kafka producer
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
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
    
    # Send to conductor-events topic (EventRouter will route it)
    kafka_producer.send('conductor-events', event)
    kafka_producer.flush()
    
    logger.info(f"Sent test event for {task_type} - workflow: {workflow_id}, task: {task_id}")

def main():
    """Send test events to trigger microservices"""
    logger.info("Testing Event-Driven Architecture")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
    
    # Wait for services to be ready
    logger.info("Waiting 5 seconds for services to initialize...")
    time.sleep(5)
    
    # Send test events
    test_cases = [
        ("email_validation", "workflow_001", "task_001"),
        ("phone_validation", "workflow_002", "task_002"),
        ("enrichment", "workflow_003", "task_003")
    ]
    
    for task_type, workflow_id, task_id in test_cases:
        logger.info(f"Sending test event for {task_type}...")
        send_test_event(task_type, workflow_id, task_id)
        time.sleep(2)  # Wait between events
    
    logger.info("Test events sent! Check service logs to see event processing.")
    logger.info("You can also check Kafka UI at: http://localhost:8081")

if __name__ == '__main__':
    main()
