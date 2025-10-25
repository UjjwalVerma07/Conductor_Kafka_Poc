#!/usr/bin/env python3
"""
Email Validator Service - Event-Driven
Consumes events from Kafka and processes email validation
"""

import os
import json
import time
import logging
from kafka import KafkaConsumer, KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
SERVICE_NAME = os.getenv('SERVICE_NAME', 'email-validator')

# Initialize Kafka producer
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

class EmailValidatorService:
    """Event-driven email validation service"""
    
    def __init__(self):
        self.kafka_producer = kafka_producer
    
    def validate_emails(self, data):
        """Mock email validation logic"""
        # Simulate processing time
        time.sleep(2)
        
        # Mock validation results
        result = {
            'status': 'success',
            'total_records': 100,
            'valid_emails': 85,
            'invalid_emails': 15,
            'processing_time': '2s'
        }
        
        logger.info(f"Email validation completed: {result}")
        return result
    
    def process_task_event(self, event):
        """Process task event and publish result"""
        try:
            workflow_id = event['data']['workflowId']
            task_id = event['data']['taskId']
            input_data = event['data']['input']
            
            logger.info(f"Processing email validation for workflow {workflow_id}")
            
            # Simulate email validation
            result = self.validate_emails(input_data)
            
            # Publish result event
            result_event = {
                "eventId": f"result_{int(time.time() * 1000)}",
                "eventType": "task.completed",
                "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "source": SERVICE_NAME,
                "version": "1.0",
                "data": {
                    "workflowId": workflow_id,
                    "taskId": task_id,
                    "taskType": "email_validation",
                    "status": "COMPLETED",
                    "result": result
                }
            }
            
            # Publish to task-updates topic
            self.kafka_producer.send('task-updates', result_event)
            self.kafka_producer.flush()
            
            logger.info(f"Published result event for task {task_id}")
            
        except Exception as e:
            logger.error(f"Error processing task event: {e}", exc_info=True)
            
            # Publish failure event
            failure_event = {
                "eventId": f"failure_{int(time.time() * 1000)}",
                "eventType": "task.failed",
                "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "source": SERVICE_NAME,
                "version": "1.0",
                "data": {
                    "workflowId": event['data']['workflowId'],
                    "taskId": event['data']['taskId'],
                    "taskType": "email_validation",
                    "status": "FAILED",
                    "error": str(e)
                }
            }
            
            self.kafka_producer.send('task-updates', failure_event)
            self.kafka_producer.flush()
    
    def consume_task_events(self):
        """Consume task events from Kafka"""
        consumer = KafkaConsumer(
            'email-validation-requests',
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id=f'{SERVICE_NAME}-group'
        )
        
        logger.info(f"{SERVICE_NAME} started - listening for email validation events")
        
        for message in consumer:
            try:
                event = message.value
                logger.info(f"Received task event: {event['eventType']}")
                
                if event['eventType'] == 'task.started':
                    self.process_task_event(event)
                else:
                    logger.debug(f"Skipping event type: {event['eventType']}")
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)

def main():
    """Start the Email Validator Service"""
    logger.info(f"Starting {SERVICE_NAME} Service")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
    
    # Wait for services to be ready
    logger.info("Waiting 10 seconds for services to initialize...")
    time.sleep(10)
    
    # Start service
    service = EmailValidatorService()
    service.consume_task_events()

if __name__ == '__main__':
    main()
