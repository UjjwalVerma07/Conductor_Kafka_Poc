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
            workflow_id = event.get('workflowId')
            task_id = event.get('taskId')
            data = event.get('data', {})
            input_data = data.get('inputData', 'raw_data')
            records = data.get('records', 100)
            
            logger.info(f"Processing email validation for workflow {workflow_id}, task {task_id}")
            logger.info(f"Input data: {input_data}, Records: {records}")
            
            # Simulate email validation
            result = self.validate_emails(input_data)
            
            # Calculate processed records (simulate some failures)
            processed_records = int(records * 0.95)  # 95% success rate
            failed_records = records - processed_records
            
            # Publish result event to email-validation-results topic
            result_event = {
                "workflowId": workflow_id,
                "taskId": task_id,
                "eventType": "email_validation_completed",
                "data": {
                    "result": "success",
                    "processedRecords": processed_records,
                    "failedRecords": failed_records,
                    "outputData": "email_validated_data",
                    "pipelineStage": "email_validation",
                    "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "validationResult": result
                }
            }
            
            # Publish to email-validation-results topic
            self.kafka_producer.send('email-validation-results', result_event)
            self.kafka_producer.flush()
            
            logger.info(f"✅ Published result event to email-validation-results for task {task_id}")
            logger.info(f"Processed {processed_records} records, {failed_records} failed")
            
        except Exception as e:
            logger.error(f"Error processing task event: {e}", exc_info=True)
            
            # Publish failure event
            failure_event = {
                "workflowId": event.get('workflowId', 'unknown'),
                "taskId": event.get('taskId', 'unknown'),
                "eventType": "email_validation_completed",
                "data": {
                    "result": "failure",
                    "processedRecords": 0,
                    "failedRecords": event.get('data', {}).get('records', 0),
                    "outputData": "validation_failed",
                    "pipelineStage": "email_validation",
                    "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "error": str(e)
                }
            }
            
            self.kafka_producer.send('email-validation-results', failure_event)
            self.kafka_producer.flush()
            logger.error(f"❌ Published failure event to email-validation-results")
    
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
                logger.info(f"🔍 Raw message value type: {type(event)}")
                logger.info(f"🔍 Raw message value: {event}")
                
                if isinstance(event, dict):
                    event_type = event.get('eventType', 'unknown')
                    logger.info(f"📨 Event type: {event_type}")
                    logger.info(f"📨 Full event structure: {event}")
                    
                    if event_type == 'email_validation_request':
                        logger.info(f"✅ MATCH! Processing email validation request for workflow: {event.get('workflowId')}")
                        logger.info(f"✅ Calling process_task_event with: {event}")
                        self.process_task_event(event)
                        logger.info(f"✅ process_task_event completed")
                    else:
                        logger.warning(f"❌ NO MATCH! Expected 'email_validation_request', got: '{event_type}'")
                else:
                    logger.error(f"❌ Event is not a dictionary! Type: {type(event)}, Value: {event}")
                    
            except Exception as e:
                logger.error(f"💥 Error processing message: {e}", exc_info=True)

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
