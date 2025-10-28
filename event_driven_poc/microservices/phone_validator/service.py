#!/usr/bin/env python3
"""
Phone Validator Service - Event-Driven
Consumes events from Kafka and processes phone validation
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
SERVICE_NAME = os.getenv('SERVICE_NAME', 'phone-validator')

# Initialize Kafka producer
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

class PhoneValidatorService:
    """Event-driven phone validation service"""
    
    def __init__(self):
        self.kafka_producer = kafka_producer
    
    #This is the function that will validate the phones
    def validate_phones(self, data):
        """Mock phone validation logic"""
        # Simulate processing time
        time.sleep(2)
        
        # Mock validation results
        result = {
            'status': 'success',
            'total_records': 100,
            'valid_phones': 90,
            'invalid_phones': 10,
            'processing_time': '2s'
        }
        
        logger.info(f"Phone validation completed: {result}")
        return result
    
    #This is the function that will process the task event and then the result wiill be published to the phone-validation-results topic
    def process_task_event(self, event):
        """Process task event and publish result"""
        try:
            workflow_id = event.get('workflowId')
            task_id = event.get('taskId')
            data = event.get('data', {})
            input_data = data.get('inputData', 'email_validated_data')
            records = data.get('records', 95)
            
            logger.info(f"Processing phone validation for workflow {workflow_id}, task {task_id}")
            logger.info(f"Input data: {input_data}, Records: {records}")
            
            # Simulate phone validation
            result = self.validate_phones(input_data)
            
            # Calculate processed records (simulate some failures)
            processed_records = int(records * 0.90)  # 90% success rate
            failed_records = records - processed_records
            
            # Publish result event to phone-validation-results topic
            result_event = {
                "workflowId": workflow_id,
                "taskId": task_id,
                "eventType": "phone_validation_completed",
                "data": {
                    "result": "success",
                    "processedRecords": processed_records,
                    "failedRecords": failed_records,
                    "outputData": "phone_validated_data",
                    "pipelineStage": "phone_validation",
                    "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "validationResult": result
                }
            }
            
            # Publish to phone-validation-results topic
            self.kafka_producer.send('phone-validation-results', result_event)
            self.kafka_producer.flush()
            
            logger.info(f"✅ Published result event to phone-validation-results for task {task_id}")
            logger.info(f"Processed {processed_records} records, {failed_records} failed")
            
        except Exception as e:
            logger.error(f"Error processing task event: {e}", exc_info=True)
            
            # Publish failure event
            failure_event = {
                "workflowId": event.get('workflowId', 'unknown'),
                "taskId": event.get('taskId', 'unknown'),
                "eventType": "phone_validation_completed",
                "data": {
                    "result": "failure",
                    "processedRecords": 0,
                    "failedRecords": event.get('data', {}).get('records', 0),
                    "outputData": "validation_failed",
                    "pipelineStage": "phone_validation",
                    "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "error": str(e)
                }
            }
            
            self.kafka_producer.send('phone-validation-results', failure_event)
            self.kafka_producer.flush()
            logger.error(f"❌ Published failure event to phone-validation-results")
    
    #This is the function that will consume the task event from the phone-validation-requests and process it by calling the process_task_event function
    def consume_task_events(self):
        """Consume task events from Kafka"""
        consumer = KafkaConsumer(
            'phone-validation-requests',
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id=f'{SERVICE_NAME}-group'
        )
        
        logger.info(f"{SERVICE_NAME} started - listening for phone validation events")
        
        for message in consumer:
            try:
                event = message.value
                logger.info(f"Received task event: {event['eventType']}")
                
                event_type = event.get('eventType', 'unknown')
                logger.info(f"Received task event: {event_type}")
                
                if event_type == 'phone_validation_request':
                    self.process_task_event(event)
                else:
                    logger.debug(f"Skipping event type: {event_type}")
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)

def main():
    """Start the Phone Validator Service"""
    logger.info(f"Starting {SERVICE_NAME} Service")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
    
    # Wait for services to be ready
    logger.info("Waiting 10 seconds for services to initialize...")
    time.sleep(10)
    
    # Start service
    service = PhoneValidatorService()
    service.consume_task_events()

if __name__ == '__main__':
    main()
