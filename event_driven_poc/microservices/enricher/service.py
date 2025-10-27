#!/usr/bin/env python3
"""
Enricher Service - Event-Driven
Consumes events from Kafka and processes data enrichment
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
SERVICE_NAME = os.getenv('SERVICE_NAME', 'enricher')

# Initialize Kafka producer
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

class EnricherService:
    """Event-driven data enrichment service"""
    
    def __init__(self):
        self.kafka_producer = kafka_producer
    
    def enrich_data(self, data):
        """Mock data enrichment logic"""
        # Simulate processing time
        time.sleep(2)
        
        # Mock enrichment results
        result = {
            'status': 'success',
            'total_records': 100,
            'enriched_records': 95,
            'failed_records': 5,
            'processing_time': '2s'
        }
        
        logger.info(f"Data enrichment completed: {result}")
        return result
    
    def process_task_event(self, event):
        """Process task event and publish result"""
        try:
            workflow_id = event.get('workflowId')
            task_id = event.get('taskId')
            data = event.get('data', {})
            input_data = data.get('inputData', 'phone_validated_data')
            records = data.get('records', 90)
            
            logger.info(f"Processing data enrichment for workflow {workflow_id}, task {task_id}")
            logger.info(f"Input data: {input_data}, Records: {records}")
            
            # Simulate data enrichment
            result = self.enrich_data(input_data)
            
            # Calculate processed records (simulate some failures)
            processed_records = int(records * 0.85)  # 85% success rate
            failed_records = records - processed_records
            
            # Publish result event to enrichment-results topic
            result_event = {
                "workflowId": workflow_id,
                "taskId": task_id,
                "eventType": "enrichment_completed",
                "data": {
                    "result": "success",
                    "processedRecords": processed_records,
                    "failedRecords": failed_records,
                    "outputData": "enriched_data",
                    "pipelineStage": "enrichment",
                    "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "enrichmentResult": result
                }
            }
            
            # Publish to enrichment-results topic
            self.kafka_producer.send('enrichment-results', result_event)
            self.kafka_producer.flush()
            
            logger.info(f"✅ Published result event to enrichment-results for task {task_id}")
            logger.info(f"Processed {processed_records} records, {failed_records} failed")
            
        except Exception as e:
            logger.error(f"Error processing task event: {e}", exc_info=True)
            
            # Publish failure event
            failure_event = {
                "workflowId": event.get('workflowId', 'unknown'),
                "taskId": event.get('taskId', 'unknown'),
                "eventType": "enrichment_completed",
                "data": {
                    "result": "failure",
                    "processedRecords": 0,
                    "failedRecords": event.get('data', {}).get('records', 0),
                    "outputData": "enrichment_failed",
                    "pipelineStage": "enrichment",
                    "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "error": str(e)
                }
            }
            
            self.kafka_producer.send('enrichment-results', failure_event)
            self.kafka_producer.flush()
            logger.error(f"❌ Published failure event to enrichment-results")
    
    def consume_task_events(self):
        """Consume task events from Kafka"""
        consumer = KafkaConsumer(
            'enrichment-requests',
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id=f'{SERVICE_NAME}-group'
        )
        
        logger.info(f"{SERVICE_NAME} started - listening for enrichment events")
        
        for message in consumer:
            try:
                event = message.value
                logger.info(f"Received task event: {event['eventType']}")
                
                event_type = event.get('eventType', 'unknown')
                logger.info(f"Received task event: {event_type}")
                
                if event_type == 'enrichment_request':
                    self.process_task_event(event)
                else:
                    logger.debug(f"Skipping event type: {event_type}")
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)

def main():
    """Start the Enricher Service"""
    logger.info(f"Starting {SERVICE_NAME} Service")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
    
    # Wait for services to be ready
    logger.info("Waiting 10 seconds for services to initialize...")
    time.sleep(10)
    
    # Start service
    service = EnricherService()
    service.consume_task_events()

if __name__ == '__main__':
    main()
