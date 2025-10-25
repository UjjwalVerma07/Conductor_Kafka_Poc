#!/usr/bin/env python3
"""
Simple Microservices for Kafka-based Processing
Two services:
1. Service 1: Consumes from stage1-requests, processes, publishes to stage1-results
2. Service 2: Consumes from stage2-requests, processes, publishes to stage2-results
"""

import os
import json
import time
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')


def create_producer():
    """Create Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


def service1_processor():
    """
    Service 1: Email Validator
    Consumes from: stage1-requests
    Publishes to: stage1-results
    """
    logger.info("Starting Service 1 (Email Validator)")
    
    consumer = KafkaConsumer(
        'stage1-requests',
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='service1-group' 
    )
    
    producer = create_producer()
    
    logger.info("Service 1 listening on 'stage1-requests'...")
    
    for message in consumer:
        try:
            data = message.value
            logger.info(f"Service 1 received: {data}")
            
            # Extract metadata
            workflow_id = data.get('workflowId')
            task_id = data.get('taskId')
            input_data = data.get('data', {})
            
            # Simulate processing
            logger.info(f"Service 1 processing for workflow {workflow_id}...")
            time.sleep(2)  # Simulate work
            
            # Create result
            result = {
                'workflowId': workflow_id,
                'taskId': task_id,
                'status': 'success',
                'result': {
                    'service': 'email-validator',
                    'input': input_data,
                    'validated_emails': 150,
                    'invalid_emails': 5,
                    'processed_at': time.time()
                }
            }
            
            # Publish result
            producer.send('stage1-results', value=result)
            producer.flush()
            logger.info(f"Service 1 published result to 'stage1-results' for workflow {workflow_id}")
            
        except Exception as e:
            logger.error(f"Service 1 error: {str(e)}", exc_info=True)


def service2_processor():
    """
    Service 2: Phone Validator
    Consumes from: stage2-requests
    Publishes to: stage2-results
    """
    logger.info("Starting Service 2 (Phone Validator)")
    
    consumer = KafkaConsumer(
        'stage2-requests',
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='service2-group'
    )
    
    producer = create_producer()
    
    logger.info("Service 2 listening on 'stage2-requests'...")
    
    for message in consumer:
        try:
            data = message.value
            logger.info(f"Service 2 received: {data}")
            
            # Extract metadata
            workflow_id = data.get('workflowId')
            task_id = data.get('taskId')
            input_data = data.get('data', {})
            
            # Simulate processing
            logger.info(f"Service 2 processing for workflow {workflow_id}...")
            time.sleep(3)  # Simulate work
            
            # Create result
            result = {
                'workflowId': workflow_id,
                'taskId': task_id,
                'status': 'success',
                'result': {
                    'service': 'phone-validator',
                    'input': input_data,
                    'validated_phones': 140,
                    'invalid_phones': 15,
                    'final_output': 'All processing complete!',
                    'processed_at': time.time()
                }
            }
            
            # Publish result
            producer.send('stage2-results', value=result)
            producer.flush()
            logger.info(f"Service 2 published result to 'stage2-results' for workflow {workflow_id}")
            
        except Exception as e:
            logger.error(f"Service 2 error: {str(e)}", exc_info=True)


def main():
    """Run both services in parallel"""
    import threading
    
    logger.info(f"Starting Microservices")
    logger.info(f"Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
    
    # Wait for Kafka to be ready
    logger.info("Waiting for Kafka to be ready...")
    time.sleep(10)
    
    # Start both services in separate threads
    t1 = threading.Thread(target=service1_processor, daemon=True)
    t2 = threading.Thread(target=service2_processor, daemon=True)
    
    t1.start()
    t2.start()
    
    logger.info("Both microservices are running!")
    
    # Keep running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down microservices...")


if __name__ == '__main__':
    main()

