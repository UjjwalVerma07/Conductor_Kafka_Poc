#!/usr/bin/env python3
"""
Conductor Worker for File Processing Pipeline
Handles:
1. publish_file_request - Publishes file processing request to Kafka
2. wait_for_file_result - Waits for processing result from Kafka
"""

import os
import json
import time
import logging
from kafka import KafkaProducer, KafkaConsumer
from conductor.client.automator.task_handler import TaskHandler
from conductor.client.configuration.configuration import Configuration
from conductor.client.worker.worker import Worker
from conductor.client.http.models import Task, TaskResult
from conductor.client.http.models.task_result_status import TaskResultStatus
from minio_client import get_minio_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
CONDUCTOR_SERVER_URL = os.getenv('CONDUCTOR_SERVER_URL', 'http://localhost:8080/api')
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')

# Kafka Producer (reusable)
producer = None

def get_kafka_producer():
    """Get or create Kafka producer"""
    global producer
    if producer is None:
        logger.info(f"Creating Kafka producer: {KAFKA_BOOTSTRAP}")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_block_ms=10000
        )
    return producer


class PublishFileRequestWorker:
    """Worker that publishes file processing request to Kafka"""
    
    def execute(self, task: Task) -> TaskResult:
        """
        Publish file request to Kafka
        Input: {
            "stage": 1,
            "input_bucket": "input-data",
            "input_key": "data.csv",
            "output_bucket": "intermediate-data",
            "output_key": "email-validated.json"
        }
        """
        task_result = TaskResult(
            task_id=task.task_id,
            workflow_instance_id=task.workflow_instance_id,
            worker_id='publish_file_request_worker'
        )
        
        try:
            # Get task inputs
            stage = task.input_data.get('stage')
            input_bucket = task.input_data.get('input_bucket')
            input_key = task.input_data.get('input_key')
            output_bucket = task.input_data.get('output_bucket')
            output_key = task.input_data.get('output_key')
            
            logger.info(f"Publishing file request for stage {stage}")
            logger.info(f"Input: {input_bucket}/{input_key}")
            logger.info(f"Output: {output_bucket}/{output_key}")
            
            # Create message with MinIO metadata
            message = {
                'workflowId': task.workflow_instance_id,
                'taskId': task.task_id,
                'stage': stage,
                'input_bucket': input_bucket,
                'input_key': input_key,
                'output_bucket': output_bucket,
                'output_key': output_key
            }
            
            # Publish to Kafka (single topic: file-requests)
            kafka_producer = get_kafka_producer()
            future = kafka_producer.send('file-requests', value=message)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Published to file-requests: stage={stage}")
            
            # Mark task as completed
            task_result.status = TaskResultStatus.COMPLETED
            task_result.output_data = {
                'published': True,
                'stage': stage,
                'message': message
            }
            
        except Exception as e:
            logger.error(f"Error publishing file request: {str(e)}", exc_info=True)
            task_result.status = TaskResultStatus.FAILED
            task_result.reason_for_incompletion = str(e)
        
        return task_result


class WaitForFileResultWorker:
    """Worker that waits for file processing result from Kafka"""
    
    def execute(self, task: Task) -> TaskResult:
        """
        Wait for file result from Kafka
        Input: {
            "stage": 1,
            "result_topic": "file-results-service1"
        }
        """
        task_result = TaskResult(
            task_id=task.task_id,
            workflow_instance_id=task.workflow_instance_id,
            worker_id='wait_file_result_worker'
        )
        
        try:
            # Get task inputs
            stage = task.input_data.get('stage')
            result_topic = task.input_data.get('result_topic')
            
            logger.info(f"Waiting for result from '{result_topic}' for workflow {task.workflow_instance_id}")
            
            # Create Kafka consumer
            consumer = KafkaConsumer(
                result_topic,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id=f'conductor-wait-{task.task_id}',
                consumer_timeout_ms=5000
            )
            
            # Look for matching message
            found = False
            result_data = None
            
            for message in consumer:
                logger.info(f"Received message: {message.value}")
                
                # Match by workflowId
                if message.value.get('workflowId') == task.workflow_instance_id:
                    logger.info(f"Found matching result for workflow {task.workflow_instance_id}")
                    result_data = message.value
                    found = True
                    break
            
            consumer.close()
            
            if found:
                # Task completed with result
                task_result.status = TaskResultStatus.COMPLETED
                task_result.output_data = {
                    'stage': stage,
                    'status': result_data.get('status'),
                    'output_bucket': result_data.get('output_bucket'),
                    'output_key': result_data.get('output_key'),
                    'processing_stats': result_data.get('processing_stats', {})
                }
                logger.info(f"Task completed: {result_data.get('output_key')}")
            else:
                # No matching message yet, retry
                logger.info(f"No matching result found, returning IN_PROGRESS")
                task_result.status = TaskResultStatus.IN_PROGRESS
                task_result.callback_after_seconds = 5
            
        except Exception as e:
            logger.error(f"Error waiting for result: {str(e)}", exc_info=True)
            # Return IN_PROGRESS to retry
            task_result.status = TaskResultStatus.IN_PROGRESS
            task_result.callback_after_seconds = 5
            task_result.reason_for_incompletion = str(e)
        
        return task_result


def main():
    """Start the workers"""
    logger.info("Starting File Processing Worker")
    logger.info(f"Conductor: {CONDUCTOR_SERVER_URL}")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
    
    # Initialize MinIO client (to verify connection)
    try:
        minio_client = get_minio_client()
        logger.info("MinIO client initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize MinIO: {e}")
        raise
    
    # Configure Conductor client
    configuration = Configuration(
        server_api_url=CONDUCTOR_SERVER_URL,
        debug=True
    )
    
    # Create workers
    workers = [
        Worker(
            task_definition_name='publish_file_request',
            execute_function=PublishFileRequestWorker().execute,
            poll_interval=1.0,
            domain=None
        ),
        Worker(
            task_definition_name='wait_for_file_result',
            execute_function=WaitForFileResultWorker().execute,
            poll_interval=1.0,
            domain=None
        )
    ]
    
    # Start task handler
    task_handler = TaskHandler(
        workers=workers,
        configuration=configuration
    )
    
    # Start polling
    logger.info("Workers started and polling for tasks...")
    task_handler.start_processes()
    
    # Keep running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down workers...")
        task_handler.stop_processes()
        if producer:
            producer.close()


if __name__ == '__main__':
    main()