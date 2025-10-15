#!/usr/bin/env python3
"""
Simple Conductor Worker for Kafka-based Task Flow
Handles:
1. publish_to_kafka - Publishes task request to Kafka
2. wait_for_result - Waits for result from Kafka topic
"""

import os
import sys
import json
import time
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from conductor.client.automator.task_handler import TaskHandler
from conductor.client.configuration.configuration import Configuration
from conductor.client.configuration.settings.authentication_settings import AuthenticationSettings
from conductor.client.worker.worker import Worker
from conductor.client.http.models import Task, TaskResult
from conductor.client.http.models.task_result_status import TaskResultStatus

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
        logger.info(f"Creating Kafka producer connecting to {KAFKA_BOOTSTRAP}")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_block_ms=10000
        )
    return producer


class PublishToKafkaWorker:
    """Worker that publishes a message to Kafka topic"""
    
    def execute(self, task: Task) -> TaskResult:
        """
        Execute publish task
        Input: {
            "topic": "stage1-requests",
            "data": {"message": "some data"}
        }
        """
        task_result = TaskResult(
            task_id=task.task_id,
            workflow_instance_id=task.workflow_instance_id,
            worker_id='publish_to_kafka_worker'
        )
        
        try:
            # Get task inputs
            topic = task.input_data.get('topic')
            data = task.input_data.get('data', {})
            
            logger.info(f"Publishing to topic '{topic}'")
            logger.info(f"Data: {data}")
            
            # Add metadata
            message = {
                'workflowId': task.workflow_instance_id,
                'taskId': task.task_id,
                'data': data
            }
            
            # Publish to Kafka
            kafka_producer = get_kafka_producer()
            future = kafka_producer.send(topic, value=message)
            
            # Wait for send to complete
            record_metadata = future.get(timeout=10)
            logger.info(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
            
            # Mark task as completed
            task_result.status = TaskResultStatus.COMPLETED
            task_result.output_data = {
                'published': True,
                'topic': topic,
                'message': message
            }
            logger.info(f"Task {task.task_id} completed successfully")
            
        except Exception as e:
            logger.error(f"Error in publish task: {str(e)}", exc_info=True)
            task_result.status = TaskResultStatus.FAILED
            task_result.reason_for_incompletion = str(e)
        
        return task_result


class WaitForResultWorker:
    """Worker that waits for a result from Kafka topic"""
    
    def execute(self, task: Task) -> TaskResult:
        """
        Execute wait task - polls Kafka for matching message
        Input: {
            "topic": "stage1-results",
            "timeout": 60
        }
        """
        task_result = TaskResult(
            task_id=task.task_id,
            workflow_instance_id=task.workflow_instance_id,
            worker_id='wait_for_result_worker'
        )
        
        try:
            # Get task inputs
            topic = task.input_data.get('topic')
            timeout = task.input_data.get('timeout', 30)
            
            logger.info(f"Waiting for result from topic '{topic}' for workflow {task.workflow_instance_id}")
            
            # Create consumer
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id=f'conductor-wait-{task.task_id}',
                consumer_timeout_ms=5000  # Poll for 5 seconds
            )
            
            # Look for matching message
            found = False
            result_data = None
            
            for message in consumer:
                logger.info(f"Received message: {message.value}")
                
                # Check if this message is for our workflow
                if message.value.get('workflowId') == task.workflow_instance_id:
                    logger.info(f"Found matching message for workflow {task.workflow_instance_id}")
                    result_data = message.value
                    found = True
                    break
            
            consumer.close()
            
            if found:
                # Mark task as completed
                task_result.status = TaskResultStatus.COMPLETED
                task_result.output_data = {
                    'result': result_data.get('result', {}),
                    'status': result_data.get('status', 'success')
                }
                logger.info(f"Task {task.task_id} completed with result")
            else:
                # No matching message found yet, return IN_PROGRESS
                # Conductor will retry this task
                logger.info(f"No matching message found yet, returning IN_PROGRESS")
                task_result.status = TaskResultStatus.IN_PROGRESS
                task_result.callback_after_seconds = 5
            
        except Exception as e:
            logger.error(f"Error in wait task: {str(e)}", exc_info=True)
            # Return IN_PROGRESS on error to retry
            task_result.status = TaskResultStatus.IN_PROGRESS
            task_result.callback_after_seconds = 5
            task_result.reason_for_incompletion = str(e)
        
        return task_result


def main():
    """Start the workers"""
    logger.info(f"Starting Simple Worker")
    logger.info(f"Conductor Server: {CONDUCTOR_SERVER_URL}")
    logger.info(f"Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
    
    # Configure Conductor client
    configuration = Configuration(
        server_api_url=CONDUCTOR_SERVER_URL,
        debug=True
    )
    
    # Create workers
    workers = [
        Worker(
            task_definition_name='publish_to_kafka',
            execute_function=PublishToKafkaWorker().execute,
            poll_interval=1.0,
            domain=None
        ),
        Worker(
            task_definition_name='wait_for_result',
            execute_function=WaitForResultWorker().execute,
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
