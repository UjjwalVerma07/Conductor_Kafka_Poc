#!/usr/bin/env python3
"""
Conductor Event Publisher
Publishes events from Conductor to Kafka for event-driven microservices
"""

import os
import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
from conductor.client.configuration.configuration import Configuration
from conductor.client.automator.task_handler import TaskHandler
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

# Initialize Kafka producer
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

class ConductorEventPublisher:
    """Publishes events from Conductor to Kafka"""
    
    def publish_task_event(self, task, event_type, data=None):
        """Publish task event to Kafka"""
        event = {
            "eventId": f"evt_{int(time.time() * 1000)}",
            "eventType": event_type,
            "timestamp": datetime.utcnow().isoformat(),
            "source": "conductor",
            "version": "1.0",
            "data": {
                "workflowId": task.workflow_instance_id,
                "taskId": task.task_id,
                "taskType": task.task_definition_name,
                "status": event_type.split('.')[-1].upper(),
                "input": task.input_data
            }
        }
        
        if data:
            event["data"].update(data)
        
        # Publish to conductor-events topic
        kafka_producer.send('conductor-events', event)
        logger.info(f"Published event: {event_type} for task {task.task_id}")
    
    def execute(self, task: Task) -> TaskResult:
        """Execute task and publish events"""
        task_result = TaskResult(
            task_id=task.task_id,
            workflow_instance_id=task.workflow_instance_id,
            worker_id='conductor_event_publisher'
        )
        
        try:
            # Publish task started event
            self.publish_task_event(task, "task.started")
            
            # Mark task as completed (event-driven processing)
            task_result.status = TaskResultStatus.COMPLETED
            task_result.output_data = {
                'status': 'event_published',
                'message': f'Event published for {task.task_definition_name}',
                'eventType': 'task.started'
            }
            
            logger.info(f"Task {task.task_id} event published successfully")
            
        except Exception as e:
            logger.error(f"Error publishing event: {str(e)}", exc_info=True)
            task_result.status = TaskResultStatus.FAILED
            task_result.reason_for_incompletion = str(e)
        
        return task_result

def main():
    """Start the Conductor Event Publisher"""
    logger.info("Starting Conductor Event Publisher")
    logger.info(f"Conductor: {CONDUCTOR_SERVER_URL}")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
    
    # Configure Conductor client
    configuration = Configuration(
        server_api_url=CONDUCTOR_SERVER_URL,
        debug=True
    )
    
    # Create event publisher worker
    event_publisher = ConductorEventPublisher()
    
    # Create workers for each task type
    workers = [
        Worker(
            task_definition_name='email_validation',
            execute_function=event_publisher.execute,
            poll_interval=1.0,
            domain=None
        ),
        Worker(
            task_definition_name='phone_validation',
            execute_function=event_publisher.execute,
            poll_interval=1.0,
            domain=None
        ),
        Worker(
            task_definition_name='enrichment',
            execute_function=event_publisher.execute,
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
    logger.info("Conductor Event Publisher started and polling...")
    task_handler.start_processes()
    
    # Keep running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down event publisher...")
        task_handler.stop_processes()
        kafka_producer.close()

if __name__ == '__main__':
    main()
